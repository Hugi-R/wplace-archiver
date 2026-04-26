use crate::cli::Args;
use crate::db::{Db, TileRecord};
use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use reqwest::Client;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct ClientInfo {
    pub client: Client,
    pub local_address: Option<std::net::IpAddr>,
}

pub struct Downloader {
    db: Arc<Db>,
    args: Arc<Args>,
    clients: Arc<Mutex<HashMap<usize, ClientInfo>>>,
    #[allow(dead_code)] // for now we don't create new clients after initialization, so this is not used
    client_id_counter: AtomicUsize,
    next_client_idx: AtomicUsize,
    head_status_codes: Mutex<HashMap<u16, usize>>,
    get_status_codes: Mutex<HashMap<u16, usize>>,
    tx: mpsc::Sender<TileRecord>,
}

impl Downloader {
    pub fn new(db: Arc<Db>, args: Arc<Args>, clients: Vec<ClientInfo>) -> (Self, mpsc::Receiver<TileRecord>) {
        let (tx, rx) = mpsc::channel(1000);
        let client_id_counter = AtomicUsize::new(0);
        let mut client_map = HashMap::new();
        for client_info in clients {
            let id = client_id_counter.fetch_add(1, Ordering::SeqCst);
            client_map.insert(id, client_info);
        }

        (Self {
            db,
            args,
            clients: Arc::new(Mutex::new(client_map)),
            client_id_counter,
            next_client_idx: AtomicUsize::new(0),
            head_status_codes: Mutex::new(HashMap::new()),
            get_status_codes: Mutex::new(HashMap::new()),
            tx,
        }, rx)
    }

    fn get_client(&self) -> (usize, Client) {
        let clients = self.clients.lock().unwrap();
        let keys: Vec<usize> = clients.keys().cloned().collect();
        if keys.is_empty() {
            panic!("No clients available in the pool!");
        }
        let idx = self.next_client_idx.fetch_add(1, Ordering::Relaxed) % keys.len();
        let client_id = keys[idx];
        let client = clients.get(&client_id).unwrap().client.clone();
        (client_id, client)
    }

    fn recreate_client(&self, client_id: usize) {
        let mut clients = self.clients.lock().unwrap();
        if let Some(info) = clients.get(&client_id) {
            let local_address = info.local_address;
            
            let new_client_builder = reqwest::Client::builder().timeout(Duration::from_secs(15)).pool_max_idle_per_host(10);
            let new_client_builder = if let Some(addr) = local_address {
                new_client_builder.local_address(addr)
            } else {
                new_client_builder
            };

            match new_client_builder.build() {
                Ok(new_client) => {
                    if let Some(info) = clients.get_mut(&client_id) {
                        info.client = new_client;
                        warn!("Recreated client {client_id} with address {:?}", local_address);
                    }
                }
                Err(e) => {
                    error!("Failed to recreate client {client_id} with address {:?}: {e}. Removing client from pool.", local_address);
                    clients.remove(&client_id);
                }
            }
        }
    }

    fn record_status(&self, code: u16, label: &str) {
        let status_codes = if label == "HEAD" {
            &self.head_status_codes
        } else {
            &self.get_status_codes
        };
        let mut status_codes = status_codes.lock().unwrap();
        *status_codes.entry(code).or_insert(0) += 1;
    }

    fn report_statistics(&self, elapsed: std::time::Duration) {
        let head_status_codes = self.head_status_codes.lock().unwrap();
        let head_status_breakdown: Vec<String> = head_status_codes
            .iter()
            .map(|(code, count)| format!("{}: {}", code, count))
            .collect();
        let get_status_codes = self.get_status_codes.lock().unwrap();
        let get_status_breakdown: Vec<String> = get_status_codes
            .iter()
            .map(|(code, count)| format!("{}: {}", code, count))
            .collect();

        info!(
            "Download complete. Elapsed: {:?}, Head Status codes: [{}], Get Status codes: [{}]",
            elapsed,
            head_status_breakdown.join(", "),
            get_status_breakdown.join(", ")
        );
    }

    fn report_periodic_statistics(&self, elapsed: std::time::Duration) {
        let head_status_codes = self.head_status_codes.lock().unwrap();
        let head_status_breakdown: Vec<String> = head_status_codes
            .iter()
            .map(|(code, count)| format!("{}: {}", code, count))
            .collect();
        let get_status_codes = self.get_status_codes.lock().unwrap();
        let get_status_breakdown: Vec<String> = get_status_codes
            .iter()
            .map(|(code, count)| format!("{}: {}", code, count))
            .collect();

        info!(
            "Progress - Elapsed: {:?}, Head Status codes: [{}], Get Status codes: [{}]",
            elapsed,
            head_status_breakdown.join(", "),
            get_status_breakdown.join(", ")
        );
    }

    async fn run_consumer(
        db: Arc<Db>,
        mut rx: mpsc::Receiver<TileRecord>,
        error_tx: mpsc::Sender<anyhow::Error>,
    ) {
        let mut buffer = Vec::with_capacity(100);
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                res = rx.recv() => {
                    match res {
                        Some(tile) => {
                            buffer.push(tile);
                            if buffer.len() >= 100 {
                                if let Err(e) = db.save_tiles_batch(buffer.drain(..).collect()).await {
                                    let _ = error_tx.send(e).await;
                                    break;
                                }
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                if let Err(e) = db.save_tiles_batch(buffer.drain(..).collect()).await {
                                    let _ = error_tx.send(e).await;
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        if let Err(e) = db.save_tiles_batch(buffer.drain(..).collect()).await {
                            let _ = error_tx.send(e).await;
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn run_downloader(
        downloader: Arc<Self>,
        x_min: i32,
        x_max: i32,
        y_min: i32,
        y_max: i32,
    ) -> Result<()> {
        let tiles = futures::stream::iter((x_min..=x_max).flat_map(move |x| {
            (y_min..=y_max).map(move |y| (x, y))
        })).map(Ok);

        tiles.try_for_each_concurrent(downloader.args.concurrency, |(x, y)| {
            let d = downloader.clone();
            async move {
                d.download_tile(x, y).await.map_err(|e| {
                    error!("Error downloading tile {x}/{y}: {e}");
                    e
                })
            }
        }).await?;

        Ok(())
    }

    pub async fn run(self, rx: mpsc::Receiver<TileRecord>) -> Result<()> {
        let (x_min, x_max) = self.args.x_range.unwrap_or((0, 2048));
        let (y_min, y_max) = self.args.y_range.unwrap_or((0, 2048));

        info!("Starting download for X: {} to {}, Y: {} to {}", x_min, x_max, y_min, y_max);

        let start_time = Instant::now();

        let downloader = Arc::new(self);
        let (error_tx, mut error_rx) = mpsc::channel::<anyhow::Error>(1);
        let db = downloader.db.clone();

        let consumer_handle = tokio::spawn(Self::run_consumer(
            db,
            rx,
            error_tx,
        ));

        let downloader_clone = downloader.clone();
        let mut download_task = tokio::spawn(async move {
            Self::run_downloader(downloader_clone, x_min, x_max, y_min, y_max).await
        });

        let downloader_for_logger = downloader.clone();
        let start_time_for_logger = start_time;
        let logger_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                downloader_for_logger.report_periodic_statistics(start_time_for_logger.elapsed());
            }
        });

        let result = tokio::select! {
            res = &mut download_task => res.map_err(|e| anyhow::anyhow!("Download task panicked: {e}"))?,
            Some(e) = error_rx.recv() => {
                download_task.abort();
                Err(e)
            },
        };

        logger_handle.abort();

        if result.is_ok() {
            downloader.report_statistics(start_time.elapsed());
        }

        drop(downloader);
        let _ = consumer_handle.await;

        result
    }

    async fn send_request_with_retry<F>(
        &self,
        label: &str,
        x: i32,
        y: i32,
        request_builder_factory: F,
        attempts: &mut u32,
        max_attempts: u32,
        delay: &mut std::time::Duration,
    ) -> Result<reqwest::Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        loop {
            *attempts += 1;
            let res = match request_builder_factory().send().await {
                Ok(res) => res,
                Err(e) => {
                    self.record_status(0, label);
                    if *attempts >= max_attempts {
                        return Err(e.into());
                    }
                    warn!("{label} {x}/{y} connection error: {e}. Retrying (attempt {}/{})", *attempts, max_attempts);
                    tokio::time::sleep(*delay).await;
                    *delay = (*delay * 2).min(Duration::from_secs(10));  // Cap backoff at 10s
                    *attempts += 1;
                    continue;
                }
            };

            let status = res.status();
            self.record_status(status.as_u16(), label);

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = res.headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(std::time::Duration::from_secs)
                    .filter(|&d| d > std::time::Duration::from_secs(1));

                let wait_time = retry_after.unwrap_or(*delay);
                warn!("Received 429 for {label} {x}/{y}. Retrying in {:?} (attempt {}/{})", wait_time, *attempts, max_attempts);

                if *attempts >= max_attempts {
                    return Err(anyhow::anyhow!("Max retries reached for 429 on {label} {x}/{y}"));
                }

                tokio::time::sleep(wait_time).await;
                *delay *= 2;
                continue;
            }

            return Ok(res);
        }
    }

    async fn download_tile(&self, x: i32, y: i32) -> Result<()> {
        let url = self.args.url
            .replace("{x}", &x.to_string())
            .replace("{y}", &y.to_string());

        let mut client_retries = 0;
        const MAX_CLIENT_RETRIES: u32 = 3;

        loop {
            let (client_id, client) = self.get_client();

            let existing_metadata = self.db.get_tile_metadata(x, y).await?;

            let mut attempts = 0;
            let max_attempts = 5;
            let mut delay = std::time::Duration::from_secs(1);

            // First, check if the tile has changed using HEAD
            let head_res = match self.send_request_with_retry(
                "HEAD",
                x,
                y,
                || client.head(&url),
                &mut attempts,
                max_attempts,
                &mut delay,
            ).await {
                Ok(res) => res,
                Err(e) => {
                    if let Some(reqwest_err) = e.downcast_ref::<reqwest::Error>() {
                        
                        warn!("Network error during HEAD request for tile {x}/{y} with client {client_id}: {reqwest_err}. Try {client_retries}.",);
                        if let Some(source) = reqwest_err.source() {
                            warn!("Error source: {source}");
                        }
                        self.recreate_client(client_id);
                        client_retries += 1;
                        if client_retries >= MAX_CLIENT_RETRIES {
                            return Err(e);
                        }
                        continue;
                    }
                    return Err(e);
                }
            };

            if head_res.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(());
            }
            
            let new_etag = head_res.headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            
            let new_last_modified = head_res.headers()
                .get("last-modified")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            
            if let Some(ref meta) = existing_metadata {
                if meta.etag == new_etag && meta.last_modified == new_last_modified {
                    // Tile unchanged
                    return Ok(());
                }
            }

            // If changed or not present, fetch the full image
            let get_res = match self.send_request_with_retry(
                "GET",
                x,
                y,
                || client.get(&url),
                &mut attempts,
                max_attempts,
                &mut delay,
            ).await {
                Ok(res) => res,
                Err(e) => {
                    if let Some(reqwest_err) = e.downcast_ref::<reqwest::Error>() {
                        warn!("Network error during GET request for tile {x}/{y} with client {client_id}: {reqwest_err}. Try {client_retries}.",);
                        if let Some(source) = reqwest_err.source() {
                            warn!("Error source: {source}");
                        }
                        self.recreate_client(client_id);
                        client_retries += 1;
                        if client_retries >= MAX_CLIENT_RETRIES {
                            return Err(e);
                        }
                        continue;
                    }
                    return Err(e);
                }
            };

            if !get_res.status().is_success() {
                warn!("Failed to fetch tile {x}/{y}: {}", get_res.status());
                return Err(anyhow::anyhow!("HTTP error: {}", get_res.status()));
            }

            let data = get_res.bytes().await?.to_vec();
            
            self.tx.send(TileRecord {
                x,
                y,
                data,
                etag: new_etag,
                last_modified: new_last_modified,
            }).await.map_err(|e| anyhow::anyhow!("Failed to send tile to channel: {e}"))?;

            return Ok(());
        }
    }
}
