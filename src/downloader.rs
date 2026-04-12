use crate::cli::Args;
use crate::db::{Db, TileRecord};
use anyhow::Result;
use futures::StreamExt;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct Downloader {
    db: Arc<Db>,
    args: Arc<Args>,
    clients: Vec<Client>,
    next_client_idx: AtomicUsize,
    head_requests: AtomicUsize,
    get_requests: AtomicUsize,
    tiles_downloaded: AtomicUsize,
    status_codes: Mutex<HashMap<u16, usize>>,
    tx: mpsc::Sender<TileRecord>,
}

impl Downloader {
    pub fn new(db: Arc<Db>, args: Arc<Args>, clients: Vec<Client>) -> (Self, mpsc::Receiver<TileRecord>) {
        let (tx, rx) = mpsc::channel(1000);
        (Self {
            db,
            args,
            clients,
            next_client_idx: AtomicUsize::new(0),
            head_requests: AtomicUsize::new(0),
            get_requests: AtomicUsize::new(0),
            tiles_downloaded: AtomicUsize::new(0),
            status_codes: Mutex::new(HashMap::new()),
            tx,
        }, rx)
    }

    fn get_client(&self) -> &Client {
        let idx = self.next_client_idx.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }

    fn record_status(&self, code: u16) {
        let mut status_codes = self.status_codes.lock().unwrap();
        *status_codes.entry(code).or_insert(0) += 1;
    }

    fn increment_head_requests(&self) {
        self.head_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_get_requests(&self) {
        self.get_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_tiles_downloaded(&self) {
        self.tiles_downloaded.fetch_add(1, Ordering::Relaxed);
    }

    fn report_statistics(&self, elapsed: std::time::Duration) {
        let status_codes = self.status_codes.lock().unwrap();
        let status_breakdown: Vec<String> = status_codes
            .iter()
            .map(|(code, count)| format!("{}: {}", code, count))
            .collect();

        info!(
            "Download complete. Elapsed: {:?}, Head requests: {}, Get requests: {}, Tiles downloaded: {}, Status codes: [{}]",
            elapsed,
            self.head_requests.load(Ordering::Relaxed),
            self.get_requests.load(Ordering::Relaxed),
            self.tiles_downloaded.load(Ordering::Relaxed),
            status_breakdown.join(", ")
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
        }));

        tiles.for_each_concurrent(downloader.args.concurrency, |(x, y)| {
            let d = downloader.clone();
            async move {
                if let Err(e) = d.download_tile(x, y).await {
                    error!("Error downloading tile {x}/{y}: {e}");
                }
            }
        }).await;

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

        let result = tokio::select! {
            res = &mut download_task => res.map_err(|e| anyhow::anyhow!("Download task panicked: {e}"))?,
            Some(e) = error_rx.recv() => {
                download_task.abort();
                Err(e)
            },
        };

        if result.is_ok() {
            downloader.report_statistics(start_time.elapsed());
        }

        drop(downloader);
        let _ = consumer_handle.await;

        result
    }

    async fn download_tile(&self, x: i32, y: i32) -> Result<()> {
        let url = self.args.url
            .replace("{x}", &x.to_string())
            .replace("{y}", &y.to_string());

        let existing_metadata = self.db.get_tile_metadata(x, y).await?;

        let client = self.get_client();

        // First, check if the tile has changed using HEAD
        self.increment_head_requests();
        let head_res = match client.head(&url).send().await {
            Ok(res) => {
                let status = res.status();
                self.record_status(status.as_u16());
                if status == reqwest::StatusCode::NOT_FOUND {
                    return Ok(());
                }
                res
            }
            Err(e) => {
                self.record_status(0);
                return Err(e.into());
            }
        };
        
        let new_etag = head_res.headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        
        let new_last_modified = head_res.headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if let Some(meta) = existing_metadata {
            if meta.etag == new_etag && meta.last_modified == new_last_modified {
                // Tile unchanged
                return Ok(());
            }
        }

        // If changed or not present, fetch the full image
        self.increment_get_requests();
        let get_res = match client.get(&url).send().await {
            Ok(res) => {
                self.record_status(res.status().as_u16());
                res
            }
            Err(e) => {
                self.record_status(0);
                return Err(e.into());
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

        self.increment_tiles_downloaded();
        info!("Queued tile {x}/{y} for saving");

        Ok(())
    }
}
