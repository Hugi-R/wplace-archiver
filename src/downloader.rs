use crate::cli::Args;
use crate::db::Db;
use anyhow::Result;
use futures::StreamExt;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
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
}

impl Downloader {
    pub fn new(db: Arc<Db>, args: Arc<Args>, clients: Vec<Client>) -> Self {
        Self {
            db,
            args,
            clients,
            next_client_idx: AtomicUsize::new(0),
            head_requests: AtomicUsize::new(0),
            get_requests: AtomicUsize::new(0),
            tiles_downloaded: AtomicUsize::new(0),
            status_codes: Mutex::new(HashMap::new()),
        }
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

    pub async fn run(&self) -> Result<()> {
        let (x_min, x_max) = self.args.x_range.unwrap_or((0, 2048));
        let (y_min, y_max) = self.args.y_range.unwrap_or((0, 2048));

        info!("Starting download for X: {} to {}, Y: {} to {}", x_min, x_max, y_min, y_max);

        let start_time = Instant::now();

        let tiles = futures::stream::iter((x_min..=x_max).flat_map(|x| {
            (y_min..=y_max).map(move |y| (x, y))
        }));

        tiles.for_each_concurrent(self.args.concurrency, |(x, y)| async move {
            if let Err(e) = self.download_tile(x, y).await {
                error!("Error downloading tile {x}/{y}: {e}");
            }
        }).await;

        let elapsed = start_time.elapsed();
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

        Ok(())
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
                self.record_status(res.status().as_u16());
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
        
        self.db.save_tile(x, y, data, new_etag, new_last_modified).await?;
        self.increment_tiles_downloaded();
        info!("Saved tile {x}/{y}");

        Ok(())
    }
}
