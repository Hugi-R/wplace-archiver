use crate::cli::Args;
use crate::db::Db;
use anyhow::Result;
use futures::StreamExt;
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{error, info, warn};

pub struct Downloader {
    db: Arc<Db>,
    args: Arc<Args>,
    clients: Vec<Client>,
    next_client_idx: AtomicUsize,
}

impl Downloader {
    pub fn new(db: Arc<Db>, args: Arc<Args>, clients: Vec<Client>) -> Self {
        Self {
            db,
            args,
            clients,
            next_client_idx: AtomicUsize::new(0),
        }
    }

    fn get_client(&self) -> &Client {
        let idx = self.next_client_idx.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }

    pub async fn run(&self) -> Result<()> {
        let (x_min, x_max) = self.args.x_range.unwrap_or((0, 2048));
        let (y_min, y_max) = self.args.y_range.unwrap_or((0, 2048));

        info!("Starting download for X: {} to {}, Y: {} to {}", x_min, x_max, y_min, y_max);

        let tiles = futures::stream::iter((x_min..=x_max).flat_map(|x| {
            (y_min..=y_max).map(move |y| (x, y))
        }));

        tiles.for_each_concurrent(self.args.concurrency, |(x, y)| async move {
            if let Err(e) = self.download_tile(x, y).await {
                error!("Error downloading tile {x}/{y}: {e}");
            }
        }).await;

        info!("Download complete.");
        Ok(())
    }

    async fn download_tile(&self, x: i32, y: i32) -> Result<()> {
        let url = self.args.url
            .replace("{x}", &x.to_string())
            .replace("{y}", &y.to_string());

        let existing_metadata = self.db.get_tile_metadata(x, y).await?;

        let client = self.get_client();

        // First, check if the tile has changed using HEAD
        let head_res = client.head(&url).send().await?;
        
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
        let get_res = client.get(&url).send().await?;
        if !get_res.status().is_success() {
            warn!("Failed to fetch tile {x}/{y}: {}", get_res.status());
            return Err(anyhow::anyhow!("HTTP error: {}", get_res.status()));
        }

        let data = get_res.bytes().await?.to_vec();
        
        self.db.save_tile(x, y, data, new_etag, new_last_modified).await?;
        info!("Saved tile {x}/{y}");

        Ok(())
    }
}
