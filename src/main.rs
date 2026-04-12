mod cli;
mod db;
mod downloader;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::error;

use crate::cli::Args;
use crate::db::Db;
use crate::downloader::Downloader;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arc::new(Args::parse());

    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Initialize database
    let db_path = args.db_path.clone();
    let parent = db_path.parent().unwrap_or_else(|| std::path::Path::new(""));
    
    if !parent.as_os_str().is_empty() {
        std::fs::create_dir_all(parent)?;
    }

    let abs_path = if db_path.is_absolute() {
        db_path
    } else {
        std::env::current_dir()?.join(&db_path)
    };
    
    let db = Arc::new(Db::init(&abs_path).await?);

    // Initialize clients
    let mut clients = Vec::new();
    if args.bind.is_empty() {
        let client = reqwest::Client::builder()
            .build()?;
        clients.push(client);
    } else {
        for ip in args.bind.iter() {
            let client = reqwest::Client::builder()
                .local_address(*ip)
                .build()?;
            clients.push(client);
        }
    }

    // Run downloader
    let (downloader, rx) = Downloader::new(db, args, clients);
    if let Err(e) = downloader.run(rx).await {
        error!("Downloader error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
