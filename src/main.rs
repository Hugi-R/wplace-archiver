mod cli;
mod db;
mod downloader;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::{error, info};

use crate::cli::Args;
use crate::db::Db;
use crate::downloader::{ClientInfo, Downloader};

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
    let mut bind_ips = args.bind.clone();

    if let Some(ref bind_file) = args.bind_file {
        let content = std::fs::read_to_string(bind_file)
            .map_err(|e| anyhow::anyhow!("Failed to read bind file '{:?}': {}", bind_file, e))?;
        
        for (line_idx, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            
            let ip = trimmed.parse::<std::net::IpAddr>()
                .map_err(|_| anyhow::anyhow!("Invalid IP address '{}' in '{:?}' at line {}", trimmed, bind_file, line_idx + 1))?;
            
            bind_ips.push(ip);
        }
    }

    if bind_ips.is_empty() {
        let client = reqwest::Client::builder()
            .build()?;
        clients.push(ClientInfo {
            client,
            local_address: None,
        });
    } else {
        for ip in bind_ips.iter() {
            let client = reqwest::Client::builder()
                .local_address(*ip)
                .build()?;
            clients.push(ClientInfo {
                client,
                local_address: Some(*ip),
            });
        }
    }
    info!("Initialized {} client(s).", clients.len());

    // Run downloader
    let (downloader, rx) = Downloader::new(db, args, clients);
    if let Err(e) = downloader.run(rx).await {
        error!("Downloader error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
