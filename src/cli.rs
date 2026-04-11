use clap::Parser;
use std::net::IpAddr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// X range in format min-max (e.g. 0-100)
    #[arg(long, value_parser = parse_range)]
    pub x_range: Option<(i32, i32)>,

    /// Y range in format min-max (e.g. 0-100)
    #[arg(long, value_parser = parse_range)]
    pub y_range: Option<(i32, i32)>,

    /// List of IP addresses to bind to
    #[arg(long)]
    pub bind: Vec<IpAddr>,

    /// Path to the SQLite database file
    #[arg(long, default_value = "tiles.db")]
    pub db_path: PathBuf,

    /// Maximum number of concurrent requests
    #[arg(long, default_value_t = 10)]
    pub concurrency: usize,

    /// Base URL template
    #[arg(
        long,
        default_value = "https://backend.wplace.live/files/s0/tiles/{x}/{y}.png"
    )]
    pub url: String,
}

fn parse_range(s: &str) -> Result<(i32, i32), String> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 2 {
        return Err("Range must be in format min-max".to_string());
    }
    let min = parts[0].parse::<i32>().map_err(|_| "Invalid min value")?;
    let max = parts[1].parse::<i32>().map_err(|_| "Invalid max value")?;
    Ok((min, max))
}
