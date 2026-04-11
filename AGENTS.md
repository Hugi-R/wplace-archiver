# wplace-archiver

The goal of this project is to download tiles from "https://backend.wplace.live/files/s0/tiles/{X}/{Y}.png", where X and Y are between 0 and 2048.
The header is checked for `etag` and and `last-modified` before downloading and saving the tile.
Tiles and their metadata are saved in a SQLite database.

**Inputs:**
- A range of X/Y (optional, default on all). To select area to save.
- A list of IPv4/IPv6 address to bind clients to (optional, default on no binding). To use multiple network interface.

## Architecture

The `wplace-archiver` is a Rust-based command-line tool designed to download and archive tiles from wplace.live.

### Core Components

- **`main.rs`**: The application entry point. It handles initialization of tracing, the SQLite database, and the HTTP clients, then starts the downloader.
- **`cli.rs`**: Defines the command-line interface using `clap`. It manages configuration such as X/Y ranges, IP binding for network interfaces, database path, concurrency level, and the tile URL template.
- **`db.rs`**: Manages the SQLite database using `sqlx`. It handles database initialization (`tiles` table) and provides methods for retrieving and saving tile metadata (ETag, Last-Modified) and image data.
- **`downloader.rs`**: Implements the core downloading logic. It uses `futures` for concurrent downloads, `reqwest` for HTTP requests, and a round-robin selection of HTTP clients (to support binding to multiple network interfaces). It optimizes downloads by first performing a `HEAD` request to check for changes via `ETag` or `Last-Modified` headers before downloading the full image.

### Data Flow

1. **Initialization**: `main.rs` parses CLI arguments, initializes the database, and creates a pool of `reqwest` clients.
2. **Downloader Execution**: The `Downloader` is instantiated and starts iterating through the specified X/Y coordinate ranges.
3. **Concurrency**: Tiles are processed concurrently up to the specified `concurrency` limit.
4. **Change Detection**: For each tile:
    - A `HEAD` request is sent to the tile URL.
    - The `ETag` and `Last-Modified` headers are compared with the existing metadata in the SQLite database.
    - If the tile is unchanged, the process skips to the next tile.
5. **Download and Save**: If the tile is new or has changed:
    - A `GET` request is performed to download the tile image.
    - The image data and new metadata are saved into the SQLite database using an `UPSERT` operation.

