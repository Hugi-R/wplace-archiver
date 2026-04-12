# wplace-archiver

A high-performance Rust-based command-line tool designed to download and archive tiles from wplace.live.

## Features

- **Efficient Downloads**: Uses `HEAD` requests to check `ETag` and `Last-Modified` headers, avoiding unnecessary downloads of unchanged tiles.
- **Concurrency**: Highly concurrent downloading using `tokio` and `futures`.
- **Database Storage**: Saves tiles and their metadata in a SQLite database for easy retrieval and tracking.
- **Network Flexibility**: Supports binding to specific network interfaces (IPv4/IPv6) using a pool of HTTP clients.
- **Customizable**: Allows specifying X/Y ranges, custom URL templates, and concurrency levels.

## Installation

Ensure you have the Rust toolchain installed. Then, clone the repository and build the project:

```bash
cargo build --release
```

The binary will be located in `target/release/wplace-archiver`.

## Usage

Beware of the rate limit! Avoid downloading more than 100 tiles at once.

### Specify X/Y Ranges

To download a specific area:

```bash
./target/release/wplace-archiver --x-range 0-5 --y-range 0-5
```

### Bind to Specific IP Addresses

To use multiple network interfaces for downloading:

```bash
./target/release/wplace-archiver --bind 192.168.1.10 --bind 192.168.1.11
```

Alternatively, you can provide a file containing a list of IP addresses:

```bash
./target/release/wplace-archiver --bind-file addresses.txt
```

## Configuration Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--x-range` | X range in `min-max` format (e.g., `0-100`) | `0-2048` |
| `--y-range` | Y range in `min-max` format (e.g., `0-100`) | `0-2048` |
| `--bind` | List of IP addresses to bind clients to | None |
| `--bind-file` | Path to a file containing a list of IP addresses | None |
| `--db-path` | Path to the SQLite database file | `tiles.db` |
| `--concurrency` | Maximum number of concurrent requests | `10` |
| `--url` | Base URL template (supports `{x}` and `{y}`) | `https://backend.wplace.live/files/s0/tiles/{x}/{y}.png` |

## License

UNLICENSE
