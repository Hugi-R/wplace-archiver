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

### Specify X/Y Ranges
> Beware of the rate limit! If downloading more than 100 tiles at once, you'll have to wait.

To download a specific area:
```bash
./target/release/wplace-archiver --x-range 0-5 --y-range 0-5
```
The result will be stored inside a SQLite database (by default `tiles.db`).
To retrieve a tile from it (eg: 0-0) as a png (eg: `img.png`), run:
```bash
sqlite3 tiles.db "select writefile('img.png', data) from tiles where x=0 and y=0;"
```

### Archiving the entire site

Alternatively, you can provide a file containing a list of IP addresses to use to download the tiles.

If your server has an IPv6 subnet (which is usually the case, try Scaleway DEV1-S instance), your can create a bunch of addresses on that subnet, then run the archiver.
Essentially bypassing the rate limiting for free.
```bash
# Create addresses and store them in addresses.txt
./create_addresses 500
./target/release/wplace-archiver --concurrency 50 --bind-file addresses.txt
```
Don't create too many addresses (500 is OK, more is not great), you will reach kernel limits, and quickly run out of available ports.

Creating the archive from scratch will take a few hours, as all tiles need to be downloaded. But if you keep `tiles.db` and run the archiver again, only the updated tiles will be downloaded, thus taking less than an hour (all tiles still need to be scanned).

Thanks to Rust, Reqwest+Tokio, and SQLite, the archiver is surprinsingly efficient, I got it running on 1vCPU/1GB instance, with ~20GB of free disk space.

#### Extracting the newly archived tiles as a separate DB
```sql
attach 'extract.db' as extract;
create table extract.tiles (x INTEGER, y INTEGER, data BLOB, PRIMARY KEY (x, y));
insert into extract.tiles (x, y, data) select x, y, data from tiles where datetime(updated_at) > datetime('2026-04-13 06:00:00'); /* change the date */
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

## AI Disclosure
This entire project was built in less than 2 days using `gemma-4-26B-A4B-it-UD-Q4_K_M.gguf` running with llama.cpp on a RTX3090, and Kilo Code as the agent harness.
Quite impressive what you can achieve fully local nowadays, tho the "Plan then Execute in a different session" is essential for it to work.

## License

UNLICENSE
