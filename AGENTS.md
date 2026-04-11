# wplace-archiver

The goal of this project is to download tiles from "https://backend.wplace.live/files/s0/tiles/{X}/{Y}.png", where X and Y are between 0 and 2048.
The header is checked for `etag` and and `last-modified` before downloading and saving the tile.
Tiles and their metadata are saved in a SQLite database.

**Inputs:**
- A range of X/Y (optional, default on all). To select area to save.
- A list of IPv4/IPv6 address to bind clients to (optional, default on no binding). To use multiple network interface.