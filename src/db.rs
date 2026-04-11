use sqlx::{sqlite::SqliteConnectOptions, SqlitePool, Pool, Sqlite};
use anyhow::Result;
use std::path::Path;

pub struct TileMetadata {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub struct Db {
    pool: Pool<Sqlite>,
}

impl Db {
    pub async fn init(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path.to_path_buf())
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tiles (
                x INTEGER,
                y INTEGER,
                data BLOB,
                etag TEXT,
                last_modified TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (x, y)
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn get_tile_metadata(&self, x: i32, y: i32) -> Result<Option<TileMetadata>> {
        let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT etag, last_modified FROM tiles WHERE x = ? AND y = ?"
        )
        .bind(x)
        .bind(y)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(etag, last_modified)| TileMetadata { etag, last_modified }))
    }

    pub async fn save_tile(
        &self,
        x: i32,
        y: i32,
        data: Vec<u8>,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO tiles (x, y, data, etag, last_modified, updated_at)
             VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
             ON CONFLICT(x, y) DO UPDATE SET
                data = excluded.data,
                etag = excluded.etag,
                last_modified = excluded.last_modified,
                updated_at = excluded.updated_at"
        )
        .bind(x)
        .bind(y)
        .bind(data)
        .bind(etag)
        .bind(last_modified)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
