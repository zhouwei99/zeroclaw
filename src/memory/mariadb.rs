use super::traits::{Memory, MemoryCategory, MemoryEntry};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use mysql::prelude::Queryable;
use mysql::{params, Opts, OptsBuilder, Pool, SslOpts};
use std::time::Duration;
use uuid::Uuid;

/// Maximum allowed connect timeout (seconds) to avoid unreasonable waits.
const MARIADB_CONNECT_TIMEOUT_CAP_SECS: u64 = 300;

/// MariaDB/MySQL-backed persistent memory.
///
/// This backend focuses on reliable CRUD and keyword recall using SQL.
pub struct MariadbMemory {
    pool: Pool,
    qualified_table: String,
}

impl MariadbMemory {
    pub fn new(
        db_url: &str,
        schema: &str,
        table: &str,
        connect_timeout_secs: Option<u64>,
        tls_mode: bool,
    ) -> Result<Self> {
        validate_identifier(table, "storage table")?;

        // Treat "public" as unset for MariaDB/MySQL compatibility because
        // this default originates from PostgreSQL config conventions.
        let schema = normalize_schema(schema);
        if let Some(schema_name) = schema.as_deref() {
            validate_identifier(schema_name, "storage schema")?;
        }

        let table_ident = quote_identifier(table);
        let qualified_table = match schema.as_deref() {
            Some(schema_name) => format!("{}.{}", quote_identifier(schema_name), table_ident),
            None => table_ident,
        };

        let pool = Self::initialize_pool(
            db_url,
            connect_timeout_secs,
            tls_mode,
            schema.as_deref(),
            &qualified_table,
        )?;

        Ok(Self {
            pool,
            qualified_table,
        })
    }

    fn initialize_pool(
        db_url: &str,
        connect_timeout_secs: Option<u64>,
        tls_mode: bool,
        schema: Option<&str>,
        qualified_table: &str,
    ) -> Result<Pool> {
        let db_url = db_url.to_string();
        let schema = schema.map(str::to_string);
        let qualified_table = qualified_table.to_string();

        let init_handle = std::thread::Builder::new()
            .name("mariadb-memory-init".to_string())
            .spawn(move || -> Result<Pool> {
                let mut builder = OptsBuilder::from_opts(
                    Opts::from_url(&db_url).context("invalid MariaDB connection URL")?,
                );

                if let Some(timeout_secs) = connect_timeout_secs {
                    let bounded = timeout_secs.min(MARIADB_CONNECT_TIMEOUT_CAP_SECS);
                    builder = builder.tcp_connect_timeout(Some(Duration::from_secs(bounded)));
                }

                if tls_mode {
                    builder = builder.ssl_opts(Some(SslOpts::default()));
                }

                let pool = Pool::new(builder).context("failed to create MariaDB pool")?;
                let mut conn = pool
                    .get_conn()
                    .context("failed to connect to MariaDB memory backend")?;
                Self::init_schema(&mut conn, schema.as_deref(), &qualified_table)?;
                drop(conn);
                Ok(pool)
            })
            .context("failed to spawn MariaDB initializer thread")?;

        init_handle
            .join()
            .map_err(|_| anyhow::anyhow!("MariaDB initializer thread panicked"))?
    }

    fn init_schema(
        conn: &mut mysql::PooledConn,
        schema: Option<&str>,
        qualified_table: &str,
    ) -> Result<()> {
        if let Some(schema_name) = schema {
            let create_schema = format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                quote_identifier(schema_name)
            );
            conn.query_drop(create_schema)?;
        }

        let create_table = format!(
            "
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                id VARCHAR(64) PRIMARY KEY,
                `key` VARCHAR(255) NOT NULL UNIQUE,
                content LONGTEXT NOT NULL,
                category VARCHAR(64) NOT NULL,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL,
                session_id VARCHAR(255) NULL,
                INDEX idx_memories_category (category),
                INDEX idx_memories_session_id (session_id),
                INDEX idx_memories_updated_at (updated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "
        );
        conn.query_drop(create_table)?;

        Ok(())
    }

    fn category_to_str(category: &MemoryCategory) -> String {
        match category {
            MemoryCategory::Core => "core".to_string(),
            MemoryCategory::Daily => "daily".to_string(),
            MemoryCategory::Conversation => "conversation".to_string(),
            MemoryCategory::Custom(name) => name.clone(),
        }
    }

    fn parse_category(value: &str) -> MemoryCategory {
        match value {
            "core" => MemoryCategory::Core,
            "daily" => MemoryCategory::Daily,
            "conversation" => MemoryCategory::Conversation,
            other => MemoryCategory::Custom(other.to_string()),
        }
    }

    fn row_to_entry(row: mysql::Row) -> Result<MemoryEntry> {
        let id: String = row.get(0).context("missing id column in memory row")?;
        let key: String = row.get(1).context("missing key column in memory row")?;
        let content: String = row.get(2).context("missing content column in memory row")?;
        let category: String = row
            .get(3)
            .context("missing category column in memory row")?;
        let timestamp: String = row
            .get(4)
            .context("missing created_at column in memory row")?;
        let session_id: Option<String> = row.get(5);
        let score: Option<f64> = row.get(6);

        Ok(MemoryEntry {
            id,
            key,
            content,
            category: Self::parse_category(&category),
            timestamp,
            session_id,
            score,
        })
    }
}

fn normalize_schema(schema: &str) -> Option<String> {
    let trimmed = schema.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("public") {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn validate_identifier(value: &str, field_name: &str) -> Result<()> {
    if value.is_empty() {
        anyhow::bail!("{field_name} must not be empty");
    }

    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        anyhow::bail!("{field_name} must not be empty");
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        anyhow::bail!("{field_name} must start with an ASCII letter or underscore; got '{value}'");
    }

    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        anyhow::bail!(
            "{field_name} can only contain ASCII letters, numbers, and underscores; got '{value}'"
        );
    }

    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("`{value}`")
}

#[async_trait]
impl Memory for MariadbMemory {
    fn name(&self) -> &str {
        "mariadb"
    }

    async fn store(
        &self,
        key: &str,
        content: &str,
        category: MemoryCategory,
        session_id: Option<&str>,
    ) -> Result<()> {
        let pool = self.pool.clone();
        let qualified_table = self.qualified_table.clone();
        let key = key.to_string();
        let content = content.to_string();
        let category = Self::category_to_str(&category);
        let session_id = session_id.map(str::to_string);

        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = pool.get_conn()?;
            let now = Utc::now().to_rfc3339();
            let sql = format!(
                "
                INSERT INTO {qualified_table}
                    (id, `key`, content, category, created_at, updated_at, session_id)
                VALUES
                    (:id, :key, :content, :category, :created_at, :updated_at, :session_id)
                ON DUPLICATE KEY UPDATE
                    content = VALUES(content),
                    category = VALUES(category),
                    updated_at = VALUES(updated_at),
                    session_id = VALUES(session_id)
                "
            );

            conn.exec_drop(
                sql,
                params! {
                    "id" => Uuid::new_v4().to_string(),
                    "key" => key,
                    "content" => content,
                    "category" => category,
                    "created_at" => now.clone(),
                    "updated_at" => now,
                    "session_id" => session_id,
                },
            )?;
            Ok(())
        })
        .await?
    }

    async fn recall(
        &self,
        query: &str,
        limit: usize,
        session_id: Option<&str>,
    ) -> Result<Vec<MemoryEntry>> {
        let pool = self.pool.clone();
        let qualified_table = self.qualified_table.clone();
        let query = query.trim().to_string();
        let session_id = session_id.map(str::to_string);

        tokio::task::spawn_blocking(move || -> Result<Vec<MemoryEntry>> {
            let mut conn = pool.get_conn()?;
            let sql = format!(
                "
                SELECT
                    id,
                    `key`,
                    content,
                    category,
                    created_at,
                    session_id,
                    (
                        CASE WHEN LOWER(`key`) LIKE CONCAT('%', LOWER(:query), '%') THEN 2.0 ELSE 0.0 END +
                        CASE WHEN LOWER(content) LIKE CONCAT('%', LOWER(:query), '%') THEN 1.0 ELSE 0.0 END
                    ) AS score
                FROM {qualified_table}
                WHERE (:session_id IS NULL OR session_id = :session_id)
                  AND (
                    :query = '' OR
                    LOWER(`key`) LIKE CONCAT('%', LOWER(:query), '%') OR
                    LOWER(content) LIKE CONCAT('%', LOWER(:query), '%')
                  )
                ORDER BY score DESC, updated_at DESC
                LIMIT :limit
                "
            );

            #[allow(clippy::cast_possible_wrap)]
            let limit_i64 = limit as i64;

            let rows = conn.exec(
                sql,
                params! {
                    "query" => query,
                    "session_id" => session_id,
                    "limit" => limit_i64,
                },
            )?;

            rows.into_iter()
                .map(Self::row_to_entry)
                .collect::<Result<Vec<MemoryEntry>>>()
        })
        .await?
    }

    async fn get(&self, key: &str) -> Result<Option<MemoryEntry>> {
        let pool = self.pool.clone();
        let qualified_table = self.qualified_table.clone();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || -> Result<Option<MemoryEntry>> {
            let mut conn = pool.get_conn()?;
            let sql = format!(
                "
                SELECT id, `key`, content, category, created_at, session_id
                FROM {qualified_table}
                WHERE `key` = :key
                LIMIT 1
                "
            );

            let row = conn.exec_first(sql, params! { "key" => key })?;
            row.map(Self::row_to_entry).transpose()
        })
        .await?
    }

    async fn list(
        &self,
        category: Option<&MemoryCategory>,
        session_id: Option<&str>,
    ) -> Result<Vec<MemoryEntry>> {
        let pool = self.pool.clone();
        let qualified_table = self.qualified_table.clone();
        let category = category.map(Self::category_to_str);
        let session_id = session_id.map(str::to_string);

        tokio::task::spawn_blocking(move || -> Result<Vec<MemoryEntry>> {
            let mut conn = pool.get_conn()?;
            let sql = format!(
                "
                SELECT id, `key`, content, category, created_at, session_id
                FROM {qualified_table}
                WHERE (:category IS NULL OR category = :category)
                  AND (:session_id IS NULL OR session_id = :session_id)
                ORDER BY updated_at DESC
                "
            );

            let rows = conn.exec(
                sql,
                params! {
                    "category" => category,
                    "session_id" => session_id,
                },
            )?;

            rows.into_iter()
                .map(Self::row_to_entry)
                .collect::<Result<Vec<MemoryEntry>>>()
        })
        .await?
    }

    async fn forget(&self, key: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let qualified_table = self.qualified_table.clone();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut conn = pool.get_conn()?;
            let sql = format!("DELETE FROM {qualified_table} WHERE `key` = :key");
            conn.exec_drop(sql, params! { "key" => key })?;
            Ok(conn.affected_rows() > 0)
        })
        .await?
    }

    async fn count(&self) -> Result<usize> {
        let pool = self.pool.clone();
        let qualified_table = self.qualified_table.clone();

        tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut conn = pool.get_conn()?;
            let sql = format!("SELECT COUNT(*) FROM {qualified_table}");
            let count: Option<i64> = conn.query_first(sql)?;
            let count = count.unwrap_or(0);
            let count =
                usize::try_from(count).context("MariaDB returned a negative memory count")?;
            Ok(count)
        })
        .await?
    }

    async fn health_check(&self) -> bool {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || -> bool {
            match pool.get_conn() {
                Ok(mut conn) => conn.query_drop("SELECT 1").is_ok(),
                Err(_) => false,
            }
        })
        .await
        .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_identifiers_pass_validation() {
        assert!(validate_identifier("memories", "table").is_ok());
        assert!(validate_identifier("_memories_01", "table").is_ok());
    }

    #[test]
    fn invalid_identifiers_are_rejected() {
        assert!(validate_identifier("", "schema").is_err());
        assert!(validate_identifier("1bad", "schema").is_err());
        assert!(validate_identifier("bad-name", "table").is_err());
    }

    #[test]
    fn parse_category_maps_known_and_custom_values() {
        assert_eq!(MariadbMemory::parse_category("core"), MemoryCategory::Core);
        assert_eq!(
            MariadbMemory::parse_category("daily"),
            MemoryCategory::Daily
        );
        assert_eq!(
            MariadbMemory::parse_category("conversation"),
            MemoryCategory::Conversation
        );
        assert_eq!(
            MariadbMemory::parse_category("custom_notes"),
            MemoryCategory::Custom("custom_notes".into())
        );
    }

    #[test]
    fn normalize_schema_handles_default_postgres_schema() {
        assert!(normalize_schema("").is_none());
        assert!(normalize_schema("public").is_none());
        assert!(normalize_schema("PUBLIC").is_none());
        assert_eq!(normalize_schema("zeroclaw"), Some("zeroclaw".into()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn new_does_not_panic_inside_tokio_runtime() {
        let outcome = std::panic::catch_unwind(|| {
            MariadbMemory::new(
                "mysql://zeroclaw:password@127.0.0.1:1/zeroclaw",
                "public",
                "memories",
                Some(1),
                false,
            )
        });

        assert!(outcome.is_ok(), "MariadbMemory::new should not panic");
        assert!(
            outcome.unwrap().is_err(),
            "MariadbMemory::new should return a connect error for an unreachable endpoint"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn integration_roundtrip_when_test_db_is_configured() {
        let Some(db_url) = std::env::var("ZEROCLAW_TEST_MARIADB_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!("Skipping MariaDB integration test: set ZEROCLAW_TEST_MARIADB_URL to enable");
            return;
        };

        let schema = format!("zeroclaw_test_{}", Uuid::new_v4().simple());
        let memory = MariadbMemory::new(&db_url, &schema, "memories", Some(5), false)
            .expect("should initialize MariaDB memory backend");

        memory
            .store(
                "integration_key",
                "integration content",
                MemoryCategory::Conversation,
                None,
            )
            .await
            .expect("store should succeed");

        let fetched = memory
            .get("integration_key")
            .await
            .expect("get should succeed")
            .expect("entry should exist");
        assert_eq!(fetched.content, "integration content");

        let recalled = memory
            .recall("integration", 5, None)
            .await
            .expect("recall should succeed");
        assert!(
            recalled.iter().any(|entry| entry.key == "integration_key"),
            "recall should return the stored key"
        );
    }
}
