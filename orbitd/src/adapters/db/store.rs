// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::{net::IpAddr, path::Path, str::FromStr, time::Duration};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::app::types::{
    Address, BlueprintRecord, Distro, HostRecord, JobRecord, NewBlueprintBuild, NewHost, NewJob,
    SlurmVersion,
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Error)]
pub enum HostStoreError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid address (both hostname and ip are missing)")]
    InvalidAddress,
    #[error("empty name")]
    EmptyName,
    #[error("empty blueprint name")]
    EmptyBlueprintName,
    #[error("empty blueprint tarball hash")]
    EmptyBlueprintTarballHash,
    #[error("empty blueprint tarball hash function")]
    EmptyBlueprintTarballHashFunction,
    #[error("empty blueprint tool version")]
    EmptyBlueprintToolVersion,
    #[error("host not found: {0}")]
    HostNotFound(String),
}

// This structure is used for inserting data into the db when data first appears
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NewPartition {
    /// Slurm partition name
    pub name: String,
    /// Arbitrary metadata
    pub info: Option<serde_json::Value>,
}

// This structure is returned from the db
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PartitionRecord {
    pub id: i64,
    pub host_id: i64,
    pub name: String,
    pub info: Option<serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
}
pub type Result<T> = std::result::Result<T, HostStoreError>;

/// Async store
/// TODO: since it stores not only hosts but also partitions, jobs etc., this needs to be renamed.
#[derive(Clone)]
pub struct HostStore {
    pool: SqlitePool,
}

impl HostStore {
    /// Open (or create) a file-backed SQLite DB.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let url = format!("sqlite://{}", path_ref.to_string_lossy());
        let opts = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;
        let store = Self { pool };
        store.bootstrap().await?;
        Ok(store)
    }

    /// Open an in-memory store (handy for tests).
    #[allow(dead_code)]
    pub async fn open_memory() -> Result<Self> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        let store = Self { pool };
        store.bootstrap().await?;
        Ok(store)
    }

    async fn bootstrap(&self) -> Result<()> {
        // Improve concurrency for file DBs.
        let _ = sqlx::query("PRAGMA journal_mode=WAL;")
            .execute(&self.pool)
            .await;

        self.ensure_hosts_table().await?;
        self.ensure_partitions_table().await?;
        self.ensure_jobs_table().await?;
        self.ensure_blueprints_table().await?;
        Ok(())
    }

    async fn ensure_hosts_table(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS hosts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT,                       -- unique short id; made UNIQUE/indexed below
              username TEXT NOT NULL,
              hostname TEXT,
              ip TEXT,
              port INTEGER NOT NULL,
              identity_path TEXT,
              slurm_major INTEGER NOT NULL,
              slurm_minor INTEGER NOT NULL,
              slurm_patch INTEGER NOT NULL,
              distro_name TEXT NOT NULL,
              distro_version TEXT NOT NULL,
              kernel_version TEXT NOT NULL,
              accounting_available INTEGER NOT NULL,
              default_base_path TEXT,
              default_scratch_directory TEXT,
              is_default INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              CHECK (hostname IS NOT NULL OR ip IS NOT NULL)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hosts_name ON hosts(name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hosts_user_addr
              ON hosts(username, COALESCE(hostname, ip), port);
            CREATE INDEX IF NOT EXISTS idx_hosts_hostname ON hosts(hostname);
            CREATE INDEX IF NOT EXISTS idx_hosts_ip ON hosts(ip);
            CREATE INDEX IF NOT EXISTS idx_hosts_username ON hosts(username);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hosts_single_default
              ON hosts(is_default)
              WHERE is_default = 1;
            "#,
        )
        .execute(&self.pool)
        .await?;
        // TODO: move migrations into a separate function.
        let columns: Vec<String> = sqlx::query("PRAGMA table_info(hosts)")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .filter_map(|row| row.try_get::<String, _>("name").ok())
            .collect();
        if !columns.iter().any(|name| name == "default_base_path") {
            sqlx::query("ALTER TABLE hosts ADD COLUMN default_base_path TEXT")
                .execute(&self.pool)
                .await?;
        }
        if !columns
            .iter()
            .any(|name| name == "default_scratch_directory")
        {
            sqlx::query("ALTER TABLE hosts ADD COLUMN default_scratch_directory TEXT")
                .execute(&self.pool)
                .await?;
        }
        if !columns.iter().any(|name| name == "is_default") {
            sqlx::query("ALTER TABLE hosts ADD COLUMN is_default INTEGER NOT NULL DEFAULT 0")
                .execute(&self.pool)
                .await?;
        }

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hosts_single_default
              ON hosts(is_default)
              WHERE is_default = 1
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn ensure_partitions_table(&self) -> Result<()> {
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS partitions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          host_id INTEGER NOT NULL
            REFERENCES hosts(id) ON DELETE CASCADE,
          name TEXT NOT NULL,
          info TEXT, -- JSON (stringified)
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          UNIQUE(host_id, name)
        );
        CREATE INDEX IF NOT EXISTS idx_partitions_host_id ON partitions(host_id);
        CREATE INDEX IF NOT EXISTS idx_partitions_name    ON partitions(name);
    "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn ensure_jobs_table(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
            id integer primary key autoincrement,
            scheduler_id integer,
            host_id integer not null references hosts(id) on delete cascade,
            local_path TEXT,
            remote_path TEXT NOT NULL,
            stdout_path TEXT NOT NULL,
            stderr_path TEXT,
            blueprint_name TEXT,
            default_retrieve_path TEXT,
            template_values TEXT,
            is_completed boolean default 0,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            completed_at text,
            terminal_state text,
            scheduler_state text,
            check (local_path is not null or blueprint_name is not null));
            CREATE INDEX IF NOT EXISTS idx_jobs_host_id ON jobs(host_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_scheduler_id_host_id ON jobs(scheduler_id,host_id);
            
    "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn ensure_blueprints_table(&self) -> Result<()> {
        // Important: blueprint name also contains version. it's stored as name:tag.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blueprints (
              name TEXT PRIMARY KEY,
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              tarball_hash TEXT,
              tarball_hash_function TEXT NOT NULL DEFAULT 'blake3',
              tool_version TEXT,
              template_config_json TEXT,
              submit_sbatch_script TEXT,
              sbatch_scripts TEXT,
              default_retrieve_path TEXT,
              sync_include TEXT,
              sync_exclude TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_blueprints_updated_at ON blueprints(updated_at DESC);
            "#,
        )
        .execute(&self.pool)
        .await?;
        let columns: Vec<String> = sqlx::query("PRAGMA table_info(blueprints)")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .filter_map(|row| row.try_get::<String, _>("name").ok())
            .collect();
        if columns.iter().any(|column| column == "path") {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                r#"
                CREATE TABLE blueprints_new (
                  name TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                  tarball_hash TEXT,
                  tarball_hash_function TEXT NOT NULL DEFAULT 'blake3',
                  tool_version TEXT,
                  template_config_json TEXT,
                  submit_sbatch_script TEXT,
                  sbatch_scripts TEXT,
                  default_retrieve_path TEXT,
                  sync_include TEXT,
                  sync_exclude TEXT
                )
                "#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO blueprints_new(
                  name, created_at, updated_at, tarball_hash, tarball_hash_function,
                  tool_version, template_config_json, submit_sbatch_script, sbatch_scripts,
                  default_retrieve_path, sync_include, sync_exclude
                )
                SELECT
                  name, created_at, updated_at, tarball_hash, COALESCE(tarball_hash_function, 'blake3'),
                  tool_version, template_config_json, submit_sbatch_script, sbatch_scripts,
                  default_retrieve_path, sync_include, sync_exclude
                FROM blueprints
                "#,
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query("DROP TABLE blueprints")
                .execute(&mut *tx)
                .await?;
            sqlx::query("ALTER TABLE blueprints_new RENAME TO blueprints")
                .execute(&mut *tx)
                .await?;
            sqlx::query("CREATE INDEX idx_blueprints_updated_at ON blueprints(updated_at DESC)")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        Ok(())
    }
    /// Insert a new host. Returns the new row id.
    pub async fn insert_host(&self, host: &NewHost) -> Result<i64> {
        if host.name.trim().is_empty() {
            return Err(HostStoreError::EmptyName);
        }

        let (hostname, ip) = match &host.address {
            Address::Hostname(h) => (Some(h.as_str()), None),
            Address::Ip(ip) => (None, Some(ip.to_string())),
        };
        if hostname.is_none() && ip.is_none() {
            return Err(HostStoreError::InvalidAddress);
        }

        let rec = sqlx::query(
            r#"
            INSERT INTO hosts(
              name,
              username, hostname, ip,
              slurm_major, slurm_minor, slurm_patch,
              distro_name, distro_version, kernel_version,
              port, identity_path, accounting_available, default_base_path, default_scratch_directory, is_default
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            "#,
        )
        .bind(&host.name)
        .bind(&host.username)
        .bind(hostname)
        .bind(ip.as_deref())
        .bind(host.slurm.major)
        .bind(host.slurm.minor)
        .bind(host.slurm.patch)
        .bind(&host.distro.name)
        .bind(&host.distro.version)
        .bind(&host.kernel_version)
        .bind(host.port)
        .bind(&host.identity_path)
        .bind(host.accounting_available)
        .bind(&host.default_base_path)
        .bind(&host.default_scratch_directory)
        .bind(host.is_default)
        .fetch_one(&self.pool)
        .await?;

        Ok(rec.try_get::<i64, _>("id")?)
    }

    /// Upsert priority:
    /// 1) If a row with `name` exists, update it.
    /// 2) Else, if a row with (username, address, port) exists, update it and set/replace name.
    /// 3) Else, insert a new row.
    pub async fn upsert_host(&self, host: &NewHost) -> Result<i64> {
        if let Some(id) = self.find_id_by_name(&host.name).await? {
            if host.is_default {
                self.clear_default_flags(Some(id)).await?;
            }
            self.update_host(id, host).await?;
            return Ok(id);
        }
        if let Some(id) = self
            .find_id_by_user_and_address(&host.username, &host.address, host.port)
            .await?
        {
            if host.is_default {
                self.clear_default_flags(Some(id)).await?;
            }
            self.update_host(id, host).await?;
            return Ok(id);
        }
        if host.is_default {
            self.clear_default_flags(None).await?;
        }
        self.insert_host(host).await
    }

    /// Update a host by id with the values from `NewHost`.
    pub async fn update_host(&self, id: i64, host: &NewHost) -> Result<()> {
        if host.name.trim().is_empty() {
            return Err(HostStoreError::EmptyName);
        }
        let (hostname, ip) = match &host.address {
            Address::Hostname(h) => (Some(h.as_str()), None),
            Address::Ip(ip) => (None, Some(ip.to_string())),
        };
        if hostname.is_none() && ip.is_none() {
            return Err(HostStoreError::InvalidAddress);
        }

        let now = now_rfc3339();
        sqlx::query(
            r#"
            UPDATE hosts SET
              name = ?1,
              username = ?2,
              hostname = ?3,
              ip = ?4,
              slurm_major = ?5,
              slurm_minor = ?6,
              slurm_patch = ?7,
              distro_name = ?8,
              distro_version = ?9,
              kernel_version = ?10,
              updated_at = ?11,
              port = ?12,
              identity_path = ?13,
              accounting_available = ?14,
              default_base_path = ?15,
              default_scratch_directory = ?16,
              is_default = ?17
            WHERE id = ?18
            "#,
        )
        .bind(&host.name)
        .bind(&host.username)
        .bind(hostname)
        .bind(ip.as_deref())
        .bind(host.slurm.major)
        .bind(host.slurm.minor)
        .bind(host.slurm.patch)
        .bind(&host.distro.name)
        .bind(&host.distro.version)
        .bind(&host.kernel_version)
        .bind(now)
        .bind(host.port)
        .bind(&host.identity_path)
        .bind(host.accounting_available)
        .bind(&host.default_base_path)
        .bind(&host.default_scratch_directory)
        .bind(host.is_default)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Delete a host by numeric id. Returns rows affected (0 or 1).
    #[allow(dead_code)]
    pub async fn delete_host(&self, id: i64) -> Result<usize> {
        let res = sqlx::query("DELETE FROM hosts WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() as usize)
    }

    /// Delete by `name`. Returns rows affected (0 or 1).
    #[allow(dead_code)]
    pub async fn delete_by_name(&self, name: &str) -> Result<usize> {
        let res = sqlx::query("DELETE FROM hosts WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() as usize)
    }

    /// Get a host by row id.
    #[allow(dead_code)]
    pub async fn get_host(&self, id: i64) -> Result<Option<HostRecord>> {
        let row = sqlx::query("select * from hosts where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(row_to_host))
    }

    /// Get a host by human-readable `name` (fast, uses unique index).
    pub async fn get_by_name(&self, name: &str) -> Result<Option<HostRecord>> {
        let row = sqlx::query("select * from hosts where name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(row_to_host))
    }

    /// Get by (username, hostname, port) or (username, ip, port).
    #[allow(dead_code)]
    pub async fn get_by_user_and_address(
        &self,
        username: &str,
        address: &Address,
        port: u16,
    ) -> Result<Option<HostRecord>> {
        let row = match address {
            Address::Hostname(h) => {
                sqlx::query("SELECT * FROM hosts WHERE username = ? AND hostname = ? AND port = ?")
                    .bind(username)
                    .bind(h)
                    .bind(port)
                    .fetch_optional(&self.pool)
                    .await?
            }
            Address::Ip(ip) => {
                let ip_s = ip.to_string();
                sqlx::query("SELECT * FROM hosts WHERE username = ? AND ip = ? AND port = ?")
                    .bind(username)
                    .bind(&ip_s)
                    .bind(port)
                    .fetch_optional(&self.pool)
                    .await?
            }
        };
        Ok(row.map(row_to_host))
    }

    /// List all hosts (optionally filter by username).
    pub async fn list_hosts(&self, username: Option<&str>) -> Result<Vec<HostRecord>> {
        let mut out = Vec::new();
        let mut rows = if let Some(u) = username {
            sqlx::query("SELECT * FROM hosts WHERE username = ? ORDER BY id ASC")
                .bind(u)
                .fetch(&self.pool)
        } else {
            sqlx::query("SELECT * FROM hosts ORDER BY id ASC").fetch(&self.pool)
        };

        use futures_util::TryStreamExt;
        while let Some(row) = rows.try_next().await? {
            out.push(row_to_host(row));
        }
        Ok(out)
    }

    async fn clear_default_flags(&self, except_id: Option<i64>) -> Result<()> {
        match except_id {
            Some(id) => {
                sqlx::query("UPDATE hosts SET is_default = 0 WHERE is_default = 1 AND id != ?")
                    .bind(id)
                    .execute(&self.pool)
                    .await?;
            }
            None => {
                sqlx::query("UPDATE hosts SET is_default = 0 WHERE is_default = 1")
                    .execute(&self.pool)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn upsert_blueprint(&self, name: &str) -> Result<BlueprintRecord> {
        let trimmed_name = name.trim();
        if trimmed_name.is_empty() {
            return Err(HostStoreError::EmptyBlueprintName);
        }

        let row = sqlx::query(
            r#"
            INSERT INTO blueprints(name)
            VALUES (?1)
            ON CONFLICT(name) DO UPDATE SET
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            RETURNING name, created_at, updated_at,
                      tarball_hash, tarball_hash_function, tool_version, template_config_json,
                      submit_sbatch_script, sbatch_scripts,
                      default_retrieve_path, sync_include, sync_exclude
            "#,
        )
        .bind(trimmed_name)
        .fetch_one(&self.pool)
        .await?;
        Ok(row_to_blueprint(row))
    }

    pub async fn upsert_blueprint_build(
        &self,
        build: &NewBlueprintBuild,
    ) -> Result<BlueprintRecord> {
        let trimmed_name = build.name.trim();
        if trimmed_name.is_empty() {
            return Err(HostStoreError::EmptyBlueprintName);
        }
        if build.tarball_hash.trim().is_empty() {
            return Err(HostStoreError::EmptyBlueprintTarballHash);
        }
        if build.tarball_hash_function.trim().is_empty() {
            return Err(HostStoreError::EmptyBlueprintTarballHashFunction);
        }
        if build.tool_version.trim().is_empty() {
            return Err(HostStoreError::EmptyBlueprintToolVersion);
        }

        let sbatch_scripts = serialize_string_list(&build.sbatch_scripts)?;
        let sync_include = serialize_string_list(&build.sync_include)?;
        let sync_exclude = serialize_string_list(&build.sync_exclude)?;

        let row = sqlx::query(
            r#"
            INSERT INTO blueprints(
              name,
              tarball_hash,
              tarball_hash_function,
              tool_version,
              template_config_json,
              submit_sbatch_script,
              sbatch_scripts,
              default_retrieve_path,
              sync_include,
              sync_exclude
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(name) DO UPDATE SET
              tarball_hash = excluded.tarball_hash,
              tarball_hash_function = excluded.tarball_hash_function,
              tool_version = excluded.tool_version,
              template_config_json = excluded.template_config_json,
              submit_sbatch_script = excluded.submit_sbatch_script,
              sbatch_scripts = excluded.sbatch_scripts,
              default_retrieve_path = excluded.default_retrieve_path,
              sync_include = excluded.sync_include,
              sync_exclude = excluded.sync_exclude,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            RETURNING name, created_at, updated_at,
                      tarball_hash, tarball_hash_function, tool_version, template_config_json,
                      submit_sbatch_script, sbatch_scripts,
                      default_retrieve_path, sync_include, sync_exclude
            "#,
        )
        .bind(trimmed_name)
        .bind(build.tarball_hash.trim())
        .bind(build.tarball_hash_function.trim())
        .bind(build.tool_version.trim())
        .bind(build.template_config_json.as_deref())
        .bind(build.submit_sbatch_script.as_deref())
        .bind(sbatch_scripts.as_deref())
        .bind(build.default_retrieve_path.as_deref())
        .bind(sync_include.as_deref())
        .bind(sync_exclude.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(row_to_blueprint(row))
    }

    pub async fn get_blueprint_by_name(&self, name: &str) -> Result<Option<BlueprintRecord>> {
        let row = sqlx::query(
            r#"
            SELECT name, created_at, updated_at,
                   tarball_hash, tarball_hash_function, tool_version, template_config_json,
                   submit_sbatch_script, sbatch_scripts,
                   default_retrieve_path, sync_include, sync_exclude
              FROM blueprints
             WHERE name = ?1
            "#,
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_blueprint))
    }

    pub async fn get_latest_blueprint_build(
        &self,
        blueprint_name: &str,
    ) -> Result<Option<BlueprintRecord>> {
        let escaped = escape_like(blueprint_name);
        let pattern = format!("{escaped}:%");
        let latest_name = format!("{blueprint_name}:latest");
        let row = sqlx::query(
            r#"
            SELECT name, created_at, updated_at,
                   tarball_hash, tarball_hash_function, tool_version, template_config_json,
                   submit_sbatch_script, sbatch_scripts,
                   default_retrieve_path, sync_include, sync_exclude
              FROM blueprints
             WHERE name LIKE ?1 ESCAPE '\'
               AND name != ?2
             ORDER BY updated_at DESC
             LIMIT 1
            "#,
        )
        .bind(pattern)
        .bind(latest_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_blueprint))
    }

    pub async fn list_blueprints(&self) -> Result<Vec<BlueprintRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT name, created_at, updated_at,
                   tarball_hash, tarball_hash_function, tool_version, template_config_json,
                   submit_sbatch_script, sbatch_scripts,
                   default_retrieve_path, sync_include, sync_exclude
              FROM blueprints
             WHERE name NOT LIKE '%:latest'
             ORDER BY name ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_blueprint).collect())
    }

    pub async fn delete_blueprint_by_name(&self, name: &str) -> Result<usize> {
        let result = sqlx::query(
            r#"
            DELETE FROM blueprints
             WHERE name = ?1
            "#,
        )
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn delete_blueprints_by_base_name(&self, name: &str) -> Result<usize> {
        let escaped = escape_like(name);
        let pattern = format!("{escaped}:%");
        let result = sqlx::query(
            r#"
            DELETE FROM blueprints
             WHERE name = ?1
                OR name LIKE ?2 ESCAPE '\'
            "#,
        )
        .bind(name)
        .bind(pattern)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn max_build_number_for_date(
        &self,
        blueprint_name: &str,
        date_prefix: &str,
    ) -> Result<Option<u16>> {
        let escaped = escape_like(blueprint_name);
        let pattern = format!("{escaped}:{date_prefix}.%");
        let rows = sqlx::query(
            r#"
            SELECT name
              FROM blueprints
             WHERE name LIKE ?1 ESCAPE '\'
            "#,
        )
        .bind(pattern)
        .fetch_all(&self.pool)
        .await?;

        let mut max: Option<u16> = None;
        for row in rows {
            let name: String = row.try_get("name")?;
            let tag = match name.split_once(':') {
                Some((_proj, tag)) => tag,
                None => continue,
            };
            let Some((date, suffix)) = tag.split_once('.') else {
                continue;
            };
            if date != date_prefix {
                continue;
            }
            if suffix.len() != 3 || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
                continue;
            }
            if let Ok(value) = suffix.parse::<u16>() {
                max = Some(max.map_or(value, |current| current.max(value)));
            }
        }

        Ok(max)
    }

    // --- internals ---

    async fn find_id_by_name(&self, name: &str) -> Result<Option<i64>> {
        let row = sqlx::query("SELECT id FROM hosts WHERE name = ? LIMIT 1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.try_get::<i64, _>("id").unwrap()))
    }

    async fn find_id_by_user_and_address(
        &self,
        username: &str,
        address: &Address,
        port: u16,
    ) -> Result<Option<i64>> {
        let row =
            match address {
                Address::Hostname(h) => sqlx::query(
                    "SELECT id FROM hosts WHERE username = ? AND hostname = ? AND port = ? LIMIT 1",
                )
                .bind(username)
                .bind(h)
                .bind(port)
                .fetch_optional(&self.pool)
                .await?,
                Address::Ip(ip) => {
                    let ip_s = ip.to_string();
                    sqlx::query(
                        "SELECT id FROM hosts WHERE username = ? AND ip = ? AND port = ? LIMIT 1",
                    )
                    .bind(username)
                    .bind(&ip_s)
                    .bind(port)
                    .fetch_optional(&self.pool)
                    .await?
                }
            };
        Ok(row.map(|r| r.try_get::<i64, _>("id").unwrap()))
    }
    #[allow(dead_code)]
    pub async fn upsert_partition_by_name(&self, name: &str, spec: &NewPartition) -> Result<i64> {
        let host_id = self
            .find_id_by_name(name)
            .await?
            .ok_or_else(|| HostStoreError::HostNotFound(name.into()))?;
        let info_text = spec.info.as_ref().map(|v| v.to_string());
        let rec = sqlx::query(r#"
        INSERT INTO partitions(host_id, name, info)
        VALUES (?1, ?2, ?3)
        ON CONFLICT (host_id, name)
        DO UPDATE SET
            info = excluded.info, -- excluded is sqlite name for table with values that would be inserted if conflict didn't happen
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        RETURNING id;"#).bind(host_id).bind(&spec.name).bind(&info_text).fetch_one(&self.pool).await?;
        Ok(rec.try_get::<i64, _>("id")?)
    }
    #[allow(dead_code)]
    pub async fn get_partition_by_name_and_partition_name(
        &self,
        name: &str,
        partition_name: &str,
    ) -> Result<Option<PartitionRecord>> {
        let row = sqlx::query(
            r#"
        SELECT p.*
        FROM partitions AS p
        JOIN hosts AS h ON h.id = p.host_id
        WHERE h.name = ?1 AND p.name = ?2
        LIMIT 1
    "#,
        )
        .bind(name)
        .bind(partition_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_partition))
    }
    #[allow(dead_code)]
    pub async fn list_partitions_by_name(&self, name: &str) -> Result<Vec<PartitionRecord>> {
        let rows = sqlx::query(
            r#"
        SELECT p.*
        FROM partitions p
        JOIN hosts h ON h.id = p.host_id
        WHERE h.name = ?1
        ORDER BY p.name ASC
    "#,
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(row_to_partition).collect())
    }
    #[allow(dead_code)]
    pub async fn replace_partitions_by_name(
        &self,
        name: &str,
        parts: &[NewPartition],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let host_id = self
            .find_id_by_name(name)
            .await?
            .ok_or_else(|| HostStoreError::HostNotFound(name.to_string()))?;

        sqlx::query("DELETE FROM partitions WHERE host_id = ?")
            .bind(host_id)
            .execute(&mut *tx)
            .await?;

        for p in parts {
            let info_text = p.info.as_ref().map(|v| v.to_string());
            sqlx::query(
                r#"
            INSERT INTO partitions(host_id, name, info)
            VALUES (?1, ?2, ?3)
        "#,
            )
            .bind(host_id)
            .bind(&p.name)
            .bind(info_text)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
    /// fetch a host (by name) along with all its partitions
    #[allow(dead_code)]
    pub async fn get_host_with_partitions_by_name(
        &self,
        name: &str,
    ) -> Result<Option<(HostRecord, Vec<PartitionRecord>)>> {
        let host = self.get_by_name(name).await?;
        let Some(host) = host else { return Ok(None) };
        let parts = self.list_partitions_by_name(name).await?;
        Ok(Some((host, parts)))
    }

    pub async fn insert_job(&self, job: &NewJob) -> Result<i64> {
        let rec = sqlx::query(
            r#"
        insert into jobs(
            scheduler_id, host_id, local_path, remote_path, stdout_path, stderr_path,
            blueprint_name, default_retrieve_path, template_values
        )
        values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        returning id;
    "#,
        )
        .bind(job.scheduler_id)
        .bind(job.host_id)
        .bind(job.local_path.clone())
        .bind(job.remote_path.clone())
        .bind(job.stdout_path.clone())
        .bind(job.stderr_path.clone())
        .bind(job.blueprint_name.clone())
        .bind(job.default_retrieve_path.clone())
        .bind(job.template_values.clone())
        .fetch_one(&self.pool)
        .await?;
        Ok(rec.try_get::<i64, _>("id")?)
    }

    pub async fn latest_remote_path_for_local_path(
        &self,
        host_name: &str,
        local_path: &str,
        template_values: Option<&str>,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            r#"
            select j.remote_path as remote_path
            from jobs j
            join hosts h on j.host_id = h.id
            where h.name = ?1 and j.local_path = ?2 and j.template_values is ?3
            order by j.id desc
            limit 1
            "#,
        )
        .bind(host_name)
        .bind(local_path)
        .bind(template_values)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.try_get::<String, _>("remote_path").unwrap()))
    }

    pub async fn latest_remote_path_for_blueprint(
        &self,
        host_name: &str,
        blueprint_name: &str,
        template_values: Option<&str>,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            r#"
            select j.remote_path as remote_path
            from jobs j
            join hosts h on j.host_id = h.id
            where h.name = ?1 and j.blueprint_name = ?2 and j.template_values is ?3
            order by j.id desc
            limit 1
            "#,
        )
        .bind(host_name)
        .bind(blueprint_name)
        .bind(template_values)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.try_get::<String, _>("remote_path").unwrap()))
    }

    pub async fn running_job_id_for_remote_path(
        &self,
        host_name: &str,
        remote_path: &str,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            r#"
            select j.id as id
            from jobs j
            join hosts h on j.host_id = h.id
            where h.name = ?1 and j.remote_path = ?2 and j.is_completed = 0
            order by j.id desc
            limit 1
            "#,
        )
        .bind(host_name)
        .bind(remote_path)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.try_get::<i64, _>("id").unwrap()))
    }

    pub async fn running_job_id_for_local_path(
        &self,
        host_name: &str,
        local_path: &str,
        template_values: Option<&str>,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            r#"
            select j.id as id
            from jobs j
            join hosts h on j.host_id = h.id
            where h.name = ?1 and j.local_path = ?2 and j.template_values is ?3 and j.is_completed = 0
            order by j.id desc
            limit 1
            "#,
        )
        .bind(host_name)
        .bind(local_path)
        .bind(template_values)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.try_get::<i64, _>("id").unwrap()))
    }

    pub async fn running_job_id_for_blueprint(
        &self,
        host_name: &str,
        blueprint_name: &str,
        template_values: Option<&str>,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            r#"
            select j.id as id
            from jobs j
            join hosts h on j.host_id = h.id
            where h.name = ?1 and j.blueprint_name = ?2 and j.template_values is ?3 and j.is_completed = 0
            order by j.id desc
            limit 1
            "#,
        )
        .bind(host_name)
        .bind(blueprint_name)
        .bind(template_values)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.try_get::<i64, _>("id").unwrap()))
    }

    pub async fn list_jobs_for_host(&self, host_id: i64) -> Result<Vec<JobRecord>> {
        let rows = sqlx::query(
            r#"
            with all_jobs as (
                select * from jobs where host_id = ?1
            )
            select aj.id as id, aj.scheduler_id as scheduler_id,aj.is_completed as is_completed,aj.created_at as created_at,aj.completed_at as completed_at,aj.terminal_state as terminal_state,aj.scheduler_state as scheduler_state,aj.local_path as local_path,aj.remote_path as remote_path,aj.stdout_path as stdout_path,aj.stderr_path as stderr_path,aj.blueprint_name as blueprint_name,aj.default_retrieve_path as default_retrieve_path,aj.template_values as template_values,h.name as name
            from all_jobs aj
            join hosts h
              on aj.host_id = h.id;
            "#,
        )
        .bind(host_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_job).collect())
    }
    pub async fn get_job_by_job_id(&self, id: i64) -> Result<Option<JobRecord>> {
        let row = sqlx::query(
            r#"
            select aj.id as id,
                   aj.scheduler_id as scheduler_id,
                   aj.is_completed as is_completed,
                   aj.created_at as created_at,
                   aj.completed_at as completed_at,
                   aj.terminal_state as terminal_state,
                   aj.scheduler_state as scheduler_state,
                   aj.local_path as local_path,
                   aj.remote_path as remote_path,
                   aj.stdout_path as stdout_path,
                   aj.stderr_path as stderr_path,
                   aj.blueprint_name as blueprint_name,
                   aj.default_retrieve_path as default_retrieve_path,
                   aj.template_values as template_values,
                   h.name as name
            from jobs aj
            join hosts h on aj.host_id = h.id
            where aj.id = ?1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_job))
    }

    pub async fn list_all_jobs(&self) -> Result<Vec<JobRecord>> {
        let rows = sqlx::query(
            r#"
            with all_jobs as (
                select * from jobs
            )
            select aj.id as id, aj.scheduler_id as scheduler_id,aj.is_completed as is_completed,aj.created_at as created_at,aj.completed_at as completed_at,aj.terminal_state as terminal_state,aj.scheduler_state as scheduler_state,aj.local_path as local_path,aj.remote_path as remote_path,aj.stdout_path as stdout_path,aj.stderr_path as stderr_path,aj.blueprint_name as blueprint_name,aj.default_retrieve_path as default_retrieve_path,aj.template_values as template_values,h.name as name
            from all_jobs aj
            join hosts h
              on aj.host_id = h.id;
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_job).collect())
    }

    pub async fn list_running_jobs(&self) -> Result<Vec<JobRecord>> {
        let rows = sqlx::query(
            r#"
            select aj.id as id, aj.scheduler_id as scheduler_id,aj.is_completed as is_completed,aj.created_at as created_at,aj.completed_at as completed_at,aj.terminal_state as terminal_state,aj.scheduler_state as scheduler_state,aj.local_path as local_path,aj.remote_path as remote_path,aj.stdout_path as stdout_path,aj.stderr_path as stderr_path,aj.blueprint_name as blueprint_name,aj.default_retrieve_path as default_retrieve_path,aj.template_values as template_values,h.name as name
            from jobs aj
            join hosts h
              on aj.host_id = h.id
            where aj.is_completed = 0;
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_job).collect())
    }

    pub async fn mark_job_completed(&self, id: i64, terminal_state: Option<&str>) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query(
            r#"
            update jobs
            set is_completed = 1,
                completed_at = ?1,
                terminal_state = ?2,
                scheduler_state = ?3
            where id = ?4
            "#,
        )
        .bind(now)
        .bind(terminal_state)
        .bind(terminal_state)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_job_by_job_id(&self, id: i64) -> Result<bool> {
        let result = sqlx::query(
            r#"
            delete from jobs
            where id = ?1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn update_job_scheduler_state(
        &self,
        id: i64,
        scheduler_state: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            update jobs
            set scheduler_state = ?1
            where id = ?2
            "#,
        )
        .bind(scheduler_state)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

// -- helpers

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
}

fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn row_to_host(row: sqlx::sqlite::SqliteRow) -> HostRecord {
    let hostname: Option<String> = row.try_get("hostname").ok().flatten();
    let ip_str: Option<String> = row.try_get("ip").ok().flatten();

    let address = if let Some(h) = hostname {
        Address::Hostname(h)
    } else if let Some(s) = ip_str {
        match s.parse::<IpAddr>() {
            Ok(ip) => Address::Ip(ip),
            Err(_) => Address::Hostname(s),
        }
    } else {
        Address::Hostname("<unknown>".into())
    };

    let accounting_available = row.try_get::<i64, _>("accounting_available").unwrap_or(0) != 0;
    let is_default = row.try_get::<i64, _>("is_default").unwrap_or(0) != 0;
    HostRecord {
        id: row.try_get("id").unwrap(),
        name: row.try_get("name").unwrap(),
        username: row.try_get("username").unwrap(),
        address,
        slurm: SlurmVersion {
            major: row.try_get("slurm_major").unwrap(),
            minor: row.try_get("slurm_minor").unwrap(),
            patch: row.try_get("slurm_patch").unwrap(),
        },
        distro: Distro {
            name: row.try_get("distro_name").unwrap(),
            version: row.try_get("distro_version").unwrap(),
        },
        kernel_version: row.try_get("kernel_version").unwrap(),
        created_at: row.try_get("created_at").unwrap(),
        updated_at: row.try_get("updated_at").unwrap(),
        port: row.try_get("port").unwrap(),
        identity_path: row.try_get("identity_path").unwrap(),
        accounting_available,
        default_base_path: row.try_get("default_base_path").unwrap(),
        default_scratch_directory: row.try_get("default_scratch_directory").ok().flatten(),
        is_default,
    }
}

fn row_to_partition(row: sqlx::sqlite::SqliteRow) -> PartitionRecord {
    let info_text: Option<String> = row.try_get("info").ok().flatten();
    let info = info_text.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok());

    PartitionRecord {
        id: row.try_get("id").unwrap(),
        host_id: row.try_get("host_id").unwrap(),
        name: row.try_get("name").unwrap(),
        info,
        created_at: row.try_get("created_at").unwrap(),
        updated_at: row.try_get("updated_at").unwrap(),
    }
}

fn serialize_string_list(values: &[String]) -> Result<Option<String>> {
    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::to_string(values)?))
    }
}

fn deserialize_string_list(raw: Option<String>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
        .unwrap_or_default()
}

fn row_to_blueprint(row: sqlx::sqlite::SqliteRow) -> BlueprintRecord {
    let raw_name: String = row.try_get("name").unwrap();
    let (name, version_tag) = match raw_name.split_once(':') {
        Some((base, tag)) => (base.to_string(), Some(tag.to_string())),
        None => (raw_name, None),
    };
    let sbatch_scripts = deserialize_string_list(
        row.try_get::<Option<String>, _>("sbatch_scripts")
            .ok()
            .flatten(),
    );
    let sync_include = deserialize_string_list(
        row.try_get::<Option<String>, _>("sync_include")
            .ok()
            .flatten(),
    );
    let sync_exclude = deserialize_string_list(
        row.try_get::<Option<String>, _>("sync_exclude")
            .ok()
            .flatten(),
    );
    BlueprintRecord {
        name,
        created_at: row.try_get("created_at").unwrap(),
        updated_at: row.try_get("updated_at").unwrap(),
        version_tag,
        tarball_hash: row.try_get("tarball_hash").ok().flatten(),
        tarball_hash_function: row.try_get("tarball_hash_function").ok().flatten(),
        tool_version: row.try_get("tool_version").ok().flatten(),
        template_config_json: row.try_get("template_config_json").ok().flatten(),
        submit_sbatch_script: row.try_get("submit_sbatch_script").ok().flatten(),
        sbatch_scripts,
        default_retrieve_path: row.try_get("default_retrieve_path").ok().flatten(),
        sync_include,
        sync_exclude,
    }
}

fn row_to_job(row: sqlx::sqlite::SqliteRow) -> JobRecord {
    JobRecord {
        id: row.try_get("id").unwrap(),
        scheduler_id: row.try_get("scheduler_id").unwrap(),
        name: row.try_get("name").unwrap(),
        created_at: row.try_get("created_at").unwrap(),
        finished_at: row.try_get("completed_at").unwrap(),
        is_completed: row.try_get("is_completed").unwrap(),
        terminal_state: row.try_get("terminal_state").unwrap(),
        scheduler_state: row.try_get("scheduler_state").unwrap(),
        local_path: row
            .try_get::<Option<String>, _>("local_path")
            .ok()
            .flatten()
            .unwrap_or_default(),
        remote_path: row.try_get("remote_path").unwrap(),
        stdout_path: row.try_get("stdout_path").unwrap(),
        stderr_path: row.try_get("stderr_path").ok().flatten(),
        blueprint_name: row.try_get("blueprint_name").ok().flatten(),
        default_retrieve_path: row.try_get("default_retrieve_path").ok().flatten(),
        template_values: row.try_get("template_values").ok().flatten(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::IpAddr;

    fn make_host(name: &str, username: &str, addr: Address) -> NewHost {
        NewHost {
            name: name.into(),
            username: username.into(),
            address: addr,
            slurm: SlurmVersion {
                major: 23,
                minor: 11,
                patch: 5,
            },
            distro: Distro {
                name: "Ubuntu".into(),
                version: "22.04".into(),
            },
            kernel_version: "6.5.0-41-generic".into(),
            port: 22,
            identity_path: Some("/home/jeff/.ssh/id_ed25519".to_string()),
            accounting_available: true,
            default_base_path: Some("/home/jeff/runs".to_string()),
            default_scratch_directory: Some("/scratch/jeff".to_string()),
            is_default: false,
        }
    }

    #[tokio::test]
    async fn round_trip_by_name() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("gpu01", "bob", Address::Hostname("node-a".into()));
        let id = db.insert_host(&host).await.unwrap();
        let got = db.get_by_name("gpu01").await.unwrap().unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.name, "gpu01");
        assert_eq!(
            got.default_scratch_directory.as_deref(),
            Some("/scratch/jeff")
        );
    }

    #[tokio::test]
    async fn upsert_prefers_name() {
        let db = HostStore::open_memory().await.unwrap();
        let ip: IpAddr = "10.0.0.42".parse().unwrap();

        let first = make_host("c1", "carol", Address::Ip(ip));
        let id1 = db.upsert_host(&first).await.unwrap();

        // Change fields and upsert with same name; should update same row.
        let mut second = first.clone();
        second.kernel_version = "6.1.0-20-amd64".into();
        let id2 = db.upsert_host(&second).await.unwrap();

        assert_eq!(id1, id2);
        let got = db.get_by_name("c1").await.unwrap().unwrap();
        assert_eq!(got.kernel_version, "6.1.0-20-amd64");
    }

    #[tokio::test]
    async fn upsert_default_unsets_previous_default() {
        let db = HostStore::open_memory().await.unwrap();

        let mut first = make_host("c1", "carol", Address::Hostname("node-1".into()));
        first.is_default = true;
        db.upsert_host(&first).await.unwrap();

        let mut second = make_host("c2", "carol", Address::Hostname("node-2".into()));
        second.is_default = true;
        db.upsert_host(&second).await.unwrap();

        let first_saved = db.get_by_name("c1").await.unwrap().unwrap();
        let second_saved = db.get_by_name("c2").await.unwrap().unwrap();
        assert!(!first_saved.is_default);
        assert!(second_saved.is_default);
    }

    #[tokio::test]
    async fn upsert_non_default_keeps_existing_default() {
        let db = HostStore::open_memory().await.unwrap();

        let mut first = make_host("c1", "carol", Address::Hostname("node-1".into()));
        first.is_default = true;
        db.upsert_host(&first).await.unwrap();

        let second = make_host("c2", "carol", Address::Hostname("node-2".into()));
        db.upsert_host(&second).await.unwrap();

        let first_saved = db.get_by_name("c1").await.unwrap().unwrap();
        let second_saved = db.get_by_name("c2").await.unwrap().unwrap();
        assert!(first_saved.is_default);
        assert!(!second_saved.is_default);
    }

    // Edge cases start here.

    #[tokio::test]
    async fn empty_name_rejected_on_insert() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("", "alice", Address::Hostname("h1".into()));
        let err = db.insert_host(&host).await.unwrap_err();
        matches!(err, HostStoreError::EmptyName);
    }

    #[tokio::test]
    async fn duplicate_name_rejected_on_insert() {
        let db = HostStore::open_memory().await.unwrap();
        let h1 = make_host("dup1", "u1", Address::Hostname("h1".into()));
        let h2 = make_host("dup1", "u2", Address::Hostname("h2".into()));

        db.insert_host(&h1).await.unwrap();
        let err = db.insert_host(&h2).await.unwrap_err();

        // Should surface a UNIQUE constraint error from SQLite via sqlx
        match err {
            HostStoreError::Sqlx(e) => {
                assert!(e.to_string().to_lowercase().contains("unique"));
            }
            other => panic!("expected sqlx unique-constraint error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn upsert_by_user_addr_replaces_name_if_new() {
        let db = HostStore::open_memory().await.unwrap();

        // Initially store with name "old"
        let h_old = make_host("old", "u", Address::Hostname("same-node".into()));
        let id = db.insert_host(&h_old).await.unwrap();

        // Upsert same (username, address) but with a new name "new"
        let h_new = make_host("new", "u", Address::Hostname("same-node".into()));
        let id2 = db.upsert_host(&h_new).await.unwrap();
        assert_eq!(id, id2);

        // Old id should disappear; new name should work.
        assert!(db.get_by_name("old").await.unwrap().is_none());
        let got = db.get_by_name("new").await.unwrap().unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.name, "new");
    }

    #[tokio::test]
    async fn get_by_name_not_found_returns_none() {
        let db = HostStore::open_memory().await.unwrap();
        assert!(db.get_by_name("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_by_name_returns_0_when_missing_and_1_when_deleted() {
        let db = HostStore::open_memory().await.unwrap();
        assert_eq!(db.delete_by_name("nope").await.unwrap(), 0);

        let h = make_host("d1", "u", Address::Hostname("h".into()));
        db.insert_host(&h).await.unwrap();
        assert_eq!(db.delete_by_name("d1").await.unwrap(), 1);
        assert!(db.get_by_name("d1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn update_name_conflict_is_rejected() {
        let db = HostStore::open_memory().await.unwrap();

        let a = make_host("a", "u", Address::Hostname("h1".into()));
        let b = make_host("b", "u", Address::Hostname("h2".into()));

        let id_a = db.insert_host(&a).await.unwrap();
        let id_b = db.insert_host(&b).await.unwrap();

        // Try to change B's name to "a" (already taken)
        let mut b2 = b.clone();
        b2.name = "a".into();
        let err = db.update_host(id_b, &b2).await.unwrap_err();

        match err {
            HostStoreError::Sqlx(e) => {
                assert!(e.to_string().to_lowercase().contains("unique"));
            }
            other => panic!("expected unique constraint failure, got {other:?}"),
        }

        // Ensure A unaffected.
        let a_fresh = db.get_by_name("a").await.unwrap().unwrap();
        assert_eq!(a_fresh.id, id_a);
    }

    #[tokio::test]
    async fn ip_roundtrip_and_lookup() {
        let db = HostStore::open_memory().await.unwrap();
        let ip: IpAddr = "2001:db8::1".parse().unwrap();
        let h = make_host("v6node", "alice", Address::Ip(ip));
        db.insert_host(&h).await.unwrap();

        // Lookup by name
        let got = db.get_by_name("v6node").await.unwrap().unwrap();
        match got.address {
            Address::Ip(parsed) => assert_eq!(parsed, ip),
            _ => panic!("expected IP address"),
        }

        // Lookup by (username, ip, port)
        let got2 = db
            .get_by_user_and_address("alice", &Address::Ip(ip), 22)
            .await
            .unwrap();
        assert!(got2.is_some());
    }

    #[tokio::test]
    async fn insert_and_retrieve_partitions_for_name() {
        let name = "gpurig";
        let db = HostStore::open_memory().await.unwrap();
        let ip: IpAddr = "2001:db8::1".parse().unwrap();
        let host = NewHost {
            name: name.into(),
            username: "alice".into(),
            address: Address::Ip(ip),
            port: 22,
            slurm: SlurmVersion {
                major: 23,
                minor: 11,
                patch: 5,
            },
            distro: Distro {
                name: "ubuntu".into(),
                version: "22.04".into(),
            },
            kernel_version: "6.5.0-41-generic".into(),
            identity_path: Some("/home/alice/.ssh/id_ed25519".to_string()),
            accounting_available: true,
            default_base_path: Some("/home/alice/runs".to_string()),
            default_scratch_directory: Some("/scratch/alice".to_string()),
            is_default: false,
        };
        db.insert_host(&host).await.unwrap();
        let mut info_map: HashMap<String, serde_json::Value> = HashMap::new();
        info_map.insert("MaxTime".into(), serde_json::json!("24:00:00"));
        info_map.insert("MaxCPUsPerNode".into(), serde_json::json!("UNLIMITED"));
        info_map.insert("QoS".into(), serde_json::json!(["normal", "high"]));
        info_map.insert("State".into(), serde_json::json!("UP"));
        info_map.insert("PriorityTier".into(), serde_json::json!(1));
        info_map.insert(
            "TRES".into(),
            serde_json::json!("cpu=64,mem=990000M,node=2,billing=64,gres/gpu=8"),
        );
        let spec = NewPartition {
            name: "gpu".into(),
            info: Some(serde_json::to_value(&info_map).unwrap()),
        };
        let _partition_id = db.upsert_partition_by_name(name, &spec).await.unwrap();
        let part = db
            .get_partition_by_name_and_partition_name(name, "gpu")
            .await
            .unwrap()
            .expect("partition should exist");
        let obj = part
            .info
            .expect("info should be present")
            .as_object()
            .cloned()
            .expect("info should be a JSON object");
        assert_eq!(obj.get("MaxTime"), Some(&serde_json::json!("24:00:00")));
        assert_eq!(
            obj.get("TRES"),
            Some(&serde_json::json!(
                "cpu=64,mem=990000M,node=2,billing=64,gres/gpu=8"
            ))
        );
        assert_eq!(obj.get("PriorityTier").and_then(|v| v.as_i64()), Some(1));

        assert_eq!(obj.get("State"), Some(&serde_json::json!("UP")));
        let qos = obj
            .get("QoS")
            .and_then(|v| v.as_array())
            .expect("qos array");
        assert_eq!(
            qos,
            &vec![serde_json::json!("normal"), serde_json::json!("high")]
        );
    }

    #[tokio::test]
    async fn list_jobs_for_host_filters_and_maps_fields() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job = NewJob {
            scheduler_id: Some(42),
            host_id: host_row.id,
            local_path: Some("/tmp/local".into()),
            remote_path: "/remote/run".into(),
            stdout_path: "/remote/run/slurm-42.out".into(),
            stderr_path: Some("/remote/run/slurm-42.out".into()),
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        db.insert_job(&job).await.unwrap();

        let jobs = db.list_jobs_for_host(host_row.id).await.unwrap();
        assert_eq!(jobs.len(), 1);
        let got = &jobs[0];
        assert_eq!(got.scheduler_id, Some(42));
        assert_eq!(got.name, "host-a");
        assert_eq!(got.local_path, "/tmp/local");
        assert_eq!(got.remote_path, "/remote/run");
        assert!(!got.is_completed);
        assert!(got.finished_at.is_none());
        assert!(got.terminal_state.is_none());
        assert!(got.scheduler_state.is_none());
        assert!(!got.created_at.is_empty());
    }

    #[tokio::test]
    async fn job_round_trips_template_values() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job = NewJob {
            scheduler_id: Some(77),
            host_id: host_row.id,
            local_path: Some("/tmp/local".into()),
            remote_path: "/remote/run".into(),
            stdout_path: "/remote/run/slurm-77.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: Some(r#"{"alpha": "beta"}"#.to_string()),
        };
        let job_id = db.insert_job(&job).await.unwrap();

        let fetched = db.get_job_by_job_id(job_id).await.unwrap().unwrap();
        assert_eq!(
            fetched.template_values.as_deref(),
            Some(r#"{"alpha": "beta"}"#)
        );
    }

    #[tokio::test]
    async fn latest_remote_path_for_local_path_returns_latest() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job1 = NewJob {
            scheduler_id: Some(40),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run1".into(),
            stdout_path: "/remote/run1/slurm-40.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job2 = NewJob {
            scheduler_id: Some(41),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run2".into(),
            stdout_path: "/remote/run2/slurm-41.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        db.insert_job(&job1).await.unwrap();
        db.insert_job(&job2).await.unwrap();

        let latest = db
            .latest_remote_path_for_local_path("host-a", "/tmp/project", None)
            .await
            .unwrap();
        assert_eq!(latest.as_deref(), Some("/remote/run2"));

        let missing = db
            .latest_remote_path_for_local_path("host-a", "/tmp/missing", None)
            .await
            .unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn latest_remote_path_for_local_path_matches_template_values() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job1 = NewJob {
            scheduler_id: Some(50),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run1".into(),
            stdout_path: "/remote/run1/slurm-50.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job2 = NewJob {
            scheduler_id: Some(51),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run2".into(),
            stdout_path: "/remote/run2/slurm-51.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: Some(r#"{"mode":"a"}"#.to_string()),
        };
        let job3 = NewJob {
            scheduler_id: Some(52),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run3".into(),
            stdout_path: "/remote/run3/slurm-52.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: Some(r#"{"mode":"b"}"#.to_string()),
        };
        let job4 = NewJob {
            scheduler_id: Some(53),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run4".into(),
            stdout_path: "/remote/run4/slurm-53.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };

        db.insert_job(&job1).await.unwrap();
        db.insert_job(&job2).await.unwrap();
        db.insert_job(&job3).await.unwrap();
        db.insert_job(&job4).await.unwrap();

        let latest_none = db
            .latest_remote_path_for_local_path("host-a", "/tmp/project", None)
            .await
            .unwrap();
        assert_eq!(latest_none.as_deref(), Some("/remote/run4"));

        let latest_a = db
            .latest_remote_path_for_local_path("host-a", "/tmp/project", Some(r#"{"mode":"a"}"#))
            .await
            .unwrap();
        assert_eq!(latest_a.as_deref(), Some("/remote/run2"));

        let latest_b = db
            .latest_remote_path_for_local_path("host-a", "/tmp/project", Some(r#"{"mode":"b"}"#))
            .await
            .unwrap();
        assert_eq!(latest_b.as_deref(), Some("/remote/run3"));

        let missing = db
            .latest_remote_path_for_local_path("host-a", "/tmp/project", Some(r#"{"mode":"c"}"#))
            .await
            .unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn latest_remote_path_for_blueprint_matches_template_values() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job1 = NewJob {
            scheduler_id: Some(60),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run1".into(),
            stdout_path: "/remote/run1/slurm-60.out".into(),
            stderr_path: None,
            blueprint_name: Some("demo".to_string()),
            default_retrieve_path: None,
            template_values: None,
        };
        let job2 = NewJob {
            scheduler_id: Some(61),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run2".into(),
            stdout_path: "/remote/run2/slurm-61.out".into(),
            stderr_path: None,
            blueprint_name: Some("demo".to_string()),
            default_retrieve_path: None,
            template_values: Some(r#"{"mode":"a"}"#.to_string()),
        };
        let job3 = NewJob {
            scheduler_id: Some(62),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run3".into(),
            stdout_path: "/remote/run3/slurm-62.out".into(),
            stderr_path: None,
            blueprint_name: Some("other".to_string()),
            default_retrieve_path: None,
            template_values: None,
        };
        let job4 = NewJob {
            scheduler_id: Some(63),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run4".into(),
            stdout_path: "/remote/run4/slurm-63.out".into(),
            stderr_path: None,
            blueprint_name: Some("demo".to_string()),
            default_retrieve_path: None,
            template_values: None,
        };

        db.insert_job(&job1).await.unwrap();
        db.insert_job(&job2).await.unwrap();
        db.insert_job(&job3).await.unwrap();
        db.insert_job(&job4).await.unwrap();

        let latest_none = db
            .latest_remote_path_for_blueprint("host-a", "demo", None)
            .await
            .unwrap();
        assert_eq!(latest_none.as_deref(), Some("/remote/run4"));

        let latest_a = db
            .latest_remote_path_for_blueprint("host-a", "demo", Some(r#"{"mode":"a"}"#))
            .await
            .unwrap();
        assert_eq!(latest_a.as_deref(), Some("/remote/run2"));

        let missing = db
            .latest_remote_path_for_blueprint("host-a", "demo", Some(r#"{"mode":"b"}"#))
            .await
            .unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn running_job_id_for_remote_path_filters_completed() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job = NewJob {
            scheduler_id: Some(200),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run".into(),
            stdout_path: "/remote/run/slurm-200.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job_id = db.insert_job(&job).await.unwrap();

        let running = db
            .running_job_id_for_remote_path("host-a", "/remote/run")
            .await
            .unwrap();
        assert_eq!(running, Some(job_id));

        db.mark_job_completed(job_id, Some("COMPLETED"))
            .await
            .unwrap();

        let running = db
            .running_job_id_for_remote_path("host-a", "/remote/run")
            .await
            .unwrap();
        assert!(running.is_none());
    }

    #[tokio::test]
    async fn running_job_id_for_local_path_matches_template_values() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job_none = NewJob {
            scheduler_id: Some(210),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run-none".into(),
            stdout_path: "/remote/run-none/slurm-210.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job_a = NewJob {
            scheduler_id: Some(211),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/run-a".into(),
            stdout_path: "/remote/run-a/slurm-211.out".into(),
            stderr_path: None,
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: Some(r#"{"mode":"a"}"#.to_string()),
        };
        let none_id = db.insert_job(&job_none).await.unwrap();
        let a_id = db.insert_job(&job_a).await.unwrap();

        let running_none = db
            .running_job_id_for_local_path("host-a", "/tmp/project", None)
            .await
            .unwrap();
        assert_eq!(running_none, Some(none_id));

        let running_a = db
            .running_job_id_for_local_path("host-a", "/tmp/project", Some(r#"{"mode":"a"}"#))
            .await
            .unwrap();
        assert_eq!(running_a, Some(a_id));

        db.mark_job_completed(a_id, Some("COMPLETED"))
            .await
            .unwrap();
        let running_a = db
            .running_job_id_for_local_path("host-a", "/tmp/project", Some(r#"{"mode":"a"}"#))
            .await
            .unwrap();
        assert!(running_a.is_none());
    }

    #[tokio::test]
    async fn running_job_id_for_blueprint_matches_template_values() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job_none = NewJob {
            scheduler_id: Some(220),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/bp-none".into(),
            stdout_path: "/remote/bp-none/slurm-220.out".into(),
            stderr_path: None,
            blueprint_name: Some("demo".to_string()),
            default_retrieve_path: None,
            template_values: None,
        };
        let job_a = NewJob {
            scheduler_id: Some(221),
            host_id: host_row.id,
            local_path: Some("/tmp/project".into()),
            remote_path: "/remote/bp-a".into(),
            stdout_path: "/remote/bp-a/slurm-221.out".into(),
            stderr_path: None,
            blueprint_name: Some("demo".to_string()),
            default_retrieve_path: None,
            template_values: Some(r#"{"mode":"a"}"#.to_string()),
        };
        let none_id = db.insert_job(&job_none).await.unwrap();
        let a_id = db.insert_job(&job_a).await.unwrap();

        let running_none = db
            .running_job_id_for_blueprint("host-a", "demo", None)
            .await
            .unwrap();
        assert_eq!(running_none, Some(none_id));

        let running_a = db
            .running_job_id_for_blueprint("host-a", "demo", Some(r#"{"mode":"a"}"#))
            .await
            .unwrap();
        assert_eq!(running_a, Some(a_id));

        db.mark_job_completed(a_id, Some("COMPLETED"))
            .await
            .unwrap();
        let running_a = db
            .running_job_id_for_blueprint("host-a", "demo", Some(r#"{"mode":"a"}"#))
            .await
            .unwrap();
        assert!(running_a.is_none());
    }

    #[tokio::test]
    async fn get_job_by_job_id_returns_row() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job = NewJob {
            scheduler_id: Some(42),
            host_id: host_row.id,
            local_path: Some("/tmp/local-a".into()),
            remote_path: "/remote/run-a".into(),
            stdout_path: "/remote/run-a/slurm-42.out".into(),
            stderr_path: Some("/remote/run-a/slurm-42.out".into()),
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job_id = db.insert_job(&job).await.unwrap();

        let got = db.get_job_by_job_id(job_id).await.unwrap().unwrap();
        assert_eq!(got.id, job_id);
        assert_eq!(got.scheduler_id, Some(42));
        assert_eq!(got.name, "host-a");
    }

    #[tokio::test]
    async fn update_job_scheduler_state_persists_value() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job = NewJob {
            scheduler_id: Some(42),
            host_id: host_row.id,
            local_path: Some("/tmp/local-a".into()),
            remote_path: "/remote/run-a".into(),
            stdout_path: "/remote/run-a/slurm-42.out".into(),
            stderr_path: Some("/remote/run-a/slurm-42.out".into()),
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job_id = db.insert_job(&job).await.unwrap();

        db.update_job_scheduler_state(job_id, Some("PENDING"))
            .await
            .unwrap();

        let got = db.get_job_by_job_id(job_id).await.unwrap().unwrap();
        assert_eq!(got.scheduler_state.as_deref(), Some("PENDING"));
    }

    #[tokio::test]
    async fn list_running_jobs_skips_completed_jobs() {
        let db = HostStore::open_memory().await.unwrap();
        let host = make_host("host-a", "alice", Address::Hostname("node-a".into()));
        db.insert_host(&host).await.unwrap();

        let host_row = db.get_by_name("host-a").await.unwrap().unwrap();
        let job1 = NewJob {
            scheduler_id: Some(101),
            host_id: host_row.id,
            local_path: Some("/tmp/local1".into()),
            remote_path: "/remote/run1".into(),
            stdout_path: "/remote/run1/slurm-101.out".into(),
            stderr_path: Some("/remote/run1/slurm-101.out".into()),
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job2 = NewJob {
            scheduler_id: Some(102),
            host_id: host_row.id,
            local_path: Some("/tmp/local2".into()),
            remote_path: "/remote/run2".into(),
            stdout_path: "/remote/run2/slurm-102.out".into(),
            stderr_path: Some("/remote/run2/slurm-102.out".into()),
            blueprint_name: None,
            default_retrieve_path: None,
            template_values: None,
        };
        let job1_id = db.insert_job(&job1).await.unwrap();
        let job2_id = db.insert_job(&job2).await.unwrap();

        db.mark_job_completed(job1_id, Some("FAILED"))
            .await
            .unwrap();

        let running = db.list_running_jobs().await.unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, job2_id);

        let all = db.list_all_jobs().await.unwrap();
        let completed = all.iter().find(|j| j.id == job1_id).unwrap();
        assert!(completed.is_completed);
        assert!(completed.finished_at.is_some());
        assert_eq!(completed.terminal_state.as_deref(), Some("FAILED"));
        assert_eq!(completed.scheduler_state.as_deref(), Some("FAILED"));
    }

    #[tokio::test]
    async fn delete_blueprint_by_name_removes_project_record() {
        let db = HostStore::open_memory().await.unwrap();

        let created = db
            .upsert_blueprint("proj-a")
            .await
            .expect("project created");
        assert_eq!(created.name, "proj-a");

        let deleted = db
            .delete_blueprint_by_name("proj-a")
            .await
            .expect("delete should succeed");
        assert_eq!(deleted, 1);

        let project = db
            .get_blueprint_by_name("proj-a")
            .await
            .expect("query should succeed");
        assert!(project.is_none());
    }

    #[tokio::test]
    async fn delete_blueprints_by_base_name_removes_all_tags() {
        let db = HostStore::open_memory().await.unwrap();

        db.upsert_blueprint("proj-a")
            .await
            .expect("project created");

        let build_a = NewBlueprintBuild {
            name: "proj-a:20250101.001".to_string(),
            tarball_hash: "hash-a".to_string(),
            tarball_hash_function: "blake3".to_string(),
            tool_version: "1.0.0".to_string(),
            template_config_json: None,
            submit_sbatch_script: None,
            sbatch_scripts: Vec::new(),
            default_retrieve_path: None,
            sync_include: Vec::new(),
            sync_exclude: Vec::new(),
        };
        db.upsert_blueprint_build(&build_a).await.expect("build a");

        let build_b = NewBlueprintBuild {
            name: "proj-a:20250101.002".to_string(),
            tarball_hash: "hash-b".to_string(),
            tarball_hash_function: "blake3".to_string(),
            tool_version: "1.0.0".to_string(),
            template_config_json: None,
            submit_sbatch_script: None,
            sbatch_scripts: Vec::new(),
            default_retrieve_path: None,
            sync_include: Vec::new(),
            sync_exclude: Vec::new(),
        };
        db.upsert_blueprint_build(&build_b).await.expect("build b");

        let deleted = db
            .delete_blueprints_by_base_name("proj-a")
            .await
            .expect("delete all");
        assert_eq!(deleted, 3);

        let remaining = db.list_blueprints().await.expect("list blueprints");
        assert!(remaining.is_empty());
    }
}
