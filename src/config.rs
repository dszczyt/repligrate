use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// PostgreSQL connection URL
    pub database_url: String,

    /// Output directory for generated migrations
    pub output_dir: PathBuf,

    /// Replication settings
    pub replication: ReplicationConfig,

    /// Schema filtering
    pub schema_filter: SchemaFilterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Slot name for logical replication
    pub slot_name: String,

    /// Publication name
    pub publication_name: String,

    /// Batch size for processing WAL messages
    pub batch_size: usize,

    /// Timeout for receiving messages (in seconds)
    pub receive_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFilterConfig {
    /// Schemas to include (empty = all)
    pub include_schemas: Vec<String>,

    /// Schemas to exclude
    pub exclude_schemas: Vec<String>,

    /// Tables to include (empty = all)
    pub include_tables: Vec<String>,

    /// Tables to exclude
    pub exclude_tables: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            output_dir: PathBuf::from("./migrations"),
            replication: ReplicationConfig::default(),
            schema_filter: SchemaFilterConfig::default(),
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            slot_name: "repligrate_slot".to_string(),
            publication_name: "repligrate_pub".to_string(),
            batch_size: 1000,
            receive_timeout: 30,
        }
    }
}

impl Default for SchemaFilterConfig {
    fn default() -> Self {
        Self {
            include_schemas: vec![],
            exclude_schemas: vec!["pg_catalog".to_string(), "information_schema".to_string()],
            include_tables: vec![],
            exclude_tables: vec![],
        }
    }
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow!("DATABASE_URL environment variable not set"))?;

        let output_dir = std::env::var("OUTPUT_DIR")
            .unwrap_or_else(|_| "./migrations".to_string());

        Ok(Self {
            database_url,
            output_dir: PathBuf::from(output_dir),
            ..Default::default()
        })
    }

    /// Load configuration from a TOML file
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.database_url.is_empty() {
            return Err(anyhow!("database_url is required"));
        }

        if !self.output_dir.exists() {
            std::fs::create_dir_all(&self.output_dir)?;
        }

        Ok(())
    }
}

