use crate::config::Config;
use crate::db::DbConnection;
use crate::migration::{ MigrationGenerator, MigrationWriter };
use crate::schema::{ SchemaChange, SchemaChangeParser };
use anyhow::Result;
use tracing::{ debug, info, warn };

/// Listens to PostgreSQL logical replication changes
pub struct ReplicationListener {
    config: Config,
    slot_name: String,
    publication_name: String,
    db: DbConnection,
}

impl ReplicationListener {
    /// Create a new replication listener
    pub async fn new(config: Config, slot_name: String, publication_name: String) -> Result<Self> {
        config.validate()?;
        let db = DbConnection::new(&config.database_url)?;

        Ok(Self {
            config,
            slot_name,
            publication_name,
            db,
        })
    }

    /// Start listening for replication changes
    pub async fn listen(&mut self, tables: Option<String>) -> Result<()> {
        info!("Setting up replication...");

        // Parse table list if provided
        let table_list = tables.map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>()
        });

        // Create replication slot and publication
        self.db.create_replication_slot(&self.slot_name)?;
        self.db.create_publication(&self.publication_name, table_list)?;

        info!("Replication setup complete. Listening for changes...");

        // Collect schema changes
        let mut schema_changes: Vec<SchemaChange> = Vec::new();
        let _change_count = 0;

        // In a real implementation, we would connect to the replication slot
        // and process messages. For now, we'll demonstrate the structure.
        info!("Waiting for schema changes (press Ctrl+C to stop)...");

        // Simulate receiving changes
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            // In production, this would receive actual replication messages
            // For now, we just wait for interrupt
        }
    }

    /// Process a replication message
    fn process_message(&self, message: &str) -> Result<Option<SchemaChange>> {
        debug!("Processing replication message: {}", message);

        // Parse the message to extract SQL
        if let Some((change_type, object_name)) = SchemaChangeParser::parse(message) {
            let schema_change = SchemaChange::new(
                change_type,
                "public".to_string(),
                object_name,
                message.to_string()
            );

            info!("Detected schema change: {:?}", schema_change.change_type);
            return Ok(Some(schema_change));
        }

        Ok(None)
    }

    /// Generate migration from collected changes
    pub fn generate_migration(&self, changes: Vec<SchemaChange>) -> Result<()> {
        if changes.is_empty() {
            warn!("No schema changes detected");
            return Ok(());
        }

        let migration_name = format!("migration_{}", chrono::Local::now().format("%Y%m%d_%H%M%S"));
        let migration = MigrationGenerator::generate(migration_name, changes)?;

        MigrationWriter::write(&migration, &self.config.output_dir)?;

        Ok(())
    }

    /// Cleanup replication resources
    pub fn cleanup(&mut self) -> Result<()> {
        info!("Cleaning up replication resources...");
        self.db.drop_replication_slot(&self.slot_name)?;
        self.db.drop_publication(&self.publication_name)?;
        info!("Cleanup complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_change_parser() {
        let sql = "CREATE TABLE users (id INT PRIMARY KEY)";
        let result = SchemaChangeParser::parse(sql);
        assert!(result.is_some());
    }
}
