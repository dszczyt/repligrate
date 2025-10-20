use crate::schema::{ChangeType, SchemaChange};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use tracing::{debug, info};

/// Represents a pgroll migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgrollMigration {
    pub name: String,
    pub operations: Vec<Value>,
}

impl PgrollMigration {
    pub fn new(name: String) -> Self {
        Self {
            name,
            operations: Vec::new(),
        }
    }

    pub fn add_operation(&mut self, operation: Value) {
        self.operations.push(operation);
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(&self)?)
    }
}

/// Converter from schema changes to pgroll migrations
pub struct MigrationGenerator;

impl MigrationGenerator {
    /// Generate a pgroll migration from schema changes
    pub fn generate(
        migration_name: String,
        changes: Vec<SchemaChange>,
    ) -> Result<PgrollMigration> {
        let mut migration = PgrollMigration::new(migration_name);

        for change in changes {
            let operation = Self::schema_change_to_operation(&change)?;
            migration.add_operation(operation);
        }

        Ok(migration)
    }

    /// Convert a single schema change to a pgroll operation
    fn schema_change_to_operation(change: &SchemaChange) -> Result<Value> {
        match change.change_type {
            ChangeType::CreateTable => {
                Self::create_table_operation(change)
            }
            ChangeType::DropTable => {
                Self::drop_table_operation(change)
            }
            ChangeType::AddColumn => {
                Self::add_column_operation(change)
            }
            ChangeType::DropColumn => {
                Self::drop_column_operation(change)
            }
            ChangeType::AlterTable => {
                Self::alter_table_operation(change)
            }
            ChangeType::CreateIndex => {
                Self::create_index_operation(change)
            }
            ChangeType::DropIndex => {
                Self::drop_index_operation(change)
            }
            _ => {
                debug!("Unsupported change type: {:?}", change.change_type);
                Ok(json!({}))
            }
        }
    }

    fn create_table_operation(change: &SchemaChange) -> Result<Value> {
        Ok(json!({
            "create_table": {
                "name": change.object_name,
                "columns": []
            }
        }))
    }

    fn drop_table_operation(change: &SchemaChange) -> Result<Value> {
        Ok(json!({
            "drop_table": {
                "name": change.object_name
            }
        }))
    }

    fn add_column_operation(change: &SchemaChange) -> Result<Value> {
        Ok(json!({
            "add_column": {
                "table": change.object_name,
                "column": {
                    "name": "new_column",
                    "type": "text"
                }
            }
        }))
    }

    fn drop_column_operation(change: &SchemaChange) -> Result<Value> {
        Ok(json!({
            "drop_column": {
                "table": change.object_name,
                "column": "column_name"
            }
        }))
    }

    fn alter_table_operation(change: &SchemaChange) -> Result<Value> {
        Ok(json!({
            "raw_sql": {
                "up": change.details.sql.clone(),
                "down": format!("-- Rollback for: {}", change.details.sql)
            }
        }))
    }

    fn create_index_operation(change: &SchemaChange) -> Result<Value> {
        Ok(json!({
            "raw_sql": {
                "up": change.details.sql.clone(),
                "down": format!("DROP INDEX IF EXISTS {}", change.object_name)
            }
        }))
    }

    fn drop_index_operation(change: &SchemaChange) -> Result<Value> {
        Ok(json!({
            "raw_sql": {
                "up": change.details.sql.clone(),
                "down": format!("-- Recreate index: {}", change.object_name)
            }
        }))
    }
}

/// Migration file manager
pub struct MigrationWriter;

impl MigrationWriter {
    /// Write migration to file
    pub fn write(migration: &PgrollMigration, output_dir: &PathBuf) -> Result<PathBuf> {
        // Create output directory if it doesn't exist
        std::fs::create_dir_all(output_dir)?;

        // Generate filename with timestamp
        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
        let filename = format!("{}_{}.json", timestamp, migration.name);
        let filepath = output_dir.join(&filename);

        // Write migration to file
        let json_content = migration.to_json()?;
        std::fs::write(&filepath, json_content)?;

        info!("Migration written to: {}", filepath.display());
        Ok(filepath)
    }

    /// List all migrations in directory
    pub fn list_migrations(output_dir: &PathBuf) -> Result<Vec<PathBuf>> {
        let mut migrations = Vec::new();

        if !output_dir.exists() {
            return Ok(migrations);
        }

        for entry in std::fs::read_dir(output_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "json") {
                migrations.push(path);
            }
        }

        migrations.sort();
        Ok(migrations)
    }
}

