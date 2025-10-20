use crate::schema::{ ChangeType, SchemaChange };
use serde_json::{ json, Value };
use std::collections::HashMap;
use tracing::{ debug, info };

/// Optimizes migrations by merging related schema changes
pub struct MigrationOptimizer;

impl MigrationOptimizer {
    /// Optimize a list of schema changes by merging related operations
    /// while preserving the correct migration semantics
    pub fn optimize(changes: Vec<SchemaChange>) -> Vec<SchemaChange> {
        if changes.is_empty() {
            return changes;
        }

        info!("Optimizing {} schema changes", changes.len());

        let mut optimized = Vec::new();
        let mut pending_changes: HashMap<String, Vec<SchemaChange>> = HashMap::new();

        // Group changes by table
        for change in changes {
            let key = format!("{}.{}", change.schema_name, change.object_name);
            pending_changes.entry(key).or_insert_with(Vec::new).push(change);
        }

        // Process each group of changes
        for (_, group) in pending_changes {
            let merged = Self::merge_table_changes(group);
            optimized.extend(merged);
        }

        info!("Optimized to {} operations", optimized.len());
        optimized
    }

    /// Merge related changes for a single table
    fn merge_table_changes(changes: Vec<SchemaChange>) -> Vec<SchemaChange> {
        if changes.is_empty() {
            return changes;
        }

        let mut result = Vec::new();
        let mut i = 0;

        while i < changes.len() {
            let current = &changes[i];

            // Try to merge consecutive ADD COLUMN operations
            if current.change_type == ChangeType::AddColumn && i + 1 < changes.len() {
                let mut merged_columns = vec![current.clone()];
                let mut j = i + 1;

                // Collect consecutive ADD COLUMN operations
                while j < changes.len() && changes[j].change_type == ChangeType::AddColumn {
                    merged_columns.push(changes[j].clone());
                    j += 1;
                }

                // If we found multiple consecutive ADD COLUMN operations, merge them
                if merged_columns.len() > 1 {
                    let merged = Self::merge_add_columns(merged_columns);
                    result.push(merged);
                    i = j;
                    continue;
                }
            }

            // Try to merge ALTER TABLE operations
            if current.change_type == ChangeType::AlterTable && i + 1 < changes.len() {
                let mut merged_alters = vec![current.clone()];
                let mut j = i + 1;

                // Collect consecutive ALTER TABLE operations on the same table
                while
                    j < changes.len() &&
                    changes[j].change_type == ChangeType::AlterTable &&
                    changes[j].object_name == current.object_name
                {
                    merged_alters.push(changes[j].clone());
                    j += 1;
                }

                // If we found multiple consecutive ALTER TABLE operations, merge them
                if merged_alters.len() > 1 {
                    let merged = Self::merge_alter_tables(merged_alters);
                    result.push(merged);
                    i = j;
                    continue;
                }
            }

            // Try to merge ADD CONSTRAINT operations
            if current.change_type == ChangeType::AddConstraint && i + 1 < changes.len() {
                let mut merged_constraints = vec![current.clone()];
                let mut j = i + 1;

                // Collect consecutive ADD CONSTRAINT operations on the same table
                while
                    j < changes.len() &&
                    changes[j].change_type == ChangeType::AddConstraint &&
                    changes[j].object_name == current.object_name
                {
                    merged_constraints.push(changes[j].clone());
                    j += 1;
                }

                // If we found multiple consecutive ADD CONSTRAINT operations, merge them
                if merged_constraints.len() > 1 {
                    let merged = Self::merge_add_constraints(merged_constraints);
                    result.push(merged);
                    i = j;
                    continue;
                }
            }

            // No merge possible, add as-is
            result.push(current.clone());
            i += 1;
        }

        result
    }

    /// Merge multiple ADD COLUMN operations into a single operation
    fn merge_add_columns(changes: Vec<SchemaChange>) -> SchemaChange {
        debug!("Merging {} ADD COLUMN operations", changes.len());

        let first = &changes[0];
        let mut merged_sql = String::new();

        // Build a combined ALTER TABLE statement
        merged_sql.push_str(&format!("ALTER TABLE {} ", first.object_name));

        let column_sqls: Vec<String> = changes
            .iter()
            .filter_map(|change| {
                // Extract the ADD COLUMN part from each SQL statement
                let sql = &change.details.sql;
                if let Some(add_col_idx) = sql.to_uppercase().find("ADD COLUMN") {
                    Some(sql[add_col_idx..].to_string())
                } else {
                    None
                }
            })
            .collect();

        merged_sql.push_str(&column_sqls.join(", "));
        merged_sql.push(';');

        let mut merged = first.clone();
        merged.details.sql = merged_sql;

        merged
    }

    /// Merge multiple ALTER TABLE operations into a single operation
    fn merge_alter_tables(changes: Vec<SchemaChange>) -> SchemaChange {
        debug!("Merging {} ALTER TABLE operations", changes.len());

        let first = &changes[0];
        let mut merged_sql = String::new();

        // Build a combined ALTER TABLE statement
        merged_sql.push_str(&format!("ALTER TABLE {} ", first.object_name));

        let alter_clauses: Vec<String> = changes
            .iter()
            .filter_map(|change| {
                let sql = &change.details.sql;
                // Extract the ALTER clause (everything after ALTER TABLE tablename)
                if let Some(alter_idx) = sql.to_uppercase().find("ALTER TABLE") {
                    let after_table = &sql[alter_idx + 11..]; // Skip "ALTER TABLE"
                    // Skip the table name
                    if let Some(space_idx) = after_table.find(|c: char| c.is_whitespace()) {
                        let clause = after_table[space_idx..].trim();
                        if !clause.is_empty() {
                            return Some(clause.to_string());
                        }
                    }
                }
                None
            })
            .collect();

        merged_sql.push_str(&alter_clauses.join(", "));
        merged_sql.push(';');

        let mut merged = first.clone();
        merged.details.sql = merged_sql;

        merged
    }

    /// Merge multiple ADD CONSTRAINT operations into a single operation
    fn merge_add_constraints(changes: Vec<SchemaChange>) -> SchemaChange {
        debug!("Merging {} ADD CONSTRAINT operations", changes.len());

        let first = &changes[0];
        let mut merged_sql = String::new();

        // Build a combined ALTER TABLE statement
        merged_sql.push_str(&format!("ALTER TABLE {} ", first.object_name));

        let constraint_clauses: Vec<String> = changes
            .iter()
            .filter_map(|change| {
                let sql = &change.details.sql;
                // Extract the ADD CONSTRAINT part from each SQL statement
                if let Some(add_idx) = sql.to_uppercase().find("ADD CONSTRAINT") {
                    Some(sql[add_idx..].to_string())
                } else {
                    None
                }
            })
            .collect();

        merged_sql.push_str(&constraint_clauses.join(", "));
        merged_sql.push(';');

        let mut merged = first.clone();
        merged.details.sql = merged_sql;

        merged
    }

    /// Check if two changes can be merged
    fn can_merge(change1: &SchemaChange, change2: &SchemaChange) -> bool {
        // Same table and compatible operations
        change1.object_name == change2.object_name &&
            Self::are_compatible_operations(&change1.change_type, &change2.change_type)
    }

    /// Check if two operation types are compatible for merging
    fn are_compatible_operations(type1: &ChangeType, type2: &ChangeType) -> bool {
        matches!(
            (type1, type2),
            (ChangeType::AddColumn, ChangeType::AddColumn) |
                (ChangeType::AlterTable, ChangeType::AlterTable) |
                (ChangeType::DropColumn, ChangeType::DropColumn) |
                (ChangeType::AddConstraint, ChangeType::AddConstraint) |
                (ChangeType::DropConstraint, ChangeType::DropConstraint)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_consecutive_add_columns() {
        let changes = vec![
            SchemaChange::new(
                ChangeType::AddColumn,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD COLUMN email VARCHAR(255)".to_string()
            ),
            SchemaChange::new(
                ChangeType::AddColumn,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD COLUMN phone VARCHAR(20)".to_string()
            )
        ];

        let optimized = MigrationOptimizer::optimize(changes);
        assert_eq!(optimized.len(), 1);
        assert!(optimized[0].details.sql.contains("email"));
        assert!(optimized[0].details.sql.contains("phone"));
    }

    #[test]
    fn test_no_merge_different_tables() {
        let changes = vec![
            SchemaChange::new(
                ChangeType::AddColumn,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD COLUMN email VARCHAR(255)".to_string()
            ),
            SchemaChange::new(
                ChangeType::AddColumn,
                "public".to_string(),
                "orders".to_string(),
                "ALTER TABLE orders ADD COLUMN status VARCHAR(50)".to_string()
            )
        ];

        let optimized = MigrationOptimizer::optimize(changes);
        assert_eq!(optimized.len(), 2);
    }

    #[test]
    fn test_merge_alter_tables() {
        let changes = vec![
            SchemaChange::new(
                ChangeType::AlterTable,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id)".to_string()
            ),
            SchemaChange::new(
                ChangeType::AlterTable,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD CONSTRAINT uk_email UNIQUE (email)".to_string()
            )
        ];

        let optimized = MigrationOptimizer::optimize(changes);
        assert_eq!(optimized.len(), 1);
    }

    #[test]
    fn test_preserve_non_mergeable_operations() {
        let changes = vec![
            SchemaChange::new(
                ChangeType::CreateTable,
                "public".to_string(),
                "users".to_string(),
                "CREATE TABLE users (id SERIAL PRIMARY KEY)".to_string()
            ),
            SchemaChange::new(
                ChangeType::AddColumn,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD COLUMN email VARCHAR(255)".to_string()
            ),
            SchemaChange::new(
                ChangeType::CreateIndex,
                "public".to_string(),
                "idx_users_email".to_string(),
                "CREATE INDEX idx_users_email ON users(email)".to_string()
            )
        ];

        let optimized = MigrationOptimizer::optimize(changes);
        assert_eq!(optimized.len(), 3);
    }

    #[test]
    fn test_merge_add_constraints() {
        let changes = vec![
            SchemaChange::new(
                ChangeType::AddConstraint,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id)".to_string()
            ),
            SchemaChange::new(
                ChangeType::AddConstraint,
                "public".to_string(),
                "users".to_string(),
                "ALTER TABLE users ADD CONSTRAINT uk_email UNIQUE (email)".to_string()
            )
        ];

        let optimized = MigrationOptimizer::optimize(changes);
        assert_eq!(optimized.len(), 1);
    }
}
