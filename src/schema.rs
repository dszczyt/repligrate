use serde::{ Deserialize, Serialize };
use std::collections::HashMap;

/// Represents a detected schema change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    pub change_type: ChangeType,
    pub schema_name: String,
    pub object_name: String,
    pub details: ChangeDetails,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeType {
    CreateTable,
    DropTable,
    AlterTable,
    AddColumn,
    DropColumn,
    ModifyColumn,
    AddConstraint,
    DropConstraint,
    CreateIndex,
    DropIndex,
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeDetails {
    pub sql: String,
    pub columns: Option<Vec<ColumnInfo>>,
    pub constraints: Option<Vec<ConstraintInfo>>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub constraints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintInfo {
    pub name: String,
    pub constraint_type: String,
    pub definition: String,
}

#[allow(dead_code)]
impl SchemaChange {
    pub fn new(
        change_type: ChangeType,
        schema_name: String,
        object_name: String,
        sql: String
    ) -> Self {
        Self {
            change_type,
            schema_name,
            object_name,
            details: ChangeDetails {
                sql,
                columns: None,
                constraints: None,
                metadata: HashMap::new(),
            },
            timestamp: chrono::Utc::now(),
        }
    }

    pub fn with_columns(mut self, columns: Vec<ColumnInfo>) -> Self {
        self.details.columns = Some(columns);
        self
    }

    pub fn with_constraints(mut self, constraints: Vec<ConstraintInfo>) -> Self {
        self.details.constraints = Some(constraints);
        self
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.details.metadata.insert(key, value);
        self
    }
}

/// Parser for SQL statements to extract schema changes
pub struct SchemaChangeParser;

#[allow(dead_code)]
impl SchemaChangeParser {
    /// Parse SQL statement and extract schema change information
    pub fn parse(sql: &str) -> Option<(ChangeType, String)> {
        let sql_upper = sql.trim().to_uppercase();

        if sql_upper.starts_with("CREATE TABLE") {
            Some((ChangeType::CreateTable, extract_table_name(sql)))
        } else if sql_upper.starts_with("DROP TABLE") {
            Some((ChangeType::DropTable, extract_table_name(sql)))
        } else if sql_upper.starts_with("ALTER TABLE") {
            if sql_upper.contains("ADD COLUMN") {
                Some((ChangeType::AddColumn, extract_table_name(sql)))
            } else if sql_upper.contains("DROP COLUMN") {
                Some((ChangeType::DropColumn, extract_table_name(sql)))
            } else if sql_upper.contains("ALTER COLUMN") {
                Some((ChangeType::ModifyColumn, extract_table_name(sql)))
            } else if sql_upper.contains("ADD CONSTRAINT") {
                Some((ChangeType::AddConstraint, extract_table_name(sql)))
            } else if sql_upper.contains("DROP CONSTRAINT") {
                Some((ChangeType::DropConstraint, extract_table_name(sql)))
            } else {
                Some((ChangeType::AlterTable, extract_table_name(sql)))
            }
        } else if sql_upper.starts_with("CREATE INDEX") {
            Some((ChangeType::CreateIndex, extract_index_name(sql)))
        } else if sql_upper.starts_with("DROP INDEX") {
            Some((ChangeType::DropIndex, extract_index_name(sql)))
        } else {
            Some((
                ChangeType::Other(
                    sql_upper.split_whitespace().next().unwrap_or("UNKNOWN").to_string()
                ),
                String::new(),
            ))
        }
    }
}

#[allow(unused_parens)]
fn trim_quotes(s: &str) -> &str {
    s.trim_matches(|c| (c == '"' || c == '`'))
}

#[allow(dead_code)]
fn extract_table_name(sql: &str) -> String {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    for (i, part) in parts.iter().enumerate() {
        if part.to_uppercase() == "TABLE" && i + 1 < parts.len() {
            return trim_quotes(parts[i + 1]).to_string();
        }
    }
    String::new()
}

#[allow(dead_code)]
fn extract_index_name(sql: &str) -> String {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    for (i, part) in parts.iter().enumerate() {
        if part.to_uppercase() == "INDEX" && i + 1 < parts.len() {
            return trim_quotes(parts[i + 1]).to_string();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))";
        let result = SchemaChangeParser::parse(sql);
        assert!(result.is_some());
        let (change_type, _) = result.unwrap();
        assert_eq!(change_type, ChangeType::CreateTable);
    }

    #[test]
    fn test_parse_add_column() {
        let sql = "ALTER TABLE users ADD COLUMN email VARCHAR(255)";
        let result = SchemaChangeParser::parse(sql);
        assert!(result.is_some());
        let (change_type, _) = result.unwrap();
        assert_eq!(change_type, ChangeType::AddColumn);
    }

    #[test]
    fn test_parse_add_constraint() {
        let sql = "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id)";
        let result = SchemaChangeParser::parse(sql);
        assert!(result.is_some());
        let (change_type, _) = result.unwrap();
        assert_eq!(change_type, ChangeType::AddConstraint);
    }

    #[test]
    fn test_parse_drop_constraint() {
        let sql = "ALTER TABLE users DROP CONSTRAINT pk_users";
        let result = SchemaChangeParser::parse(sql);
        assert!(result.is_some());
        let (change_type, _) = result.unwrap();
        assert_eq!(change_type, ChangeType::DropConstraint);
    }

    #[test]
    fn test_parse_add_unique_constraint() {
        let sql = "ALTER TABLE users ADD CONSTRAINT uk_email UNIQUE (email)";
        let result = SchemaChangeParser::parse(sql);
        assert!(result.is_some());
        let (change_type, _) = result.unwrap();
        assert_eq!(change_type, ChangeType::AddConstraint);
    }

    #[test]
    fn test_parse_add_foreign_key() {
        let sql =
            "ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)";
        let result = SchemaChangeParser::parse(sql);
        assert!(result.is_some());
        let (change_type, _) = result.unwrap();
        assert_eq!(change_type, ChangeType::AddConstraint);
    }
}
