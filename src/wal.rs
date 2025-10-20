use anyhow::Result;
use tracing::{debug, info};

/// Represents a WAL (Write-Ahead Log) message
#[derive(Debug, Clone)]
pub struct WalMessage {
    pub lsn: String,
    pub timestamp: i64,
    pub content: String,
    pub is_commit: bool,
}

/// Parser for WAL messages
pub struct WalMessageParser;

impl WalMessageParser {
    /// Parse a WAL message string
    pub fn parse(message: &str) -> Result<Option<WalMessage>> {
        debug!("Parsing WAL message: {}", message);

        // Extract LSN from message (format: "LSN X/Y")
        let lsn = Self::extract_lsn(message).unwrap_or_else(|| "0/0".to_string());

        // Check if this is a commit message
        let is_commit = message.contains("COMMIT") || message.contains("commit");

        // Extract SQL content
        let content = message.to_string();

        Ok(Some(WalMessage {
            lsn,
            timestamp: chrono::Utc::now().timestamp_millis(),
            content,
            is_commit,
        }))
    }

    /// Extract LSN from message
    fn extract_lsn(message: &str) -> Option<String> {
        // Look for pattern like "0/12345678"
        let parts: Vec<&str> = message.split_whitespace().collect();
        for part in parts {
            if part.contains('/') && part.len() > 2 {
                let hex_parts: Vec<&str> = part.split('/').collect();
                if hex_parts.len() == 2 {
                    // Validate hex format
                    if hex_parts[0].chars().all(|c| c.is_ascii_hexdigit())
                        && hex_parts[1].chars().all(|c| c.is_ascii_hexdigit())
                    {
                        return Some(part.to_string());
                    }
                }
            }
        }
        None
    }

    /// Extract SQL statement from WAL message
    pub fn extract_sql(message: &str) -> Option<String> {
        // Look for common SQL keywords
        let sql_keywords = [
            "CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
        ];

        for keyword in &sql_keywords {
            if message.to_uppercase().contains(keyword) {
                return Some(message.to_string());
            }
        }

        None
    }

    /// Check if message contains DDL (Data Definition Language) operations
    pub fn is_ddl(message: &str) -> bool {
        let ddl_keywords = [
            "CREATE TABLE",
            "DROP TABLE",
            "ALTER TABLE",
            "CREATE INDEX",
            "DROP INDEX",
            "CREATE SCHEMA",
            "DROP SCHEMA",
            "ADD COLUMN",
            "DROP COLUMN",
            "ALTER COLUMN",
        ];

        let upper = message.to_uppercase();
        ddl_keywords.iter().any(|keyword| upper.contains(keyword))
    }

    /// Check if message contains DML (Data Manipulation Language) operations
    pub fn is_dml(message: &str) -> bool {
        let dml_keywords = ["INSERT", "UPDATE", "DELETE", "TRUNCATE"];
        let upper = message.to_uppercase();
        dml_keywords.iter().any(|keyword| upper.contains(keyword))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_lsn() {
        let message = "LSN 0/12345678 CREATE TABLE users";
        let lsn = WalMessageParser::extract_lsn(message);
        assert_eq!(lsn, Some("0/12345678".to_string()));
    }

    #[test]
    fn test_is_ddl() {
        assert!(WalMessageParser::is_ddl("CREATE TABLE users (id INT)"));
        assert!(WalMessageParser::is_ddl("ALTER TABLE users ADD COLUMN name VARCHAR"));
        assert!(!WalMessageParser::is_ddl("INSERT INTO users VALUES (1)"));
    }

    #[test]
    fn test_is_dml() {
        assert!(WalMessageParser::is_dml("INSERT INTO users VALUES (1)"));
        assert!(WalMessageParser::is_dml("UPDATE users SET name = 'John'"));
        assert!(!WalMessageParser::is_dml("CREATE TABLE users (id INT)"));
    }
}

