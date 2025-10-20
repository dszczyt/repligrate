use anyhow::Result;
use postgres::Client;
use tracing::{debug, info};

/// Database connection manager
pub struct DbConnection {
    client: Client,
}

#[allow(dead_code)]
impl DbConnection {
    /// Create a new database connection
    pub fn new(database_url: &str) -> Result<Self> {
        let client = Client::connect(database_url, postgres::NoTls)?;
        info!("Connected to PostgreSQL database");
        Ok(Self { client })
    }

    /// Create a replication slot
    pub fn create_replication_slot(&mut self, slot_name: &str) -> Result<()> {
        debug!("Creating replication slot: {}", slot_name);

        // Check if slot already exists
        let rows = self.client.query(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )?;

        if !rows.is_empty() {
            info!("Replication slot {} already exists", slot_name);
            return Ok(());
        }

        // Create the slot
        self.client.execute(
            &format!(
                "SELECT * FROM pg_create_logical_replication_slot('{}', 'test_decoding')",
                slot_name
            ),
            &[],
        )?;

        info!("Created replication slot: {}", slot_name);
        Ok(())
    }

    /// Create a publication for specific tables
    pub fn create_publication(
        &mut self,
        pub_name: &str,
        tables: Option<Vec<String>>,
    ) -> Result<()> {
        debug!("Creating publication: {}", pub_name);

        // Check if publication already exists
        let rows = self.client.query(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&pub_name],
        )?;

        if !rows.is_empty() {
            info!("Publication {} already exists", pub_name);
            return Ok(());
        }

        // Create publication
        let sql = if let Some(table_list) = tables {
            if table_list.is_empty() {
                format!("CREATE PUBLICATION {} FOR ALL TABLES", pub_name)
            } else {
                let tables_str = table_list.join(", ");
                format!("CREATE PUBLICATION {} FOR TABLE {}", pub_name, tables_str)
            }
        } else {
            format!("CREATE PUBLICATION {} FOR ALL TABLES", pub_name)
        };

        self.client.execute(&sql, &[])?;
        info!("Created publication: {}", pub_name);
        Ok(())
    }

    /// Drop a replication slot
    pub fn drop_replication_slot(&mut self, slot_name: &str) -> Result<()> {
        debug!("Dropping replication slot: {}", slot_name);

        self.client.execute(
            &format!("SELECT pg_drop_replication_slot('{}')", slot_name),
            &[],
        )?;

        info!("Dropped replication slot: {}", slot_name);
        Ok(())
    }

    /// Drop a publication
    pub fn drop_publication(&mut self, pub_name: &str) -> Result<()> {
        debug!("Dropping publication: {}", pub_name);

        self.client
            .execute(&format!("DROP PUBLICATION IF EXISTS {}", pub_name), &[])?;

        info!("Dropped publication: {}", pub_name);
        Ok(())
    }

    /// Get the current LSN (Log Sequence Number)
    pub fn get_current_lsn(&mut self) -> Result<String> {
        let row = self
            .client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])?;
        let lsn: String = row.get(0);
        Ok(lsn)
    }

    /// Get replication slot info
    pub fn get_slot_info(&mut self, slot_name: &str) -> Result<Option<SlotInfo>> {
        let rows = self.client.query(
            "SELECT slot_name, slot_type, datoid, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name]
        )?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        Ok(Some(SlotInfo {
            slot_name: row.get(0),
            slot_type: row.get(1),
            datoid: row.get(2),
            confirmed_flush_lsn: row.get(3),
        }))
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SlotInfo {
    pub slot_name: String,
    pub slot_type: String,
    pub datoid: i32,
    pub confirmed_flush_lsn: String,
}
