# Repligrate

**Repligrate** is a tool that automatically generates [pgroll](https://pgroll.com/) migrations by listening to PostgreSQL schema changes via logical replication. It acts as a logical replica to capture DDL (Data Definition Language) operations and converts them into pgroll-compatible migration files.

## Features

- 🔍 **Automatic Schema Change Detection**: Listens to PostgreSQL logical replication stream to detect schema changes
- 📝 **pgroll Migration Generation**: Automatically converts detected schema changes into pgroll migration JSON format
- 🔄 **Logical Replication**: Uses PostgreSQL's native logical replication mechanism for reliable change capture
- 🛡️ **Zero-Downtime Ready**: Generated migrations are compatible with pgroll's zero-downtime migration approach
- 🎯 **Selective Monitoring**: Filter which schemas and tables to monitor
- 📦 **Easy Integration**: Simple CLI interface for integration into CI/CD pipelines

## Architecture

```
PostgreSQL Database
        ↓
   Logical Replication
        ↓
   WAL Message Parser
        ↓
   Schema Change Detector
        ↓
   pgroll Migration Generator
        ↓
   Migration Files (JSON)
```

## Installation

### From Source

```bash
git clone https://github.com/dszczyt/repligrate.git
cd repligrate
cargo build --release
```

The binary will be available at `target/release/repligrate`.

## Quick Start

### 1. Initialize Replication

```bash
export DATABASE_URL="postgres://user:password@localhost:5432/mydb"
repligrate listen --slot-name repligrate_slot --publication-name repligrate_pub
```

### 2. Make Schema Changes

In another terminal, make schema changes to your database:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE
);

ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
```

### 3. Generated Migrations

Repligrate will automatically generate pgroll migration files in the output directory:

```json
{
  "name": "migration_20240101_120000",
  "operations": [
    {
      "create_table": {
        "name": "users",
        "columns": [
          {
            "name": "id",
            "type": "integer",
            "pk": true
          },
          {
            "name": "name",
            "type": "varchar(255)",
            "nullable": false
          }
        ]
      }
    }
  ]
}
```

## Configuration

### Environment Variables

- `DATABASE_URL`: PostgreSQL connection string (required)
- `OUTPUT_DIR`: Directory for generated migrations (default: `./migrations`)

### Configuration File

Create a `repligrate.toml`:

```toml
database_url = "postgres://user:password@localhost:5432/mydb"
output_dir = "./migrations"

[replication]
slot_name = "repligrate_slot"
publication_name = "repligrate_pub"
batch_size = 1000
receive_timeout = 30

[schema_filter]
include_schemas = []
exclude_schemas = ["pg_catalog", "information_schema"]
include_tables = []
exclude_tables = []
```

## CLI Commands

### listen

Start listening for schema changes:

```bash
repligrate listen \
  --slot-name repligrate_slot \
  --publication-name repligrate_pub \
  --tables "public.users,public.orders"
```

Options:
- `--slot-name`: Name of the replication slot (default: `repligrate_slot`)
- `--publication-name`: Name of the publication (default: `repligrate_pub`)
- `--tables`: Comma-separated list of tables to monitor (empty = all)

### status

Check replication status:

```bash
repligrate status
```

### cleanup

Clean up replication resources:

```bash
repligrate cleanup --slot-name repligrate_slot
```

### test

Generate a test migration:

```bash
repligrate test
```

## Supported Schema Changes

Repligrate currently supports the following schema change operations:

- ✅ CREATE TABLE
- ✅ DROP TABLE
- ✅ ALTER TABLE
- ✅ ADD COLUMN
- ✅ DROP COLUMN
- ✅ MODIFY COLUMN
- ✅ CREATE INDEX
- ✅ DROP INDEX
- ⏳ ADD CONSTRAINT
- ⏳ DROP CONSTRAINT

## How It Works

1. **Replication Slot Creation**: Creates a logical replication slot to capture changes
2. **Publication Setup**: Creates a publication for the tables to monitor
3. **WAL Monitoring**: Listens to the Write-Ahead Log (WAL) stream
4. **DDL Detection**: Filters for DDL operations (schema changes)
5. **Migration Generation**: Converts DDL operations to pgroll migration format
6. **File Output**: Writes migrations to JSON files with timestamps

## Requirements

- PostgreSQL 14.0 or later
- Rust 1.70 or later (for building from source)
- `wal_level = logical` in PostgreSQL configuration

## PostgreSQL Configuration

Ensure your PostgreSQL instance is configured for logical replication:

```sql
-- Check current settings
SHOW wal_level;  -- Should be 'logical'
SHOW max_wal_senders;  -- Should be > 0
SHOW max_replication_slots;  -- Should be > 0
```

If needed, update `postgresql.conf`:

```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

Then restart PostgreSQL.

## Development

### Running Tests

```bash
cargo test
```

### Building Documentation

```bash
cargo doc --open
```

## Limitations

- Currently supports basic DDL operations
- Complex migrations may require manual adjustment
- Requires PostgreSQL 14.0+
- Logical replication must be enabled

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Related Projects

- [pgroll](https://pgroll.com/) - Zero-downtime, reversible schema migrations for PostgreSQL
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)

## Support

For issues, questions, or suggestions, please open an issue on GitHub.

