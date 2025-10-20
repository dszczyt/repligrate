use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;

use crate::config::Config;
use crate::migration::{MigrationGenerator, MigrationWriter};
use crate::replication::ReplicationListener;
use crate::schema::{ChangeType, SchemaChange};

#[derive(Parser)]
#[command(name = "repligrate")]
#[command(
    about = "Generate pgroll migrations by listening to PostgreSQL changes via logical replication",
    long_about = None
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,

    /// Configuration file path
    #[arg(global = true, short, long)]
    pub config: Option<PathBuf>,

    /// Database URL
    #[arg(global = true, long)]
    pub database_url: Option<String>,

    /// Output directory for migrations
    #[arg(global = true, short, long)]
    pub output: Option<PathBuf>,

    /// Verbosity level
    #[arg(global = true, short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize replication slot and start listening for changes
    Listen {
        /// Replication slot name
        #[arg(long, default_value = "repligrate_slot")]
        slot_name: String,

        /// Publication name
        #[arg(long, default_value = "repligrate_pub")]
        publication_name: String,

        /// Tables to monitor (comma-separated, empty = all)
        #[arg(long)]
        tables: Option<String>,
    },

    /// Show current replication status
    Status,

    /// Pause the replication listener
    Pause {
        /// Replication slot name
        #[arg(long, default_value = "repligrate_slot")]
        slot_name: String,
    },

    /// Continue the replication listener
    Continue {
        /// Replication slot name
        #[arg(long, default_value = "repligrate_slot")]
        slot_name: String,
    },

    /// Clean up replication slot
    Cleanup {
        /// Replication slot name
        #[arg(long, default_value = "repligrate_slot")]
        slot_name: String,
    },

    /// Generate a test migration
    Test,
}

pub async fn run(args: Args) -> Result<()> {
    // Load configuration (optional for test command)
    let mut config = if let Some(config_path) = args.config {
        Config::from_file(&config_path)?
    } else if matches!(args.command, Commands::Test) {
        // For test command, use default config
        Config::default()
    } else {
        Config::from_env()?
    };

    // Override with CLI arguments
    if let Some(db_url) = args.database_url {
        config.database_url = db_url;
    }
    if let Some(output) = args.output {
        config.output_dir = output;
    }

    info!("Configuration loaded: {:?}", config);

    match args.command {
        Commands::Listen {
            slot_name,
            publication_name,
            tables,
        } => {
            info!("Starting replication listener");
            let mut listener =
                ReplicationListener::new(config, slot_name, publication_name).await?;
            listener.listen(tables).await?;
        }
        Commands::Status => {
            info!("Checking replication status");
            // TODO: Implement status check
            println!("Status check not yet implemented");
        }
        Commands::Pause { slot_name: _ } => {
            info!("Pause command received");
            println!(
                "Note: Pause/Unpause commands require the listener to be running in another process"
            );
            println!(
                "Use a signal handler or IPC mechanism to communicate with the running listener"
            );
            println!("For now, you can use Ctrl+C to stop the listener");
        }
        Commands::Continue { slot_name: _ } => {
            info!("Continue command received");
            println!(
                "Note: Pause/Continue commands require the listener to be running in another process"
            );
            println!(
                "Use a signal handler or IPC mechanism to communicate with the running listener"
            );
            println!("For now, you can use Ctrl+C to stop the listener");
        }
        Commands::Cleanup { slot_name } => {
            info!("Cleaning up replication slot: {}", slot_name);
            // TODO: Implement cleanup
            println!("Cleanup not yet implemented");
        }
        Commands::Test => {
            info!("Generating test migration");
            generate_test_migration(&config)?;
        }
    }

    Ok(())
}

/// Generate a test migration for demonstration purposes
fn generate_test_migration(config: &Config) -> Result<()> {
    info!("Creating test schema changes");

    // Create sample schema changes
    let changes = vec![
        SchemaChange::new(
            ChangeType::CreateTable,
            "public".to_string(),
            "users".to_string(),
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)".to_string(),
        ),
        SchemaChange::new(
            ChangeType::AddColumn,
            "public".to_string(),
            "users".to_string(),
            "ALTER TABLE users ADD COLUMN email VARCHAR(255) UNIQUE".to_string(),
        ),
        SchemaChange::new(
            ChangeType::CreateIndex,
            "public".to_string(),
            "idx_users_email".to_string(),
            "CREATE INDEX idx_users_email ON users(email)".to_string(),
        ),
    ];

    info!("Generating migration from {} schema changes", changes.len());
    let migration = MigrationGenerator::generate("test_migration".to_string(), changes)?;

    let filepath = MigrationWriter::write(&migration, &config.output_dir)?;
    println!("✓ Test migration generated: {}", filepath.display());
    println!("✓ Migration content:");
    println!("{}", migration.to_json()?);

    Ok(())
}
