mod cli;
mod config;
mod db;
mod migration;
mod optimizer;
mod replication;
mod schema;
mod state;
mod wal;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber
        ::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter
                ::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .init();

    let args = cli::Args::parse();
    cli::run(args).await
}
