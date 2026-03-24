mod commands;
mod error;
mod schema;
mod storage;

use clap::{Parser, Subcommand};
use storage::Storage;

#[derive(Parser)]
#[command(
    name = "searchdb",
    about = "ES in your pocket — embedded search backed by tantivy + Delta Lake",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Data directory for indexes
    #[arg(long, global = true, default_value = ".searchdb")]
    data_dir: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new index from a schema declaration
    New {
        /// Index name
        name: String,
        /// Schema JSON: {"fields":{"name":"keyword","notes":"text"}}
        #[arg(long)]
        schema: String,
        /// Overwrite if index already exists
        #[arg(long, default_value_t = false)]
        overwrite: bool,
    },
}

fn main() {
    env_logger::init();
    let cli = Cli::parse();
    let storage = Storage::new(&cli.data_dir);

    let result = match cli.command {
        Commands::New {
            name,
            schema,
            overwrite,
        } => commands::new_index::run(&storage, &name, &schema, overwrite),
    };

    if let Err(e) = result {
        eprintln!("[searchdb] Error: {e}");
        std::process::exit(1);
    }
}
