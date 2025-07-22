use std::sync::mpsc::{channel, RecvTimeoutError};
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use dimple_db::{sync::SyncEngine, Db};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};
use chrono::Utc;

#[derive(Parser)]
#[command(name = "todo")]
#[command(about = "A CLI Todo app demonstrating DimpleDb sync capabilities")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// S3 endpoint URL
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 bucket name
    #[arg(long)]
    s3_bucket: Option<String>,

    /// S3 region
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,

    /// S3 access key
    #[arg(long)]
    s3_access_key: Option<String>,

    /// S3 secret key
    #[arg(long)]
    s3_secret_key: Option<String>,

    /// Storage prefix/path
    #[arg(long, default_value = "dimple-todos")]
    prefix: String,

    /// Encryption passphrase
    #[arg(long)]
    passphrase: Option<String>,

    /// Database file path (defaults to in-memory if S3 not configured)
    #[arg(long, default_value = "todos.db")]
    db_path: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new todo item
    New {
        /// Todo item text
        text: String,
    },
    /// List all todo items (syncs first)
    List,
    /// Delete a todo item by ID (uses tombstone)
    Delete {
        /// Todo item ID to delete
        id: String,
    },
    /// Sync with remote storage
    Sync,
    /// Watch for changes and sync continuously
    Watch,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct Todo {
    pub id: String,
    pub text: String,
    pub completed: bool,
    pub _deleted: bool,
    pub created_at: String,
    pub updated_at: String,
}

fn get_migrations() -> Migrations<'static> {
    Migrations::new(vec![
        M::up("
            CREATE TABLE Todo (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL,
                completed BOOLEAN NOT NULL DEFAULT 0,
                _deleted BOOLEAN NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        "),
    ])
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    // Open database
    let db = if cli.db_path == ":memory:" {
        Db::open_memory()?
    } else {
        Db::open(&cli.db_path)?
    };

    // Setup schema
    db.migrate(&get_migrations())?;

    // Setup sync engine if S3 credentials provided
    let sync_engine = if let (Some(endpoint), Some(bucket), Some(access_key), Some(secret_key)) = (
        cli.s3_endpoint.as_ref(),
        cli.s3_bucket.as_ref(), 
        cli.s3_access_key.as_ref(),
        cli.s3_secret_key.as_ref()
    ) {
        let mut builder = SyncEngine::builder()
            .s3(endpoint, bucket, &cli.s3_region, access_key, secret_key)?
            .prefix(&cli.prefix);
        
        if let Some(passphrase) = cli.passphrase.as_ref() {
            builder = builder.encrypted(passphrase);
        }
        
        Some(builder.build()?)
    } else {
        println!("Warning: No S3 credentials provided, using in-memory sync for demo");
        Some(SyncEngine::builder().in_memory().prefix(&cli.prefix).build()?)
    };

    match cli.command {
        Commands::New { text } => {
            new_todo(&db, &text)?;
            if let Some(ref sync) = sync_engine {
                println!("Syncing...");
                sync.sync(&db)?;
                println!("Sync complete");
            }
        }
        Commands::List => {
            if let Some(ref sync) = sync_engine {
                println!("Syncing...");
                sync.sync(&db)?;
                println!("Sync complete\n");
            }
            list_todos(&db)?;
        }
        Commands::Delete { id } => {
            delete_todo(&db, &id)?;
            if let Some(ref sync) = sync_engine {
                println!("Syncing...");
                sync.sync(&db)?;
                println!("Sync complete");
            }
        }
        Commands::Sync => {
            if let Some(ref sync) = sync_engine {
                println!("Syncing...");
                sync.sync(&db)?;
                println!("Sync complete");
            } else {
                println!("No sync engine configured");
            }
        }
        Commands::Watch => {
            if let Some(sync) = sync_engine {
                watch_todos(&db, sync)?;
            } else {
                println!("No sync engine configured for watch mode");
            }
        }
    }

    Ok(())
}

fn new_todo(db: &Db, text: &str) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    let todo = Todo {
        text: text.to_string(),
        completed: false,
        _deleted: false,
        created_at: now.clone(),
        updated_at: now,
        ..Default::default()
    };
    
    let saved = db.save(&todo)?;
    let id_suffix = &saved.id[saved.id.len().saturating_sub(8)..];
    println!("Created todo: {} - {}", id_suffix, saved.text);
    Ok(())
}

fn list_todos(db: &Db) -> Result<()> {
    let todos: Vec<Todo> = db.query(
        "SELECT * FROM Todo WHERE _deleted = 0 ORDER BY created_at ASC", 
        ()
    )?;
    
    if todos.is_empty() {
        println!("No todos found");
        return Ok(());
    }
    
    println!("Todos:");
    for todo in todos {
        let status = if todo.completed { "✓" } else { "○" };
        let id_suffix = &todo.id[todo.id.len().saturating_sub(8)..];
        println!("  {} {} - {}", status, id_suffix, todo.text);
    }
    Ok(())
}

fn delete_todo(db: &Db, id: &str) -> Result<()> {
    // Find todos that end with the given ID suffix
    let todos: Vec<Todo> = db.query(
        "SELECT * FROM Todo WHERE id LIKE ? AND _deleted = 0", 
        [format!("%{}", id)]
    )?;
    
    match todos.len() {
        0 => {
            println!("No todo found with ID ending with '{}'", id);
            return Ok(());
        }
        1 => {
            let mut todo = todos[0].clone();
            todo._deleted = true;
            todo.updated_at = Utc::now().to_rfc3339();
            db.save(&todo)?;
            let id_suffix = &todo.id[todo.id.len().saturating_sub(8)..];
            println!("Deleted todo: {} - {}", id_suffix, todo.text);
        }
        _ => {
            println!("Multiple todos found with ID ending with '{}'. Be more specific:", id);
            for todo in todos {
                let id_suffix = &todo.id[todo.id.len().saturating_sub(8)..];
                println!("  {} - {}", id_suffix, todo.text);
            }
        }
    }
    Ok(())
}

fn watch_todos(db: &Db, sync_engine: SyncEngine) -> Result<()> {
    println!("Watching for todo changes... (Press Ctrl+C to exit)");
    println!("Syncing every 3 seconds...\n");
    
    // Set up reactive query for todos
    let (sender, receiver) = channel::<Vec<Todo>>();
    let _subscription = db.query_subscribe(
        "SELECT * FROM Todo WHERE _deleted = 0 ORDER BY created_at ASC",
        (),
        move |todos: Vec<Todo>| {
            let _ = sender.send(todos);
        },
    )?;
    
    // Initial sync
    print!("Initial sync... ");
    sync_engine.sync(&db)?;
    println!("✓");
    
    // Handle todo updates with periodic sync
    let mut last_count = 0;
    let mut sync_counter = 0;
    
    loop {
        match receiver.recv_timeout(Duration::from_secs(3)) {
            Ok(todos) => {
                if todos.len() != last_count {
                    println!("\n=== Todo List Updated ===");
                    if todos.is_empty() {
                        println!("No todos");
                    } else {
                        for todo in &todos {
                            let status = if todo.completed { "✓" } else { "○" };
                            let id_suffix = &todo.id[todo.id.len().saturating_sub(8)..];
                            println!("  {} {} - {}", status, id_suffix, todo.text);
                        }
                    }
                    last_count = todos.len();
                    println!("========================\n");
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // Timeout - perform sync
                sync_counter += 1;
                print!("Syncing... ({})", sync_counter);
                if let Err(e) = sync_engine.sync(&db) {
                    eprintln!("\nSync error: {}", e);
                } else {
                    println!(" ✓");
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                println!("Watch subscription disconnected");
                break;
            }
        }
    }
    
    Ok(())
}

