use std::sync::mpsc::{channel, RecvTimeoutError};
use std::time::Duration;

use anyhow::{Result, Context};
use clap::{Parser, Subcommand};
use dimple_db::{sync::SyncEngine, Db};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};
use chrono::Utc;
use url::Url;

#[derive(Parser)]
#[command(name = "todo")]
#[command(about = "A CLI Todo app demonstrating DimpleDb sync capabilities")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Database URL (memory:// or file://path/to/db.sqlite)
    #[arg(long = "database-url", default_value = "memory://")]
    database: String,

    /// Sync storage URL (memory://prefix, file://path/prefix, or s3://access_key:secret_key@endpoint/bucket/prefix?region=us-east-1)
    #[arg(long = "sync-url", default_value = "memory://dimple-todos")]
    sync_storage: String,

    /// Encryption passphrase for sync storage
    #[arg(long = "sync-passphrase")]
    passphrase: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new todo item
    New {
        /// Todo item text
        text: String,
    },
    /// List all todo items
    List,
    /// Delete a todo item by ID (uses tombstone)
    Delete {
        /// Todo item ID to delete
        id: String,
    },
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

fn parse_database_url(url_str: &str) -> Result<Db> {
    let url = Url::parse(url_str)
        .context("Invalid database URL")?;
    
    match url.scheme() {
        "memory" => Db::open_memory(),
        "file" => {
            let path = url.path();
            if path.is_empty() || path == "/" {
                return Err(anyhow::anyhow!("file:// URL must include a path"));
            }
            // Remove leading slash on Unix-like systems
            let path = if path.starts_with('/') && !cfg!(windows) {
                &path[1..]
            } else {
                path
            };
            Db::open(path)
        },
        scheme => Err(anyhow::anyhow!("Unsupported database URL scheme: {}. Use memory:// or file://", scheme))
    }
}

// TODO sus
fn parse_s3_url(url_str: &str, passphrase: Option<&String>) -> Result<SyncEngine> {
    // Format: s3://access_key:secret_key@endpoint/bucket/prefix?region=us-east-1
    // Parse manually due to special characters in keys
    if !url_str.starts_with("s3://") {
        return Err(anyhow::anyhow!("Invalid S3 URL format"));
    }
    
    let after_scheme = &url_str[5..]; // Skip "s3://"
    let (creds_and_rest, query) = if let Some(pos) = after_scheme.find('?') {
        (&after_scheme[..pos], Some(&after_scheme[pos+1..]))
    } else {
        (after_scheme, None)
    };
    
    let (access_key, secret_key, rest) = if let Some(at_pos) = creds_and_rest.rfind('@') {
        let creds = &creds_and_rest[..at_pos];
        let rest = &creds_and_rest[at_pos+1..];
        
        let (ak, sk) = creds.split_once(':')
            .ok_or_else(|| anyhow::anyhow!("S3 URL must include access_key:secret_key"))?;
        (ak.to_string(), sk.to_string(), rest)
    } else {
        return Err(anyhow::anyhow!("S3 URL must include credentials: s3://access_key:secret_key@endpoint/bucket/prefix"));
    };
    
    // Parse endpoint/bucket/prefix from rest
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() < 3 {
        return Err(anyhow::anyhow!("S3 URL must include endpoint, bucket, and prefix"));
    }
    
    let endpoint = format!("https://{}", parts[0]);
    let bucket = parts[1];
    let prefix = parts[2..].join("/");
    
    // Parse region from query string
    let region = if let Some(query_str) = query {
        query_str.split('&')
            .find_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                if k == "region" { Some(v.to_string()) } else { None }
            })
            .unwrap_or_else(|| "us-east-1".to_string())
    } else {
        "us-east-1".to_string()
    };
    
    let mut builder = SyncEngine::builder()
        .s3(&endpoint, bucket, &region, &access_key, &secret_key)?
        .prefix(&prefix);
    
    if let Some(pass) = passphrase {
        builder = builder.encrypted(pass);
    }
    
    builder.build()
}

// TODO double sus
fn parse_sync_storage_url(url_str: &str, passphrase: Option<&String>) -> Result<SyncEngine> {
    // For S3 URLs with special characters, parse manually
    if url_str.starts_with("s3://") {
        return parse_s3_url(url_str, passphrase);
    }
    
    let url = Url::parse(url_str)
        .context("Invalid sync storage URL")?;
    
    match url.scheme() {
        "memory" => {
            let prefix = url.path().trim_start_matches('/');
            // For URLs like memory://prefix, url.path() returns empty, but host contains the prefix
            let prefix = if prefix.is_empty() {
                url.host_str().unwrap_or("")
            } else {
                prefix
            };
            
            if prefix.is_empty() {
                return Err(anyhow::anyhow!("memory:// URL must include a prefix"));
            }
            
            let mut builder = SyncEngine::builder()
                .in_memory()
                .prefix(prefix);
            
            if let Some(pass) = passphrase {
                builder = builder.encrypted(pass);
            }
            
            builder.build()
        },
        "file" => {
            let full_path = url.path();
            if full_path.is_empty() || full_path == "/" {
                return Err(anyhow::anyhow!("file:// URL must include a path and prefix"));
            }
            
            // Find the last component as prefix
            let path_parts: Vec<&str> = full_path.split('/').filter(|s| !s.is_empty()).collect();
            if path_parts.is_empty() {
                return Err(anyhow::anyhow!("file:// URL must include both directory and prefix"));
            }
            
            let prefix = path_parts.last().unwrap();
            let dir_path = if path_parts.len() > 1 {
                path_parts[..path_parts.len()-1].join("/")
            } else {
                ".".to_string()
            };
            
            let mut builder = SyncEngine::builder()
                .local(&dir_path)
                .prefix(prefix);
            
            if let Some(pass) = passphrase {
                builder = builder.encrypted(pass);
            }
            
            builder.build()
        },
        "s3" => {
            // This case should not be reached since we handle it above
            unreachable!("S3 URLs are handled in parse_s3_url")
        },
        scheme => Err(anyhow::anyhow!("Unsupported sync storage URL scheme: {}. Use memory://, file://, or s3://", scheme))
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    // Parse and open database
    let db = parse_database_url(&cli.database)?;

    // Setup schema
    db.migrate(&get_migrations())?;

    // Parse and setup sync engine
    let sync_engine = Some(parse_sync_storage_url(&cli.sync_storage, cli.passphrase.as_ref())?);

    // Helper function to sync before and after operations
    let do_sync = |sync: &Option<SyncEngine>, db: &Db, label: &str| -> Result<()> {
        if let Some(ref sync_engine) = sync {
            println!("{}...", label);
            sync_engine.sync(db)?;
            println!("✓");
        }
        Ok(())
    };

    match cli.command {
        Some(Commands::New { text }) => {
            do_sync(&sync_engine, &db, "Syncing before adding")?;
            new_todo(&db, &text)?;
            do_sync(&sync_engine, &db, "Syncing after adding")?;
            list_todos(&db)?;
        }
        Some(Commands::List) | None => {
            do_sync(&sync_engine, &db, "Syncing")?;
            list_todos(&db)?;
        }
        Some(Commands::Delete { id }) => {
            do_sync(&sync_engine, &db, "Syncing before deleting")?;
            delete_todo(&db, &id)?;
            do_sync(&sync_engine, &db, "Syncing after deleting")?;
            list_todos(&db)?;
        }
        Some(Commands::Watch) => {
            if let Some(sync) = sync_engine {
                watch_todos(&db, sync)?;
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

