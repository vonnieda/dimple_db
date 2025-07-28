use anyhow::{anyhow, Result};
use dimple_db::db::{Migrations, M};
use dimple_db::{Db, sync::SyncEngine};
use serde::{Deserialize, Serialize};
use slint::{ComponentHandle, VecModel};
use url::Url;
use std::rc::Rc;
use std::time::Duration;

slint::include_modules!();

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
struct Todo {
    id: String,
    title: String,
    completed: bool,
    _deleted: bool,
}

fn main() -> Result<()> {
    env_logger::init();

    // Initialize database
    let db_path = std::env::var("TODO_DB_PATH").unwrap_or_else(|_| "todo-gui.db".to_string());
    let db = Db::open(&db_path)?;
    
    // Run migrations
    db.migrate(&Migrations::new(vec![
        M::up("CREATE TABLE Todo (id TEXT PRIMARY KEY, title TEXT NOT NULL, completed BOOLEAN NOT NULL DEFAULT false, _deleted BOOLEAN NOT NULL DEFAULT false)"),
    ]))?;
    
    // Get sync URL for later use
    let sync_url = std::env::var("TODO_SYNC_URL").ok();
    
    // Create the UI
    let ui = TodoApp::new()?;
    let ui_handle = ui.as_weak();
    
    
    // Set up query subscription for real-time updates (calls callback immediately with current data)
    let ui_handle_for_subscription = ui_handle.clone();
    let _subscription = db.query_subscribe::<Todo, _, _>(
        "SELECT * FROM Todo WHERE _deleted = false ORDER BY id",
        (),
        move |todos| {
            let _ = ui_handle_for_subscription.upgrade_in_event_loop(move |ui| {
                update_ui_todos(&ui, todos);
            });
        },
    );
    
    // Handle UI callbacks
    let db_clone = db.clone();
    ui.on_add_todo(move |title| {
        let title = title.to_string();
        if !title.is_empty() {
            let _ = db_clone.save(&Todo {
                title,
                ..Default::default()
            });
        }
    });
    
    let db_clone = db.clone();
    ui.on_toggle_todo(move |id| {
        let id = id.to_string();
        if let Ok(Some(mut todo)) = db_clone.get::<Todo>(&id) {
            todo.completed = !todo.completed;
            let _ = db_clone.save(&todo);
        }
    });
    
    let db_clone = db.clone();
    ui.on_delete_todo(move |id| {
        let id = id.to_string();
        if let Ok(Some(mut todo)) = db_clone.get::<Todo>(&id) {
            todo._deleted = true;
            let _ = db_clone.save(&todo);
        }
    });
    
    // Background sync task
    if sync_url.is_some() {
        let sync_url_clone = sync_url.clone();
        let db_clone = db.clone();
        std::thread::spawn(move || {
            let sync_engine = parse_sync_url(&sync_url_clone.unwrap()).unwrap();
            loop {
                let _ = sync_engine.sync(&db_clone);
                std::thread::sleep(Duration::from_secs(5));
            }
        });
    }
    
    // Run the UI (this will block until the window is closed)
    ui.run()?;
    
    // Keep the subscription alive until the UI exits
    drop(_subscription);
    
    Ok(())
}

fn update_ui_todos(ui: &TodoApp, todos: Vec<Todo>) {
    let todo_items: Vec<TodoItem> = todos
        .into_iter()
        .map(|todo| TodoItem {
            id: todo.id.into(),
            title: todo.title.into(),
            completed: todo.completed,
        })
        .collect();
    
    let model = Rc::new(VecModel::from(todo_items));
    ui.set_todos(model.into());
}

/// Parse a url in one of the following formats and return a SyncEngine
/// configured with the results.
/// 
/// Valid url formats:
/// - s3://access_key:secret_key@endpoint/bucket/prefix?region=us-east-1
/// - memory://prefix
/// - file://base_path
/// 
fn parse_sync_url(url: &str) -> Result<SyncEngine> {
    let url = Url::parse(url)?;
    match url.scheme() {
        "s3" => {
            let access_key = url.username();
            let secret_key = url.password().ok_or_else(|| anyhow!("secret key is required"))?;
            let endpoint = url.host_str().ok_or_else(|| anyhow!("endpoint is required"))?;
            let path_segs = url.path_segments()
                .ok_or_else(|| anyhow!("bucket is required"))?
                .collect::<Vec<_>>();
            let bucket_name = path_segs.get(0).ok_or_else(|| anyhow!("bucket name is required"))?;
            if bucket_name.is_empty() {
                return Err(anyhow!("bucket name is required"))
            }
            let prefix = path_segs[1..].join("/");
            let region = url.query_pairs().find(|qp| qp.0 == "region")
                .map(|qp| qp.1).unwrap_or_default();
            SyncEngine::builder()
                .s3(endpoint, bucket_name, &region, access_key, secret_key)?
                // .encrypted("correct horse battery staple")
                .prefix(&prefix)
                .build()
        },
        "memory" => {
            let prefix = url.path();
            SyncEngine::builder()
                .in_memory()
                .prefix(&prefix)
                .build()
        },
        "file" => {
            let base_path = url.path();
            SyncEngine::builder()
                .local(base_path)
                .build()
        },
        _ => Err(anyhow!("invalid sync url format")),
    }
}

#[cfg(test)]
mod tests {
    use crate::parse_sync_url;

    #[test]
    fn test_parse_sync_url() {
        assert!(parse_sync_url("s3://access_key:secret_key@endpoint/bucket/prefix1/prefix2?region=us-east-1").is_ok());
        assert!(parse_sync_url("s3://access_key:secret_key@endpoint/bucket/prefix1/prefix2").is_ok());
        assert!(parse_sync_url("s3://access_key:secret_key@endpoint/bucket").is_ok());
        assert!(parse_sync_url("s3://access_key:secret_key@endpoint/").is_err());
        assert!(parse_sync_url("memory://").is_ok());
        assert!(parse_sync_url("memory://prefix").is_ok());
        assert!(parse_sync_url("file://base_path").is_ok());
        assert!(parse_sync_url("").is_err());
        assert!(parse_sync_url("http://example.com").is_err());
        assert!(parse_sync_url("https://example.com").is_err());
    }
}