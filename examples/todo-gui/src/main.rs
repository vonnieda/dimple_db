use anyhow::Result;
use dimple_db::{Db, sync::SyncEngine};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};
use slint::{ComponentHandle, VecModel};
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
            let sync_engine = create_sync_engine(&sync_url_clone.unwrap()).unwrap();
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


fn create_sync_engine(sync_url: &str) -> Result<SyncEngine> {
    // Parse URL format: s3://access_key:secret_key@endpoint/bucket/region
    if let Some(url) = sync_url.strip_prefix("s3://") {
        let parts: Vec<&str> = url.splitn(2, '@').collect();
        if parts.len() == 2 {
            let creds: Vec<&str> = parts[0].splitn(2, ':').collect();
            let location: Vec<&str> = parts[1].splitn(3, '/').collect();
            
            if creds.len() == 2 && location.len() == 3 {
                let engine = SyncEngine::builder()
                    .s3(location[0], location[1], location[2], creds[0], creds[1])?
                    .prefix("todo-gui")
                    .build()?;
                Ok(engine)
            } else {
                Err(anyhow::anyhow!("Invalid S3 URL format"))
            }
        } else {
            Err(anyhow::anyhow!("Invalid S3 URL format"))
        }
    } else {
        Err(anyhow::anyhow!("Only S3 URLs are supported"))
    }
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