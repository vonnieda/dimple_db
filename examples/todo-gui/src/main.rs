use anyhow::Result;
use dimple_db::{Db, sync::SyncEngine};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};
use slint::{ComponentHandle, VecModel, Image, Rgba8Pixel, SharedPixelBuffer};
use std::rc::Rc;
use std::time::Duration;
use qrcode::{QrCode, Color};

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
    
    // Set QR code if sync URL is available
    if let Some(ref url) = sync_url {
        if let Ok(qr_image) = generate_qr_code_image(url) {
            ui.set_qr_code(qr_image);
        }
    }
    
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

fn generate_qr_code_image(sync_url: &str) -> Result<Image> {
    let qr = QrCode::new(sync_url)?;
    
    // Get the QR code as a 2D boolean matrix
    let modules = qr.to_colors();
    let size = (modules.len() as f64).sqrt() as u32;
    
    // Scale up the QR code for better visibility
    let scale = 8;
    let scaled_size = size * scale;
    
    let mut rgba_buffer = vec![0u8; (scaled_size * scaled_size * 4) as usize];
    
    for y in 0..size {
        for x in 0..size {
            let color = modules[(y * size + x) as usize];
            let rgba = match color {
                Color::Dark => [0u8, 0, 0, 255],
                Color::Light => [255u8, 255, 255, 255],
            };
            
            // Scale up the pixel
            for dy in 0..scale {
                for dx in 0..scale {
                    let scaled_x = x * scale + dx;
                    let scaled_y = y * scale + dy;
                    let idx = ((scaled_y * scaled_size + scaled_x) * 4) as usize;
                    
                    if idx + 3 < rgba_buffer.len() {
                        rgba_buffer[idx] = rgba[0];     // R
                        rgba_buffer[idx + 1] = rgba[1]; // G
                        rgba_buffer[idx + 2] = rgba[2]; // B
                        rgba_buffer[idx + 3] = rgba[3]; // A
                    }
                }
            }
        }
    }
    
    let pixel_buffer = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(
        &rgba_buffer,
        scaled_size,
        scaled_size,
    );
    
    Ok(Image::from_rgba8(pixel_buffer))
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