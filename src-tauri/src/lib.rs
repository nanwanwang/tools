mod diagnostics;
mod models;
mod redis_client;
mod store;
mod tdengine_client;
mod workspace;

use diagnostics::run_health_check;
use models::{
    ConnectionHealth, ConnectionInput, ConnectionRecord, RedisBrowser, RedisBulkDeleteResult,
    RedisCliResponse, RedisHelperEntry, RedisMonitorSession, RedisMonitorSnapshot, RedisStreamData,
    TdengineObjectDetail, TdengineQueryResult, WorkspaceSnapshot,
};
use redis_client::{
    bulk_delete_keys, create_key, delete_key, delete_stream_entry, execute_cli,
    list_helper_entries, load_browser, load_stream_data, save_key_value, set_string_value,
    start_monitor_session, stop_monitor_session, update_key_ttl, update_monitor_session,
    upsert_stream_entry,
};
use store::Store;
use tdengine_client::{
    execute_query as execute_tdengine_query, load_catalog as load_tdengine_catalog,
    load_object_detail as load_tdengine_object_detail, run_health_check as run_tdengine_health_check,
};
use workspace::build_workspace_snapshot;

fn with_database(mut connection: ConnectionRecord, database: Option<u32>) -> ConnectionRecord {
    if let Some(database) = database {
        connection.database_name = database.to_string();
    }
    connection
}

#[tauri::command]
fn list_connections(app: tauri::AppHandle) -> Result<Vec<ConnectionRecord>, String> {
    Store::new(&app)?.list_connections()
}

#[tauri::command]
fn save_connection(
    app: tauri::AppHandle,
    input: ConnectionInput,
    secret: Option<String>,
) -> Result<ConnectionRecord, String> {
    Store::new(&app)?.save_connection(input, secret)
}

#[tauri::command]
fn delete_connection(app: tauri::AppHandle, id: String) -> Result<(), String> {
    Store::new(&app)?.delete_connection(&id)
}

#[tauri::command]
fn toggle_favorite(
    app: tauri::AppHandle,
    id: String,
    favorite: bool,
) -> Result<ConnectionRecord, String> {
    Store::new(&app)?.toggle_favorite(&id, favorite)
}

#[tauri::command]
fn touch_connection(app: tauri::AppHandle, id: String) -> Result<(), String> {
    Store::new(&app)?.touch_connection(&id)
}

#[tauri::command]
fn health_check(app: tauri::AppHandle, id: String) -> Result<ConnectionHealth, String> {
    let store = Store::new(&app)?;
    let connection = store.get_connection(&id)?;
    let secret = if connection.kind == "tdengine" {
        store.get_secret(&id)?
    } else {
        None
    };
    let health = run_health_check(&connection, secret);
    store.update_last_checked(&id, &health.checked_at)?;
    Ok(health)
}

#[tauri::command]
fn get_workspace_snapshot(app: tauri::AppHandle, id: String) -> Result<WorkspaceSnapshot, String> {
    let connection = Store::new(&app)?.get_connection(&id)?;
    Ok(build_workspace_snapshot(&connection))
}

#[tauri::command]
fn load_redis_browser(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    pattern: String,
    limit: usize,
    selected_key: Option<String>,
    search_mode: Option<String>,
    type_filter: Option<String>,
    selected_key_ids: Option<Vec<String>>,
) -> Result<RedisBrowser, String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    load_browser(
        &connection,
        secret,
        pattern,
        limit,
        selected_key,
        search_mode,
        type_filter,
        selected_key_ids.unwrap_or_default(),
    )
}

#[tauri::command]
fn redis_set_string_value(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key: String,
    value: String,
) -> Result<(), String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    set_string_value(&connection, secret, &key, &value)
}

#[tauri::command]
fn redis_create_key(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key_type: String,
    key: String,
    value: String,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    create_key(&connection, secret, &key_type, &key, &value, ttl_seconds)
}

#[tauri::command]
fn redis_save_key_value(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key: String,
    value: String,
) -> Result<(), String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    save_key_value(&connection, secret, &key, &value)
}

#[tauri::command]
fn redis_update_key_ttl(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key: String,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    update_key_ttl(&connection, secret, &key, ttl_seconds)
}

#[tauri::command]
fn redis_delete_key(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key: String,
) -> Result<(), String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    delete_key(&connection, secret, &key)
}

#[tauri::command]
fn redis_bulk_delete_keys(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    keys: Vec<String>,
    strategy: Option<String>,
) -> Result<RedisBulkDeleteResult, String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    bulk_delete_keys(&connection, secret, &keys, strategy.as_deref())
}

#[tauri::command]
fn load_redis_stream(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key: String,
    cursor: Option<String>,
    page_size: Option<usize>,
    filter: Option<String>,
) -> Result<RedisStreamData, String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    load_stream_data(
        &connection,
        secret,
        &key,
        cursor,
        page_size.unwrap_or(25),
        filter.unwrap_or_default(),
    )
}

#[tauri::command]
fn redis_stream_add_entry(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key: String,
    value: String,
) -> Result<(), String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    upsert_stream_entry(&connection, secret, &key, &value)
}

#[tauri::command]
fn redis_stream_delete_entry(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    key: String,
    entry_id: String,
) -> Result<(), String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    delete_stream_entry(&connection, secret, &key, &entry_id)
}

#[tauri::command]
fn redis_execute_cli(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
    command: String,
    response_mode: Option<String>,
) -> Result<RedisCliResponse, String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    execute_cli(&connection, secret, &command, response_mode.as_deref())
}

#[tauri::command]
fn redis_start_monitor(
    app: tauri::AppHandle,
    id: String,
    database: Option<u32>,
) -> Result<RedisMonitorSession, String> {
    let store = Store::new(&app)?;
    let connection = with_database(store.get_connection(&id)?, database);
    let secret = store.get_secret(&id)?;
    start_monitor_session(&connection, secret)
}

#[tauri::command]
fn redis_poll_monitor(
    app: tauri::AppHandle,
    session_id: String,
) -> Result<RedisMonitorSnapshot, String> {
    let _ = app;
    update_monitor_session(&session_id)
}

#[tauri::command]
fn redis_stop_monitor(session_id: String) -> Result<(), String> {
    stop_monitor_session(&session_id)
}

#[tauri::command]
fn list_redis_helper_entries() -> Result<Vec<RedisHelperEntry>, String> {
    Ok(list_helper_entries())
}

#[tauri::command]
fn tdengine_load_catalog(
    app: tauri::AppHandle,
    id: String,
    database: Option<String>,
    supertable: Option<String>,
) -> Result<Vec<models::ResourceNode>, String> {
    let store = Store::new(&app)?;
    let connection = store.get_connection(&id)?;
    let secret = store.get_secret(&id)?;
    load_tdengine_catalog(&connection, secret, database, supertable)
}

#[tauri::command]
fn tdengine_load_object_detail(
    app: tauri::AppHandle,
    id: String,
    database: String,
    object_name: String,
    object_kind: String,
) -> Result<TdengineObjectDetail, String> {
    let store = Store::new(&app)?;
    let connection = store.get_connection(&id)?;
    let secret = store.get_secret(&id)?;
    load_tdengine_object_detail(&connection, secret, &database, &object_name, &object_kind)
}

#[tauri::command]
fn tdengine_execute_query(
    app: tauri::AppHandle,
    id: String,
    database: String,
    sql: String,
    max_rows: Option<usize>,
) -> Result<TdengineQueryResult, String> {
    let store = Store::new(&app)?;
    let connection = store.get_connection(&id)?;
    let secret = store.get_secret(&id)?;
    execute_tdengine_query(
        &connection,
        secret,
        &database,
        &sql,
        max_rows.unwrap_or(1000),
    )
}

#[tauri::command]
fn tdengine_health_check(
    app: tauri::AppHandle,
    id: String,
) -> Result<ConnectionHealth, String> {
    let store = Store::new(&app)?;
    let connection = store.get_connection(&id)?;
    let secret = store.get_secret(&id)?;
    let health = run_tdengine_health_check(&connection, secret);
    store.update_last_checked(&id, &health.checked_at)?;
    Ok(health)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            list_connections,
            save_connection,
            delete_connection,
            toggle_favorite,
            touch_connection,
            health_check,
            get_workspace_snapshot,
            load_redis_browser,
            redis_set_string_value,
            redis_create_key,
            redis_save_key_value,
            redis_update_key_ttl,
            redis_delete_key,
            redis_bulk_delete_keys,
            load_redis_stream,
            redis_stream_add_entry,
            redis_stream_delete_entry,
            redis_execute_cli,
            redis_start_monitor,
            redis_poll_monitor,
            redis_stop_monitor,
            list_redis_helper_entries,
            tdengine_load_catalog,
            tdengine_load_object_detail,
            tdengine_execute_query,
            tdengine_health_check
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
