use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};
use encoding_rs::GBK;
use redis::{Client, Connection, Value as RedisValue};
use serde_json::{Map as JsonMap, Value as JsonValue};
use url::Url;
use uuid::Uuid;

use crate::models::{
    ConnectionRecord, RedisBrowser, RedisBulkActionState, RedisBulkDeleteResult, RedisCliResponse,
    RedisCliRow, RedisHelperEntry, RedisInfoRow, RedisKeyDetail, RedisMonitorCommand,
    RedisMonitorSession, RedisMonitorSnapshot, RedisSlowlogEntry, RedisStreamConsumer,
    RedisStreamData, RedisStreamEntry, RedisStreamGroup, RedisValueViewMode, ResourceNode,
    WorkspaceMetric,
};

struct RedisKeySummary {
    key: String,
    key_type: String,
    ttl_seconds: Option<i64>,
    size: Option<usize>,
}

struct ParsedStreamEntry {
    id: String,
    fields: Vec<(String, String)>,
}

struct MonitorState {
    connection: ConnectionRecord,
    secret: Option<String>,
    started_at: String,
    last_slowlog_id: Option<u64>,
}

struct ParsedCliCommand {
    name: String,
    args: Vec<String>,
    is_write: bool,
    is_blocking: bool,
    requires_confirmation: bool,
}

static MONITOR_SESSIONS: OnceLock<Mutex<HashMap<String, MonitorState>>> = OnceLock::new();

fn monitor_sessions() -> &'static Mutex<HashMap<String, MonitorState>> {
    MONITOR_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

const VALUE_SAMPLE_LIMIT: usize = 50;
const STREAM_PREVIEW_LIMIT: usize = 25;
const STRING_PREVIEW_LIMIT: usize = 16 * 1024;

fn normalize_search_mode(value: Option<&str>) -> String {
    match value
        .unwrap_or("pattern")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "fuzzy" => "fuzzy".into(),
        _ => "pattern".into(),
    }
}

fn normalize_type_filter(value: Option<&str>) -> Option<String> {
    let normalized = value.unwrap_or("all").trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "all" => None,
        "string" | "hash" | "list" | "set" | "zset" | "stream" | "json" => Some(normalized),
        _ => None,
    }
}

fn normalize_selected_key_ids(ids: &[String]) -> Vec<String> {
    let mut ids = ids
        .iter()
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

fn build_bulk_action_state(connection: &ConnectionRecord) -> RedisBulkActionState {
    RedisBulkActionState {
        can_delete: !connection.readonly,
        preferred_strategy: "unlink".into(),
        requires_confirmation: true,
    }
}

fn value_view_modes() -> Vec<RedisValueViewMode> {
    vec![
        RedisValueViewMode {
            id: "auto".into(),
            label: "Auto".into(),
        },
        RedisValueViewMode {
            id: "utf8".into(),
            label: "UTF-8".into(),
        },
        RedisValueViewMode {
            id: "gbk".into(),
            label: "GBK".into(),
        },
        RedisValueViewMode {
            id: "json".into(),
            label: "JSON".into(),
        },
        RedisValueViewMode {
            id: "hex".into(),
            label: "HEX".into(),
        },
        RedisValueViewMode {
            id: "ascii".into(),
            label: "ASCII".into(),
        },
        RedisValueViewMode {
            id: "base64".into(),
            label: "Base64".into(),
        },
        RedisValueViewMode {
            id: "raw".into(),
            label: "Raw".into(),
        },
    ]
}

fn fuzzy_scan_budget(limit: usize) -> usize {
    (limit.max(1) * 12).max(400)
}

fn fuzzy_match(key: &str, lowered_query: &str) -> bool {
    if lowered_query.is_empty() {
        return true;
    }

    key.to_ascii_lowercase().contains(lowered_query)
}

pub fn load_browser(
    connection: &ConnectionRecord,
    secret: Option<String>,
    pattern: String,
    limit: usize,
    selected_key: Option<String>,
    search_mode: Option<String>,
    type_filter: Option<String>,
    selected_key_ids: Vec<String>,
) -> Result<RedisBrowser, String> {
    let mut client = open_connection(connection, secret)?;
    let info = read_info(&mut client);
    let db_size: u64 = redis::cmd("DBSIZE").query(&mut client).unwrap_or(0);
    let config_rows = build_config_rows(&mut client);
    let slowlog_entries = read_slowlog_entries(&mut client, 10);
    let search_mode = normalize_search_mode(search_mode.as_deref());
    let type_filter = normalize_type_filter(type_filter.as_deref());
    let (mut keys, has_more, search_partial) =
        collect_keys(
            &mut client,
            &pattern,
            limit.max(1),
            &search_mode,
            type_filter.as_deref(),
        )?;
    let selected_key_name = resolve_selected_key_name(selected_key, &keys);
    let selected_detail = match selected_key_name {
        Some(ref key) => load_key_detail(&mut client, key).ok(),
        None => None,
    };
    inject_selected_key_summary(
        &mut client,
        &mut keys,
        limit.max(1),
        selected_detail.as_ref(),
    )?;

    Ok(RedisBrowser {
        connection_id: connection.id.clone(),
        pattern,
        search_mode: search_mode.clone(),
        search_partial,
        limit,
        loaded_count: keys.len(),
        has_more,
        metrics: build_metrics(connection.readonly, db_size, &keys, &search_mode),
        resources: build_resources(connection, &keys),
        selected_key: selected_detail,
        selected_key_ids: normalize_selected_key_ids(&selected_key_ids),
        bulk_action_state: build_bulk_action_state(connection),
        value_view_modes: value_view_modes(),
        diagnostics: build_diagnostics(
            connection,
            &keys,
            has_more,
            search_partial,
            &search_mode,
            &slowlog_entries,
        ),
        info_rows: build_info_rows(connection, db_size, &info),
        server_rows: build_server_rows(&info),
        config_rows,
        slowlog_entries,
    })
}

fn resolve_selected_key_name(
    selected_key: Option<String>,
    keys: &[RedisKeySummary],
) -> Option<String> {
    let explicit_clear = selected_key
        .as_ref()
        .map(|key| key.trim().is_empty())
        .unwrap_or(false);

    if explicit_clear {
        return None;
    }

    selected_key
        .filter(|key| !key.trim().is_empty())
        .or_else(|| keys.first().map(|item| item.key.clone()))
}

fn inject_selected_key_summary(
    connection: &mut Connection,
    keys: &mut Vec<RedisKeySummary>,
    limit: usize,
    selected_detail: Option<&RedisKeyDetail>,
) -> Result<(), String> {
    let Some(selected_detail) = selected_detail else {
        return Ok(());
    };

    if keys.iter().any(|item| item.key == selected_detail.key) {
        return Ok(());
    }

    let summary = load_key_summary(connection, &selected_detail.key)?;
    if keys.len() >= limit {
        keys.pop();
    }
    keys.insert(0, summary);
    Ok(())
}

pub fn set_string_value(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key: &str,
    value: &str,
) -> Result<(), String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret)?;
    set_string_value_on_connection(&mut client, key, value)
}

fn set_string_value_on_connection(
    connection: &mut Connection,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let key_type = read_key_type(connection, key)?;
    if key_type != "string" {
        return Err("Only string keys can be edited in this version.".into());
    }

    let ttl_before = read_ttl(connection, key)?;
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .query::<()>(connection)
        .map_err(|error| error.to_string())?;

    if let Some(ttl_seconds) = ttl_before.filter(|ttl| *ttl > 0) {
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(ttl_seconds)
            .query::<bool>(connection)
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

pub fn create_key(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key_type: &str,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    ensure_writable(connection)?;
    validate_create_request(key, ttl_seconds)?;

    let mut client = open_connection(connection, secret)?;
    ensure_key_absent(&mut client, key)?;

    match key_type.trim().to_ascii_lowercase().as_str() {
        "string" => create_string_key_on_connection(&mut client, key, value, ttl_seconds),
        "hash" => create_hash_key(&mut client, key, value, ttl_seconds),
        "list" => create_list_key(&mut client, key, value, ttl_seconds),
        "set" => create_set_key(&mut client, key, value, ttl_seconds),
        "zset" => create_zset_key(&mut client, key, value, ttl_seconds),
        _ => Err("Unsupported Redis key type.".into()),
    }
}

fn validate_create_request(key: &str, ttl_seconds: Option<i64>) -> Result<(), String> {
    if key.trim().is_empty() {
        return Err("Key name is required.".into());
    }

    if matches!(ttl_seconds, Some(ttl) if ttl <= 0) {
        return Err("TTL must be a positive number of seconds.".into());
    }

    Ok(())
}

fn ensure_key_absent(connection: &mut Connection, key: &str) -> Result<(), String> {
    let exists: u64 = redis::cmd("EXISTS")
        .arg(key)
        .query(connection)
        .map_err(|error| error.to_string())?;

    if exists > 0 {
        return Err("Key already exists.".into());
    }

    Ok(())
}

fn create_string_key_on_connection(
    connection: &mut Connection,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    let mut command = redis::cmd("SET");
    command.arg(key).arg(value);
    if let Some(ttl_seconds) = ttl_seconds {
        command.arg("EX").arg(ttl_seconds);
    }
    command.arg("NX");

    let created: Option<String> = command
        .query(connection)
        .map_err(|error| error.to_string())?;
    if created.is_none() {
        return Err("Key already exists.".into());
    }

    Ok(())
}

fn create_hash_key(
    connection: &mut Connection,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    let entries = parse_hash_entries(value)?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().cmd("HSET").arg(key);
    for (field, field_value) in entries {
        pipeline.arg(field).arg(field_value);
    }
    append_expire(&mut pipeline, key, ttl_seconds);
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn create_list_key(
    connection: &mut Connection,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    let elements = parse_string_array(value, "List editor expects a JSON array.")?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().cmd("RPUSH").arg(key);
    for element in elements {
        pipeline.arg(element);
    }
    append_expire(&mut pipeline, key, ttl_seconds);
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn create_set_key(
    connection: &mut Connection,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    let members = parse_string_array(value, "Set editor expects a JSON array.")?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().cmd("SADD").arg(key);
    for member in members {
        pipeline.arg(member);
    }
    append_expire(&mut pipeline, key, ttl_seconds);
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn create_zset_key(
    connection: &mut Connection,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    let members = parse_sorted_set_members(value)?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().cmd("ZADD").arg(key);
    for (member, score) in members {
        pipeline.arg(score).arg(member);
    }
    append_expire(&mut pipeline, key, ttl_seconds);
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn append_expire(pipeline: &mut redis::Pipeline, key: &str, ttl_seconds: Option<i64>) {
    if let Some(ttl_seconds) = ttl_seconds {
        pipeline.cmd("EXPIRE").arg(key).arg(ttl_seconds);
    }
}

pub fn save_key_value(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key: &str,
    value: &str,
) -> Result<(), String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret)?;
    let key_type = read_key_type(&mut client, key)?;

    match key_type.as_str() {
        "string" => set_string_value_on_connection(&mut client, key, value),
        "hash" => replace_hash_entries(&mut client, key, value),
        "list" => replace_list_elements(&mut client, key, value),
        "set" => replace_set_members(&mut client, key, value),
        "zset" => replace_sorted_set_members(&mut client, key, value),
        "stream" => Err("Stream editing is not supported in this version.".into()),
        _ => Err("This Redis type is not editable in this version.".into()),
    }
}

fn replace_hash_entries(connection: &mut Connection, key: &str, value: &str) -> Result<(), String> {
    let entries = parse_hash_entries(value)?;
    let ttl_before = read_ttl(connection, key)?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().del(key).cmd("HSET").arg(key);
    for (field, field_value) in entries {
        pipeline.arg(field).arg(field_value);
    }
    if let Some(ttl_seconds) = ttl_before.filter(|ttl| *ttl > 0) {
        pipeline.cmd("EXPIRE").arg(key).arg(ttl_seconds);
    }
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn replace_list_elements(
    connection: &mut Connection,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let elements = parse_string_array(value, "List editor expects a JSON array.")?;
    let ttl_before = read_ttl(connection, key)?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().del(key).cmd("RPUSH").arg(key);
    for element in elements {
        pipeline.arg(element);
    }
    if let Some(ttl_seconds) = ttl_before.filter(|ttl| *ttl > 0) {
        pipeline.cmd("EXPIRE").arg(key).arg(ttl_seconds);
    }
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn replace_set_members(connection: &mut Connection, key: &str, value: &str) -> Result<(), String> {
    let members = parse_string_array(value, "Set editor expects a JSON array.")?;
    let ttl_before = read_ttl(connection, key)?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().del(key).cmd("SADD").arg(key);
    for member in members {
        pipeline.arg(member);
    }
    if let Some(ttl_seconds) = ttl_before.filter(|ttl| *ttl > 0) {
        pipeline.cmd("EXPIRE").arg(key).arg(ttl_seconds);
    }
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn replace_sorted_set_members(
    connection: &mut Connection,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let members = parse_sorted_set_members(value)?;
    let ttl_before = read_ttl(connection, key)?;
    let mut pipeline = redis::pipe();
    pipeline.atomic().del(key).cmd("ZADD").arg(key);
    for (member, score) in members {
        pipeline.arg(score).arg(member);
    }
    if let Some(ttl_seconds) = ttl_before.filter(|ttl| *ttl > 0) {
        pipeline.cmd("EXPIRE").arg(key).arg(ttl_seconds);
    }
    pipeline
        .query::<()>(connection)
        .map_err(|error| error.to_string())
}

fn parse_hash_entries(value: &str) -> Result<Vec<(String, String)>, String> {
    let parsed = serde_json::from_str::<JsonValue>(value)
        .map_err(|_| "Hash editor expects a JSON object.".to_string())?;

    let JsonValue::Object(entries) = parsed else {
        return Err("Hash editor expects a JSON object.".into());
    };

    if entries.is_empty() {
        return Err("Hash editor cannot save an empty object. Use Delete Key instead.".into());
    }

    Ok(entries
        .into_iter()
        .map(|(field, field_value)| (field, json_value_to_redis_string(field_value)))
        .collect())
}

fn parse_string_array(value: &str, invalid_message: &str) -> Result<Vec<String>, String> {
    let parsed =
        serde_json::from_str::<JsonValue>(value).map_err(|_| invalid_message.to_string())?;
    let JsonValue::Array(items) = parsed else {
        return Err(invalid_message.into());
    };

    if items.is_empty() {
        return Err("This editor cannot save an empty collection. Use Delete Key instead.".into());
    }

    Ok(items.into_iter().map(json_value_to_redis_string).collect())
}

fn parse_sorted_set_members(value: &str) -> Result<Vec<(String, f64)>, String> {
    let parsed = serde_json::from_str::<JsonValue>(value)
        .map_err(|_| "ZSet editor expects a JSON array of { member, score }.".to_string())?;
    let JsonValue::Array(items) = parsed else {
        return Err("ZSet editor expects a JSON array of { member, score }.".into());
    };

    if items.is_empty() {
        return Err("ZSet editor cannot save an empty collection. Use Delete Key instead.".into());
    }

    items
        .into_iter()
        .map(|item| {
            let JsonValue::Object(entry) = item else {
                return Err("Each ZSet row must be an object with member and score.".into());
            };

            let member = entry
                .get("member")
                .cloned()
                .ok_or_else(|| "Each ZSet row requires a member.".to_string())
                .map(json_value_to_redis_string)?;
            let score = entry
                .get("score")
                .and_then(json_value_to_f64)
                .ok_or_else(|| "Each ZSet row requires a numeric score.".to_string())?;
            Ok((member, score))
        })
        .collect()
}

fn json_value_to_redis_string(value: JsonValue) -> String {
    match value {
        JsonValue::String(text) => text,
        other => serde_json::to_string(&other).unwrap_or_default(),
    }
}

fn json_value_to_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(number) => number.as_f64(),
        JsonValue::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

pub fn update_key_ttl(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret)?;

    if let Some(ttl_seconds) = ttl_seconds {
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(ttl_seconds)
            .query::<bool>(&mut client)
            .map_err(|error| error.to_string())?;
    } else {
        redis::cmd("PERSIST")
            .arg(key)
            .query::<bool>(&mut client)
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

pub fn delete_key(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key: &str,
) -> Result<(), String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret)?;
    redis::cmd("DEL")
        .arg(key)
        .query::<u64>(&mut client)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn bulk_delete_keys(
    connection: &ConnectionRecord,
    secret: Option<String>,
    keys: &[String],
    strategy: Option<&str>,
) -> Result<RedisBulkDeleteResult, String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret)?;
    let requested_count = keys.len();
    let started = Instant::now();
    let mut strategy = normalize_delete_strategy(strategy);
    let mut deleted_count = 0_usize;
    let mut failed_keys = Vec::new();

    for key in keys {
        match delete_key_with_strategy(&mut client, key, &strategy) {
            Ok(value) => deleted_count += value as usize,
            Err(error) if strategy == "unlink" && is_unknown_command(&error) => {
                strategy = "del".into();
                match delete_key_with_strategy(&mut client, key, &strategy) {
                    Ok(value) => deleted_count += value as usize,
                    Err(_) => failed_keys.push(key.clone()),
                }
            }
            Err(_) => failed_keys.push(key.clone()),
        }
    }

    Ok(RedisBulkDeleteResult {
        requested_count,
        deleted_count,
        failed_keys,
        strategy,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

pub fn load_stream_data(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key: &str,
    cursor: Option<String>,
    page_size: usize,
    filter: String,
) -> Result<RedisStreamData, String> {
    let mut client = open_connection(connection, secret)?;
    if read_key_type(&mut client, key)? != "stream" {
        return Err("Selected key is not a Redis stream.".into());
    }

    let mut command = redis::cmd("XREVRANGE");
    command.arg(key);
    if let Some(cursor) = cursor.as_ref().filter(|value| !value.trim().is_empty()) {
        command.arg(format!("({cursor}"));
    } else {
        command.arg("+");
    }
    command.arg("-");
    command.arg("COUNT").arg(page_size.max(1));

    let raw_value: RedisValue = command
        .query(&mut client)
        .map_err(|error| error.to_string())?;
    let mut entries = parse_stream_entries(&raw_value);
    if !filter.trim().is_empty() {
        entries = filter_stream_entries(entries, &filter);
    }
    let groups = read_stream_groups(&mut client, key);

    Ok(RedisStreamData {
        key: key.to_string(),
        cursor: entries.last().map(|entry| entry.id.clone()),
        page_size: page_size.max(1),
        filter: filter.clone(),
        entries: entries
            .iter()
            .map(|entry| RedisStreamEntry {
                id: entry.id.clone(),
                fields: entry
                    .fields
                    .iter()
                    .map(|(field, value)| RedisInfoRow {
                        label: field.clone(),
                        value: value.clone(),
                        secondary: None,
                    })
                    .collect(),
                summary: truncate_display(&summarize_pairs(&entry.fields, 3), 140),
            })
            .collect(),
        groups: groups.clone(),
        diagnostics: build_stream_diagnostics(connection, key, &entries, &groups, &filter),
        can_write: !connection.readonly,
    })
}

pub fn upsert_stream_entry(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key: &str,
    value: &str,
) -> Result<(), String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret)?;
    let entries = parse_hash_entries(value)?;
    let mut command = redis::cmd("XADD");
    command.arg(key).arg("*");
    for (field, field_value) in entries {
        command.arg(field).arg(field_value);
    }
    command
        .query::<String>(&mut client)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn delete_stream_entry(
    connection: &ConnectionRecord,
    secret: Option<String>,
    key: &str,
    entry_id: &str,
) -> Result<(), String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret)?;
    redis::cmd("XDEL")
        .arg(key)
        .arg(entry_id)
        .query::<u64>(&mut client)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn execute_cli(
    connection: &ConnectionRecord,
    secret: Option<String>,
    command: &str,
    response_mode: Option<&str>,
) -> Result<RedisCliResponse, String> {
    let parsed = parse_cli_command(command)?;
    if parsed.is_blocking {
        return Err(
            "Blocking and subscription commands are not supported in the inline CLI.".into(),
        );
    }

    if parsed.is_write {
        ensure_writable(connection)?;
    }

    let started = Instant::now();
    let mut client = open_connection(connection, secret)?;
    let mut redis_command = redis::cmd(&parsed.name);
    for arg in &parsed.args {
        redis_command.arg(arg);
    }
    let value: RedisValue = redis_command
        .query(&mut client)
        .map_err(|error| error.to_string())?;

    Ok(RedisCliResponse {
        command: command.trim().to_string(),
        response_mode: normalize_cli_response_mode(response_mode),
        raw: value_to_text(&value),
        json: serde_json::to_string_pretty(&redis_value_to_json(&value)).ok(),
        rows: redis_value_to_rows(&value),
        execution_ms: started.elapsed().as_millis() as u64,
        is_write: parsed.is_write,
        requires_confirmation: parsed.requires_confirmation,
    })
}

pub fn start_monitor_session(
    connection: &ConnectionRecord,
    secret: Option<String>,
) -> Result<RedisMonitorSession, String> {
    let mut client = open_connection(connection, secret.clone())?;
    let _ = redis::cmd("PING")
        .query::<String>(&mut client)
        .map_err(|error| error.to_string())?;

    let session_id = Uuid::new_v4().to_string();
    let started_at = Utc::now().to_rfc3339();
    monitor_sessions()
        .lock()
        .map_err(|_| "Monitor session store is unavailable.".to_string())?
        .insert(
            session_id.clone(),
            MonitorState {
                connection: connection.clone(),
                secret,
                started_at: started_at.clone(),
                last_slowlog_id: None,
            },
        );

    Ok(RedisMonitorSession {
        session_id,
        connection_id: connection.id.clone(),
        started_at,
    })
}

pub fn update_monitor_session(session_id: &str) -> Result<RedisMonitorSnapshot, String> {
    let (connection, secret, started_at, last_slowlog_id) = {
        let sessions = monitor_sessions()
            .lock()
            .map_err(|_| "Monitor session store is unavailable.".to_string())?;
        let session = sessions
            .get(session_id)
            .ok_or_else(|| "Monitor session not found.".to_string())?;
        (
            session.connection.clone(),
            session.secret.clone(),
            session.started_at.clone(),
            session.last_slowlog_id,
        )
    };

    let mut client = open_connection(&connection, secret)?;
    let info = read_info(&mut client);
    let slowlog_entries = read_slowlog_entries(&mut client, 20);
    let latest_seen = slowlog_entries
        .iter()
        .filter_map(|entry| entry.id.parse::<u64>().ok())
        .max();

    {
        let mut sessions = monitor_sessions()
            .lock()
            .map_err(|_| "Monitor session store is unavailable.".to_string())?;
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_slowlog_id = latest_seen.or(last_slowlog_id);
        }
    }

    Ok(RedisMonitorSnapshot {
        session_id: session_id.to_string(),
        running: true,
        polled_at: Utc::now().to_rfc3339(),
        metrics: build_monitor_metrics(&info),
        slowlog_entries: slowlog_entries.clone(),
        command_samples: build_monitor_commands(&slowlog_entries, last_slowlog_id, &started_at),
    })
}

pub fn stop_monitor_session(session_id: &str) -> Result<(), String> {
    let removed = monitor_sessions()
        .lock()
        .map_err(|_| "Monitor session store is unavailable.".to_string())?
        .remove(session_id);
    if removed.is_some() {
        Ok(())
    } else {
        Err("Monitor session not found.".into())
    }
}

pub fn list_helper_entries() -> Vec<RedisHelperEntry> {
    vec![
        helper_entry(
            "GET",
            "Read a string value.",
            "GET key",
            "GET session:2048",
            &["string"],
            "safe",
            &["SET", "TTL", "MGET"],
        ),
        helper_entry(
            "SET",
            "Write or replace a string value.",
            "SET key value [EX seconds|PX milliseconds] [NX|XX]",
            "SET session:2048 active EX 300",
            &["string"],
            "guarded",
            &["GET", "TTL", "EXPIRE"],
        ),
        helper_entry(
            "TTL",
            "Check how many seconds remain before a key expires.",
            "TTL key",
            "TTL session:2048",
            &["all"],
            "safe",
            &["EXPIRE", "TYPE", "GET"],
        ),
        helper_entry(
            "EXPIRE",
            "Set or update the TTL on an existing key.",
            "EXPIRE key seconds",
            "EXPIRE session:2048 600",
            &["all"],
            "guarded",
            &["TTL", "PERSIST", "SET"],
        ),
        helper_entry(
            "TYPE",
            "Return the Redis data type stored at a key.",
            "TYPE key",
            "TYPE profile:1001",
            &["all"],
            "safe",
            &["SCAN", "GET", "HGETALL"],
        ),
        helper_entry(
            "SCAN",
            "Iterate keys without blocking Redis like KEYS.",
            "SCAN cursor [MATCH pattern] [COUNT count]",
            "SCAN 0 MATCH session:* COUNT 200",
            &["all"],
            "safe",
            &["TYPE", "TTL", "UNLINK"],
        ),
        helper_entry(
            "HGETALL",
            "Read all fields and values from a hash.",
            "HGETALL key",
            "HGETALL user:42",
            &["hash"],
            "safe",
            &["HGET", "HSET", "TYPE"],
        ),
        helper_entry(
            "LRANGE",
            "Read a slice of items from a list.",
            "LRANGE key start stop",
            "LRANGE jobs:pending 0 49",
            &["list"],
            "safe",
            &["LLEN", "LPUSH", "RPUSH"],
        ),
        helper_entry(
            "SMEMBERS",
            "Read all members from a set.",
            "SMEMBERS key",
            "SMEMBERS feature:flags",
            &["set"],
            "safe",
            &["SADD", "SREM", "SCARD"],
        ),
        helper_entry(
            "ZRANGE",
            "Read members from a sorted set by rank.",
            "ZRANGE key start stop [WITHSCORES]",
            "ZRANGE leaderboard 0 19 WITHSCORES",
            &["zset"],
            "safe",
            &["ZADD", "ZREVRANGE", "ZSCORE"],
        ),
        helper_entry(
            "UNLINK",
            "Delete keys asynchronously when supported.",
            "UNLINK key [key ...]",
            "UNLINK cache:feed cache:flags",
            &["all"],
            "danger",
            &["DEL", "EXPIRE"],
        ),
        helper_entry(
            "DEL",
            "Delete keys immediately in the foreground thread.",
            "DEL key [key ...]",
            "DEL temp:job:1 temp:job:2",
            &["all"],
            "danger",
            &["UNLINK", "EXPIRE", "TTL"],
        ),
        helper_entry(
            "XADD",
            "Append a message to a Redis stream.",
            "XADD key * field value [field value ...]",
            "XADD orders.stream * orderId ord_1001 status created",
            &["stream"],
            "guarded",
            &["XRANGE", "XINFO GROUPS", "XDEL"],
        ),
        helper_entry(
            "XRANGE",
            "Read stream entries between two IDs.",
            "XRANGE key start end [COUNT count]",
            "XRANGE orders.stream - + COUNT 50",
            &["stream"],
            "safe",
            &["XADD", "XREVRANGE", "XINFO GROUPS"],
        ),
        helper_entry(
            "XINFO GROUPS",
            "Inspect stream consumer groups and pending counts.",
            "XINFO GROUPS key",
            "XINFO GROUPS orders.stream",
            &["stream"],
            "safe",
            &["XINFO CONSUMERS", "XPENDING"],
        ),
        helper_entry(
            "INFO",
            "Read server metrics and section summaries.",
            "INFO [section]",
            "INFO memory",
            &["server"],
            "safe",
            &["SLOWLOG GET", "SCAN", "TYPE"],
        ),
        helper_entry(
            "SLOWLOG GET",
            "Read recent slow commands captured by Redis.",
            "SLOWLOG GET [count]",
            "SLOWLOG GET 32",
            &["server"],
            "safe",
            &["INFO", "MONITOR", "LATENCY LATEST"],
        ),
    ]
}

fn helper_entry(
    command: &str,
    summary: &str,
    syntax: &str,
    example: &str,
    applicable_types: &[&str],
    risk_level: &str,
    related_commands: &[&str],
) -> RedisHelperEntry {
    RedisHelperEntry {
        command: command.to_string(),
        summary: summary.to_string(),
        syntax: syntax.to_string(),
        example: example.to_string(),
        applicable_types: applicable_types
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        risk_level: risk_level.to_string(),
        related_commands: related_commands
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
    }
}

fn normalize_delete_strategy(value: Option<&str>) -> String {
    match value
        .unwrap_or("unlink")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "del" => "del".into(),
        _ => "unlink".into(),
    }
}

fn normalize_cli_response_mode(value: Option<&str>) -> String {
    match value
        .unwrap_or("table")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "json" => "json".into(),
        "raw" => "raw".into(),
        _ => "table".into(),
    }
}

fn parse_cli_command(command: &str) -> Result<ParsedCliCommand, String> {
    let parts = shlex::split(command).ok_or_else(|| "Command parsing failed.".to_string())?;
    if parts.is_empty() {
        return Err("Command is required.".into());
    }

    let name = parts[0].to_ascii_uppercase();
    Ok(ParsedCliCommand {
        args: parts.into_iter().skip(1).collect(),
        is_write: is_write_command(&name),
        is_blocking: is_blocking_command(&name),
        requires_confirmation: requires_confirmation_command(&name),
        name,
    })
}

fn is_write_command(name: &str) -> bool {
    matches!(
        name,
        "SET"
            | "MSET"
            | "DEL"
            | "UNLINK"
            | "EXPIRE"
            | "PERSIST"
            | "HSET"
            | "HDEL"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "SADD"
            | "SREM"
            | "ZADD"
            | "ZREM"
            | "XADD"
            | "XDEL"
            | "FLUSHDB"
            | "FLUSHALL"
            | "CONFIG"
    )
}

fn is_blocking_command(name: &str) -> bool {
    matches!(
        name,
        "MONITOR" | "SUBSCRIBE" | "PSUBSCRIBE" | "BLPOP" | "BRPOP"
    )
}

fn requires_confirmation_command(name: &str) -> bool {
    matches!(
        name,
        "DEL" | "UNLINK" | "FLUSHDB" | "FLUSHALL" | "CONFIG" | "SHUTDOWN"
    )
}

fn delete_key_with_strategy(
    connection: &mut Connection,
    key: &str,
    strategy: &str,
) -> Result<u64, String> {
    let command_name = if strategy == "del" { "DEL" } else { "UNLINK" };
    redis::cmd(command_name)
        .arg(key)
        .query::<u64>(connection)
        .map_err(|error| error.to_string())
}

fn is_unknown_command(error: &str) -> bool {
    error.to_ascii_lowercase().contains("unknown command")
}

fn ensure_writable(connection: &ConnectionRecord) -> Result<(), String> {
    if connection.readonly {
        return Err("This connection is read-only.".into());
    }

    if connection.ssh_enabled {
        return Err("Redis over SSH tunnel is not implemented in this slice.".into());
    }

    Ok(())
}

fn open_connection(
    connection: &ConnectionRecord,
    secret: Option<String>,
) -> Result<Connection, String> {
    if connection.kind != "redis" {
        return Err("This command only supports Redis connections.".into());
    }

    if connection.ssh_enabled {
        return Err("Redis over SSH tunnel is not implemented in this slice.".into());
    }

    let mut url = Url::parse(if connection.use_tls {
        "rediss://localhost/"
    } else {
        "redis://localhost/"
    })
    .map_err(|error| error.to_string())?;

    url.set_host(Some(connection.host.as_str()))
        .map_err(|_| "Invalid Redis host.".to_string())?;
    url.set_port(Some(connection.port))
        .map_err(|_| "Invalid Redis port.".to_string())?;

    let database_index = connection.database_name.parse::<u8>().unwrap_or(0);
    url.set_path(&format!("/{database_index}"));

    if let Some(secret) = secret.filter(|value| !value.trim().is_empty()) {
        url.set_username(connection.username.trim())
            .map_err(|_| "Invalid Redis username.".to_string())?;
        url.set_password(Some(secret.as_str()))
            .map_err(|_| "Invalid Redis password.".to_string())?;
    }

    if connection.use_tls && !connection.tls_verify {
        url.set_fragment(Some("insecure"));
    }

    let client = Client::open(url.as_str()).map_err(|error| error.to_string())?;
    client.get_connection().map_err(|error| error.to_string())
}

fn collect_keys(
    connection: &mut Connection,
    pattern: &str,
    limit: usize,
    search_mode: &str,
    type_filter: Option<&str>,
) -> Result<(Vec<RedisKeySummary>, bool, bool), String> {
    let query = pattern;
    let mut cursor = 0_u64;
    let mut summaries: Vec<RedisKeySummary> = Vec::new();
    let mut has_more = false;
    let mut search_partial = false;
    let normalized_mode = normalize_search_mode(Some(search_mode));
    let lowered_query = query.trim().to_ascii_lowercase();
    let mut scanned = 0_usize;
    let scan_budget = fuzzy_scan_budget(limit);

    loop {
        let mut command = redis::cmd("SCAN");
        command.arg(cursor);
        if normalized_mode == "pattern" && !query.trim().is_empty() {
            command.arg("MATCH").arg(query.trim());
        }
        command.arg("COUNT").arg(100);

        let (next_cursor, batch): (u64, Vec<String>) = command
            .query(connection)
            .map_err(|error| error.to_string())?;
        cursor = next_cursor;

        for key in batch {
            scanned += 1;
            if normalized_mode == "fuzzy" && !fuzzy_match(&key, &lowered_query) {
                if scanned >= scan_budget && cursor != 0 {
                    search_partial = true;
                    break;
                }
                continue;
            }

            let summary = load_key_summary(connection, &key)?;
            if type_filter
                .map(|filter| filter != summary.key_type)
                .unwrap_or(false)
            {
                if scanned >= scan_budget && cursor != 0 && normalized_mode == "fuzzy" {
                    search_partial = true;
                    break;
                }
                continue;
            }

            summaries.push(summary);
            if summaries.len() >= limit {
                break;
            }
        }

        if summaries.len() >= limit {
            has_more = cursor != 0;
            break;
        }

        if search_partial {
            has_more = cursor != 0;
            break;
        }

        if cursor == 0 {
            break;
        }
    }

    summaries.truncate(limit);
    Ok((summaries, has_more, search_partial))
}

fn load_key_summary(connection: &mut Connection, key: &str) -> Result<RedisKeySummary, String> {
    Ok(RedisKeySummary {
        key: key.to_string(),
        key_type: read_key_type(connection, key)?,
        ttl_seconds: read_ttl(connection, key)?,
        size: read_size(connection, key)?,
    })
}

fn load_key_detail(connection: &mut Connection, key: &str) -> Result<RedisKeyDetail, String> {
    let key_type = read_key_type(connection, key)?;
    let ttl_seconds = read_ttl(connection, key)?;
    let size = read_size(connection, key)?;
    let encoding = query_optional::<String>(
        connection,
        "OBJECT",
        &[String::from("ENCODING"), key.to_string()],
    );

    match key_type.as_str() {
        "string" => {
            let value: RedisValue = redis::cmd("GET")
                .arg(key)
                .query(connection)
                .map_err(|error| error.to_string())?;
            let raw_bytes = value_to_bytes(&value).unwrap_or_default();
            let (preview, preview_language, truncated) = preview_bytes(&raw_bytes);
            Ok(RedisKeyDetail {
                key: key.to_string(),
                key_type,
                ttl_seconds,
                size,
                encoding,
                preview,
                preview_language,
                raw_value_base64: Some(BASE64_STANDARD.encode(raw_bytes)),
                truncated,
                rows: Vec::new(),
                editable: true,
            })
        }
        "hash" => {
            let values: Vec<String> = redis::cmd("HGETALL")
                .arg(key)
                .query(connection)
                .map_err(|error| error.to_string())?;
            let rows = pair_rows(&values);
            let preview_map = rows
                .iter()
                .map(|row| (row.label.clone(), JsonValue::String(row.value.clone())))
                .collect::<serde_json::Map<_, _>>();
            Ok(RedisKeyDetail {
                key: key.to_string(),
                key_type,
                ttl_seconds,
                size,
                encoding,
                preview: serde_json::to_string_pretty(&preview_map).unwrap_or_else(|_| "{}".into()),
                preview_language: "json".into(),
                raw_value_base64: Some(
                    BASE64_STANDARD.encode(serde_json::to_vec(&preview_map).unwrap_or_default()),
                ),
                truncated: size
                    .map(|value| value > VALUE_SAMPLE_LIMIT)
                    .unwrap_or(false),
                rows,
                editable: true,
            })
        }
        "list" => {
            let values: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg((VALUE_SAMPLE_LIMIT - 1) as i64)
                .query(connection)
                .map_err(|error| error.to_string())?;
            let rows = values
                .iter()
                .enumerate()
                .map(|(index, value)| RedisInfoRow {
                    label: index.to_string(),
                    value: value.clone(),
                    secondary: None,
                })
                .collect::<Vec<_>>();
            Ok(RedisKeyDetail {
                key: key.to_string(),
                key_type,
                ttl_seconds,
                size,
                encoding,
                preview: serde_json::to_string_pretty(&values).unwrap_or_else(|_| "[]".into()),
                preview_language: "json".into(),
                raw_value_base64: Some(
                    BASE64_STANDARD.encode(serde_json::to_vec(&values).unwrap_or_default()),
                ),
                truncated: size.map(|value| value > values.len()).unwrap_or(false),
                rows,
                editable: true,
            })
        }
        "set" => {
            let values: Vec<String> = redis::cmd("SMEMBERS")
                .arg(key)
                .query(connection)
                .map_err(|error| error.to_string())?;
            let values = values
                .into_iter()
                .take(VALUE_SAMPLE_LIMIT)
                .collect::<Vec<_>>();
            let rows = values
                .iter()
                .map(|value| RedisInfoRow {
                    label: "member".into(),
                    value: value.clone(),
                    secondary: None,
                })
                .collect::<Vec<_>>();
            Ok(RedisKeyDetail {
                key: key.to_string(),
                key_type,
                ttl_seconds,
                size,
                encoding,
                preview: serde_json::to_string_pretty(&values).unwrap_or_else(|_| "[]".into()),
                preview_language: "json".into(),
                raw_value_base64: Some(
                    BASE64_STANDARD.encode(serde_json::to_vec(&values).unwrap_or_default()),
                ),
                truncated: size.map(|value| value > values.len()).unwrap_or(false),
                rows,
                editable: true,
            })
        }
        "zset" => {
            let values: Vec<String> = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg((VALUE_SAMPLE_LIMIT - 1) as i64)
                .arg("WITHSCORES")
                .query(connection)
                .map_err(|error| error.to_string())?;
            let rows = pair_rows(&values);
            Ok(RedisKeyDetail {
                key: key.to_string(),
                key_type,
                ttl_seconds,
                size,
                encoding,
                preview: serde_json::to_string_pretty(
                    &rows
                        .iter()
                        .map(|row| serde_json::json!({ "member": row.label, "score": row.value }))
                        .collect::<Vec<_>>(),
                )
                .unwrap_or_else(|_| "[]".into()),
                preview_language: "json".into(),
                raw_value_base64: Some(BASE64_STANDARD.encode(
                    serde_json::to_vec(
                        &rows
                            .iter()
                            .map(|row| serde_json::json!({ "member": row.label, "score": row.value }))
                            .collect::<Vec<_>>(),
                    )
                    .unwrap_or_default(),
                )),
                truncated: size.map(|value| value > rows.len()).unwrap_or(false),
                rows,
                editable: true,
            })
        }
        "stream" => {
            let raw_value: RedisValue = redis::cmd("XRANGE")
                .arg(key)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(STREAM_PREVIEW_LIMIT)
                .query(connection)
                .map_err(|error| error.to_string())?;
            let stream_entries = parse_stream_entries(&raw_value);
            let preview = stream_preview(&stream_entries);
            Ok(RedisKeyDetail {
                key: key.to_string(),
                key_type,
                ttl_seconds,
                size,
                encoding,
                preview: preview.clone(),
                preview_language: "json".into(),
                raw_value_base64: Some(BASE64_STANDARD.encode(preview.as_bytes())),
                truncated: size
                    .map(|value| value > stream_entries.len())
                    .unwrap_or(false),
                rows: stream_rows(&stream_entries),
                editable: false,
            })
        }
        _ => Ok(RedisKeyDetail {
            key: key.to_string(),
            key_type,
            ttl_seconds,
            size,
            encoding,
            preview: "Unsupported Redis type preview.".into(),
            preview_language: "text".into(),
            raw_value_base64: None,
            truncated: false,
            rows: Vec::new(),
            editable: false,
        }),
    }
}

fn build_metrics(
    readonly: bool,
    db_size: u64,
    keys: &[RedisKeySummary],
    search_mode: &str,
) -> Vec<WorkspaceMetric> {
    let namespace_count = keys
        .iter()
        .map(|item| prefix_for_key(&item.key))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let ttl_count = keys
        .iter()
        .filter(|item| item.ttl_seconds.is_some())
        .count();

    vec![
        WorkspaceMetric {
            label: "Visible keys".into(),
            value: keys.len().to_string(),
            detail: format!("DB size {db_size}"),
            tone: "accent".into(),
        },
        WorkspaceMetric {
            label: "Namespaces".into(),
            value: namespace_count.to_string(),
            detail: format!("{} search", search_mode.to_ascii_uppercase()),
            tone: "neutral".into(),
        },
        WorkspaceMetric {
            label: "Write safety".into(),
            value: if readonly { "Locked" } else { "Guarded" }.into(),
            detail: if ttl_count > 0 {
                format!("{ttl_count} loaded keys have TTL")
            } else {
                "No TTL keys in current page".into()
            },
            tone: if readonly { "success" } else { "danger" }.into(),
        },
    ]
}

fn build_resources(connection: &ConnectionRecord, keys: &[RedisKeySummary]) -> Vec<ResourceNode> {
    let mut groups: BTreeMap<String, Vec<&RedisKeySummary>> = BTreeMap::new();
    for key in keys {
        groups
            .entry(prefix_for_key(&key.key))
            .or_default()
            .push(key);
    }

    let prefix_nodes = groups
        .into_iter()
        .map(|(prefix, group)| ResourceNode {
            id: format!("prefix:{prefix}"),
            label: prefix.clone(),
            kind: "prefix".into(),
            meta: Some(format!("{} keys", group.len())),
            children: Some(
                group
                    .into_iter()
                    .map(|item| ResourceNode {
                        id: format!("key:{}", item.key),
                        label: item.key.clone(),
                        kind: item.key_type.clone(),
                        meta: Some(key_meta(item)),
                        children: None,
                        expandable: None,
                    })
                    .collect(),
            ),
            expandable: None,
        })
        .collect::<Vec<_>>();

    vec![ResourceNode {
        id: format!("db:{}", connection.database_name),
        label: format!("db{}", connection.database_name),
        kind: "database".into(),
        meta: Some(format!("{} loaded", keys.len())),
        children: Some(prefix_nodes),
        expandable: None,
    }]
}

fn build_diagnostics(
    connection: &ConnectionRecord,
    keys: &[RedisKeySummary],
    has_more: bool,
    search_partial: bool,
    search_mode: &str,
    slowlog_entries: &[RedisSlowlogEntry],
) -> Vec<String> {
    let mut lines = vec![
        format!(
            "Loaded {} keys from Redis using {} search.",
            keys.len(),
            search_mode
        ),
        if has_more {
            "More keys are available. Use Load More to fetch a larger slice.".into()
        } else {
            "Current SCAN view reached the end of the keyspace.".into()
        },
    ];

    if search_partial {
        lines.push("Fuzzy search hit the scan budget before Redis finished scanning.".into());
    }

    if connection.readonly {
        lines.push(
            "This connection is read-only. Edit, TTL and delete actions stay disabled.".into(),
        );
    } else {
        lines.push("Delete and write actions remain guarded by explicit confirmation.".into());
    }

    if slowlog_entries.is_empty() {
        lines.push("Slowlog returned no recent slow commands.".into());
    } else {
        lines.push(format!(
            "Loaded {} recent slowlog entries for quick inspection.",
            slowlog_entries.len()
        ));
    }

    if connection.ssh_enabled {
        lines.push("SSH tunnel support is not implemented in this Redis slice.".into());
    }

    lines
}

fn build_stream_diagnostics(
    connection: &ConnectionRecord,
    key: &str,
    entries: &[ParsedStreamEntry],
    groups: &[RedisStreamGroup],
    filter: &str,
) -> Vec<String> {
    let mut lines = vec![format!("Loaded {} entries from {key}.", entries.len())];
    if !filter.trim().is_empty() {
        lines.push(format!("Applied local filter '{}'.", filter.trim()));
    }
    if groups.is_empty() {
        lines.push("No consumer groups were reported for this stream.".into());
    } else {
        lines.push(format!("Loaded {} consumer groups.", groups.len()));
    }
    if connection.readonly {
        lines.push("Read-only mode blocks XADD and XDEL.".into());
    }
    lines
}

fn build_info_rows(
    connection: &ConnectionRecord,
    db_size: u64,
    info: &HashMap<String, String>,
) -> Vec<RedisInfoRow> {
    vec![
        RedisInfoRow {
            label: "Redis version".into(),
            value: info
                .get("redis_version")
                .cloned()
                .unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Used memory".into(),
            value: info
                .get("used_memory_human")
                .or_else(|| info.get("used_memory"))
                .cloned()
                .unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Connected clients".into(),
            value: info
                .get("connected_clients")
                .cloned()
                .unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Database".into(),
            value: connection.database_name.clone(),
            secondary: Some(format!("{db_size} total keys")),
        },
    ]
}

fn build_server_rows(info: &HashMap<String, String>) -> Vec<RedisInfoRow> {
    let role = info
        .get("role")
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    let mode = if info.get("cluster_enabled").map(|value| value.as_str()) == Some("1") {
        "cluster"
    } else {
        "standalone"
    };
    let uptime = info
        .get("uptime_in_days")
        .map(|days| format!("{days} days"))
        .or_else(|| {
            info.get("uptime_in_seconds")
                .map(|seconds| format!("{seconds}s"))
        })
        .unwrap_or_else(|| "unknown".into());
    let ops_per_sec = info
        .get("instantaneous_ops_per_sec")
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    let hit_rate = hit_rate_label(info).unwrap_or_else(|| "unknown".into());

    vec![
        RedisInfoRow {
            label: "Role".into(),
            value: role,
            secondary: Some(format!("Mode {mode}")),
        },
        RedisInfoRow {
            label: "Uptime".into(),
            value: uptime,
            secondary: info.get("tcp_port").map(|port| format!("Port {port}")),
        },
        RedisInfoRow {
            label: "Ops/sec".into(),
            value: ops_per_sec,
            secondary: info
                .get("total_connections_received")
                .map(|value| format!("{value} total connections")),
        },
        RedisInfoRow {
            label: "Hit rate".into(),
            value: hit_rate,
            secondary: info
                .get("evicted_keys")
                .map(|value| format!("{value} evicted keys")),
        },
    ]
}

fn build_monitor_metrics(info: &HashMap<String, String>) -> Vec<RedisInfoRow> {
    let mut rows = build_server_rows(info);
    rows.extend([
        RedisInfoRow {
            label: "Memory".into(),
            value: info
                .get("used_memory_human")
                .or_else(|| info.get("used_memory"))
                .cloned()
                .unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Clients".into(),
            value: info
                .get("connected_clients")
                .cloned()
                .unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
    ]);
    rows
}

fn build_monitor_commands(
    slowlog_entries: &[RedisSlowlogEntry],
    last_slowlog_id: Option<u64>,
    started_at: &str,
) -> Vec<RedisMonitorCommand> {
    let mut commands = slowlog_entries
        .iter()
        .filter(|entry| {
            last_slowlog_id
                .and_then(|seen| entry.id.parse::<u64>().ok().map(|value| value > seen))
                .unwrap_or(true)
        })
        .map(|entry| RedisMonitorCommand {
            at: entry.started_at.clone(),
            command: entry.command.clone(),
            client: entry
                .client_address
                .clone()
                .or_else(|| entry.client_name.clone()),
        })
        .collect::<Vec<_>>();

    if commands.is_empty() {
        commands.push(RedisMonitorCommand {
            at: started_at.to_string(),
            command: "No recent command samples yet.".into(),
            client: None,
        });
    }

    commands
}

fn build_config_rows(connection: &mut Connection) -> Vec<RedisInfoRow> {
    let databases = read_config_value(connection, "databases");
    let maxmemory = read_config_value(connection, "maxmemory");
    let save = read_config_value(connection, "save");
    let slowlog_threshold = read_config_value(connection, "slowlog-log-slower-than");

    vec![
        RedisInfoRow {
            label: "databases".into(),
            value: databases.clone().unwrap_or_else(|| "16".into()),
            secondary: if databases.is_some() {
                Some("Configured logical DB count".into())
            } else {
                Some("Using default Redis DB count".into())
            },
        },
        RedisInfoRow {
            label: "maxmemory".into(),
            value: maxmemory.clone().unwrap_or_else(|| "unknown".into()),
            secondary: match maxmemory.as_deref() {
                Some("0") => Some("No memory cap".into()),
                Some(_) => None,
                None => Some("CONFIG GET unavailable".into()),
            },
        },
        RedisInfoRow {
            label: "maxmemory-policy".into(),
            value: read_config_value(connection, "maxmemory-policy")
                .unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "appendonly".into(),
            value: read_config_value(connection, "appendonly").unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "save".into(),
            value: save
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "disabled".into()),
            secondary: if save.is_some() {
                None
            } else {
                Some("CONFIG GET unavailable".into())
            },
        },
        RedisInfoRow {
            label: "slowlog-log-slower-than".into(),
            value: slowlog_threshold.unwrap_or_else(|| "unknown".into()),
            secondary: Some("microseconds".into()),
        },
        RedisInfoRow {
            label: "slowlog-max-len".into(),
            value: read_config_value(connection, "slowlog-max-len")
                .unwrap_or_else(|| "unknown".into()),
            secondary: None,
        },
    ]
}

fn key_meta(item: &RedisKeySummary) -> String {
    let ttl_label = item
        .ttl_seconds
        .map(|ttl| format!("TTL {ttl}s"))
        .unwrap_or_else(|| "No TTL".into());

    match item.size {
        Some(size) => format!("{} | {} | size {}", item.key_type, ttl_label, size),
        None => format!("{} | {}", item.key_type, ttl_label),
    }
}

fn prefix_for_key(key: &str) -> String {
    if let Some((prefix, _)) = key.split_once(':') {
        return format!("{prefix}:*");
    }

    if let Some((prefix, _)) = key.split_once('.') {
        return format!("{prefix}.*");
    }

    "misc".into()
}

fn read_key_type(connection: &mut Connection, key: &str) -> Result<String, String> {
    redis::cmd("TYPE")
        .arg(key)
        .query(connection)
        .map_err(|error| error.to_string())
}

fn read_ttl(connection: &mut Connection, key: &str) -> Result<Option<i64>, String> {
    let ttl: i64 = redis::cmd("TTL")
        .arg(key)
        .query(connection)
        .map_err(|error| error.to_string())?;

    if ttl < 0 {
        Ok(None)
    } else {
        Ok(Some(ttl))
    }
}

fn read_size(connection: &mut Connection, key: &str) -> Result<Option<usize>, String> {
    let key_type = read_key_type(connection, key)?;

    let value = match key_type.as_str() {
        "string" => query_optional::<usize>(connection, "STRLEN", &[key.to_string()]),
        "hash" => query_optional::<usize>(connection, "HLEN", &[key.to_string()]),
        "list" => query_optional::<usize>(connection, "LLEN", &[key.to_string()]),
        "set" => query_optional::<usize>(connection, "SCARD", &[key.to_string()]),
        "zset" => query_optional::<usize>(connection, "ZCARD", &[key.to_string()]),
        "stream" => query_optional::<usize>(connection, "XLEN", &[key.to_string()]),
        _ => None,
    };

    Ok(value)
}

fn read_info(connection: &mut Connection) -> HashMap<String, String> {
    let info: Result<String, _> = redis::cmd("INFO").query(connection);
    let mut values = HashMap::new();

    if let Ok(info) = info {
        for line in info.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once(':') {
                values.insert(key.to_string(), value.to_string());
            }
        }
    }

    values
}

fn read_config_value(connection: &mut Connection, key: &str) -> Option<String> {
    let values: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg(key)
        .query(connection)
        .ok()?;

    if values.len() >= 2 {
        Some(values[1].clone())
    } else {
        None
    }
}

fn read_slowlog_entries(connection: &mut Connection, limit: usize) -> Vec<RedisSlowlogEntry> {
    let raw: Result<RedisValue, _> = redis::cmd("SLOWLOG")
        .arg("GET")
        .arg(limit.max(1))
        .query(connection);

    match raw {
        Ok(value) => parse_slowlog_entries(&value),
        Err(_) => Vec::new(),
    }
}

fn read_stream_groups(connection: &mut Connection, key: &str) -> Vec<RedisStreamGroup> {
    let raw: Result<RedisValue, _> = redis::cmd("XINFO").arg("GROUPS").arg(key).query(connection);
    let Ok(raw) = raw else {
        return Vec::new();
    };

    let RedisValue::Array(entries) = unwrap_attribute(&raw) else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let values = redis_value_to_key_values(entry);
            let name = values.get("name")?.to_string();
            Some(RedisStreamGroup {
                consumers: values
                    .get("consumers")
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0),
                pending: values
                    .get("pending")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
                last_delivered_id: values
                    .get("last-delivered-id")
                    .cloned()
                    .unwrap_or_else(|| "-".into()),
                lag: values
                    .get("lag")
                    .and_then(|value| value.parse::<u64>().ok()),
                consumer_details: read_stream_consumers(connection, key, &name),
                name,
            })
        })
        .collect()
}

fn read_stream_consumers(
    connection: &mut Connection,
    key: &str,
    group: &str,
) -> Vec<RedisStreamConsumer> {
    let raw: Result<RedisValue, _> = redis::cmd("XINFO")
        .arg("CONSUMERS")
        .arg(key)
        .arg(group)
        .query(connection);
    let Ok(raw) = raw else {
        return Vec::new();
    };

    let RedisValue::Array(entries) = unwrap_attribute(&raw) else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let values = redis_value_to_key_values(entry);
            Some(RedisStreamConsumer {
                name: values.get("name")?.to_string(),
                pending: values
                    .get("pending")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
                idle_ms: values
                    .get("idle")
                    .or_else(|| values.get("idle-ms"))
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
            })
        })
        .collect()
}

fn parse_slowlog_entries(value: &RedisValue) -> Vec<RedisSlowlogEntry> {
    let RedisValue::Array(entries) = unwrap_attribute(value) else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let RedisValue::Array(parts) = unwrap_attribute(entry) else {
                return None;
            };

            if parts.len() < 4 {
                return None;
            }

            let id = value_to_text(&parts[0]);
            let started_at = value_as_i64(&parts[1])
                .map(unix_seconds_to_iso)
                .unwrap_or_else(|| value_to_text(&parts[1]));
            let duration_micros = value_as_i64(&parts[2])
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0);
            let command = match unwrap_attribute(&parts[3]) {
                RedisValue::Array(arguments) => arguments
                    .iter()
                    .map(value_to_text)
                    .collect::<Vec<_>>()
                    .join(" "),
                other => value_to_text(other),
            };

            Some(RedisSlowlogEntry {
                id,
                started_at,
                duration_micros,
                command,
                client_address: parts.get(4).map(value_to_text),
                client_name: parts.get(5).map(value_to_text),
            })
        })
        .collect()
}

fn parse_stream_entries(value: &RedisValue) -> Vec<ParsedStreamEntry> {
    let RedisValue::Array(entries) = unwrap_attribute(value) else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let RedisValue::Array(parts) = unwrap_attribute(entry) else {
                return None;
            };

            if parts.len() < 2 {
                return None;
            }

            Some(ParsedStreamEntry {
                id: value_to_text(&parts[0]),
                fields: parse_stream_fields(&parts[1]),
            })
        })
        .collect()
}

fn filter_stream_entries(entries: Vec<ParsedStreamEntry>, filter: &str) -> Vec<ParsedStreamEntry> {
    let lowered = filter.trim().to_ascii_lowercase();
    if lowered.is_empty() {
        return entries;
    }

    entries
        .into_iter()
        .filter(|entry| {
            entry.id.to_ascii_lowercase().contains(&lowered)
                || entry.fields.iter().any(|(field, value)| {
                    field.to_ascii_lowercase().contains(&lowered)
                        || value.to_ascii_lowercase().contains(&lowered)
                })
        })
        .collect()
}

fn parse_stream_fields(value: &RedisValue) -> Vec<(String, String)> {
    match unwrap_attribute(value) {
        RedisValue::Array(values) => {
            let mut fields = Vec::new();
            let mut index = 0;
            while index + 1 < values.len() {
                fields.push((
                    value_to_text(&values[index]),
                    value_to_text(&values[index + 1]),
                ));
                index += 2;
            }
            fields
        }
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(field, value)| (value_to_text(field), value_to_text(value)))
            .collect(),
        _ => Vec::new(),
    }
}

fn stream_preview(entries: &[ParsedStreamEntry]) -> String {
    let preview = entries
        .iter()
        .map(|entry| {
            let mut fields = JsonMap::new();
            for (field, value) in &entry.fields {
                fields.insert(field.clone(), JsonValue::String(value.clone()));
            }

            serde_json::json!({
                "id": entry.id,
                "fields": JsonValue::Object(fields),
            })
        })
        .collect::<Vec<_>>();

    serde_json::to_string_pretty(&preview).unwrap_or_else(|_| "[]".into())
}

fn stream_rows(entries: &[ParsedStreamEntry]) -> Vec<RedisInfoRow> {
    entries
        .iter()
        .map(|entry| RedisInfoRow {
            label: entry.id.clone(),
            value: truncate_display(&summarize_pairs(&entry.fields, 2), 120),
            secondary: if entry.fields.is_empty() {
                None
            } else {
                Some(format!("{} fields", entry.fields.len()))
            },
        })
        .collect()
}

fn pair_rows(values: &[String]) -> Vec<RedisInfoRow> {
    let mut rows = Vec::new();
    let mut index = 0;

    while index + 1 < values.len() {
        rows.push(RedisInfoRow {
            label: values[index].clone(),
            value: values[index + 1].clone(),
            secondary: None,
        });
        index += 2;
    }

    rows
}

fn preview_bytes(bytes: &[u8]) -> (String, String, bool) {
    let truncated = bytes.len() > STRING_PREVIEW_LIMIT;
    let sample = if truncated {
        &bytes[..STRING_PREVIEW_LIMIT]
    } else {
        bytes
    };

    if let Ok(text) = String::from_utf8(sample.to_vec()) {
        match serde_json::from_str::<JsonValue>(&text) {
            Ok(json_value) => (
                serde_json::to_string_pretty(&json_value).unwrap_or(text),
                "json".into(),
                truncated,
            ),
            Err(_) => (text, "text".into(), truncated),
        }
    } else {
        let (decoded, _, had_errors) = GBK.decode(sample);
        if !had_errors {
            (decoded.into_owned(), "text".into(), truncated)
        } else {
            (hex_string(sample), "text".into(), truncated)
        }
    }
}

fn value_to_bytes(value: &RedisValue) -> Option<Vec<u8>> {
    match unwrap_attribute(value) {
        RedisValue::BulkString(bytes) => Some(bytes.clone()),
        RedisValue::SimpleString(text) => Some(text.as_bytes().to_vec()),
        RedisValue::VerbatimString { text, .. } => Some(text.as_bytes().to_vec()),
        RedisValue::Int(number) => Some(number.to_string().into_bytes()),
        RedisValue::Double(number) => Some(number.to_string().into_bytes()),
        RedisValue::Boolean(flag) => Some(flag.to_string().into_bytes()),
        RedisValue::BigNumber(number) => Some(number.to_string().into_bytes()),
        _ => None,
    }
}

fn hex_string(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn query_optional<T: redis::FromRedisValue>(
    connection: &mut Connection,
    command_name: &str,
    args: &[String],
) -> Option<T> {
    let mut command = redis::cmd(command_name);
    for arg in args {
        command.arg(arg);
    }

    command.query(connection).ok()
}

fn hit_rate_label(info: &HashMap<String, String>) -> Option<String> {
    let hits = info.get("keyspace_hits")?.parse::<f64>().ok()?;
    let misses = info.get("keyspace_misses")?.parse::<f64>().ok()?;
    let total = hits + misses;

    if total <= 0.0 {
        return None;
    }

    Some(format!("{:.1}%", hits / total * 100.0))
}

fn summarize_pairs(fields: &[(String, String)], count: usize) -> String {
    if fields.is_empty() {
        return "No fields".into();
    }

    fields
        .iter()
        .take(count)
        .map(|(field, value)| format!("{field}={value}"))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn truncate_display(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn unix_seconds_to_iso(seconds: i64) -> String {
    DateTime::<Utc>::from_timestamp(seconds, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| seconds.to_string())
}

fn unwrap_attribute(value: &RedisValue) -> &RedisValue {
    match value {
        RedisValue::Attribute { data, .. } => data,
        other => other,
    }
}

fn value_as_i64(value: &RedisValue) -> Option<i64> {
    match unwrap_attribute(value) {
        RedisValue::Int(number) => Some(*number),
        RedisValue::BulkString(bytes) => String::from_utf8(bytes.clone()).ok()?.parse().ok(),
        RedisValue::SimpleString(text) => text.parse().ok(),
        RedisValue::Double(number) => Some(*number as i64),
        RedisValue::Boolean(flag) => Some(if *flag { 1 } else { 0 }),
        RedisValue::BigNumber(number) => number.to_string().parse().ok(),
        _ => None,
    }
}

fn value_to_text(value: &RedisValue) -> String {
    match unwrap_attribute(value) {
        RedisValue::Nil => "null".into(),
        RedisValue::Int(number) => number.to_string(),
        RedisValue::BulkString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        RedisValue::Array(values) => values
            .iter()
            .map(value_to_text)
            .collect::<Vec<_>>()
            .join(" "),
        RedisValue::SimpleString(text) => text.clone(),
        RedisValue::Okay => "OK".into(),
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(key, value)| format!("{}={}", value_to_text(key), value_to_text(value)))
            .collect::<Vec<_>>()
            .join(", "),
        RedisValue::Set(values) => values
            .iter()
            .map(value_to_text)
            .collect::<Vec<_>>()
            .join(", "),
        RedisValue::Double(number) => number.to_string(),
        RedisValue::Boolean(flag) => flag.to_string(),
        RedisValue::VerbatimString { text, .. } => text.clone(),
        RedisValue::BigNumber(number) => number.to_string(),
        RedisValue::Push { data, .. } => {
            data.iter().map(value_to_text).collect::<Vec<_>>().join(" ")
        }
        RedisValue::ServerError(error) => format!("{error:?}"),
        RedisValue::Attribute { .. } => unreachable!("attributes are unwrapped above"),
    }
}

fn redis_value_to_json(value: &RedisValue) -> JsonValue {
    match unwrap_attribute(value) {
        RedisValue::Nil => JsonValue::Null,
        RedisValue::Int(number) => JsonValue::Number((*number).into()),
        RedisValue::BulkString(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(text) => JsonValue::String(text),
            Err(_) => JsonValue::String(BASE64_STANDARD.encode(bytes)),
        },
        RedisValue::Array(values) => {
            JsonValue::Array(values.iter().map(redis_value_to_json).collect::<Vec<_>>())
        }
        RedisValue::SimpleString(text) => JsonValue::String(text.clone()),
        RedisValue::Okay => JsonValue::String("OK".into()),
        RedisValue::Map(entries) => JsonValue::Array(
            entries
                .iter()
                .map(|(key, value)| {
                    serde_json::json!({
                        "key": redis_value_to_json(key),
                        "value": redis_value_to_json(value),
                    })
                })
                .collect(),
        ),
        RedisValue::Set(values) => {
            JsonValue::Array(values.iter().map(redis_value_to_json).collect::<Vec<_>>())
        }
        RedisValue::Double(number) => serde_json::json!(number),
        RedisValue::Boolean(flag) => JsonValue::Bool(*flag),
        RedisValue::VerbatimString { text, .. } => JsonValue::String(text.clone()),
        RedisValue::BigNumber(number) => JsonValue::String(number.to_string()),
        RedisValue::Push { data, .. } => {
            JsonValue::Array(data.iter().map(redis_value_to_json).collect::<Vec<_>>())
        }
        RedisValue::ServerError(error) => JsonValue::String(format!("{error:?}")),
        RedisValue::Attribute { .. } => unreachable!("attributes are unwrapped above"),
    }
}

fn redis_value_to_rows(value: &RedisValue) -> Vec<RedisCliRow> {
    match unwrap_attribute(value) {
        RedisValue::Array(values) => values
            .iter()
            .map(|value| match unwrap_attribute(value) {
                RedisValue::Array(inner) => RedisCliRow {
                    columns: inner.iter().map(value_to_text).collect(),
                },
                other => RedisCliRow {
                    columns: vec![value_to_text(other)],
                },
            })
            .collect(),
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(key, value)| RedisCliRow {
                columns: vec![value_to_text(key), value_to_text(value)],
            })
            .collect(),
        other => vec![RedisCliRow {
            columns: vec![value_to_text(other)],
        }],
    }
}

fn redis_value_to_key_values(value: &RedisValue) -> HashMap<String, String> {
    match unwrap_attribute(value) {
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(key, value)| (value_to_text(key), value_to_text(value)))
            .collect(),
        RedisValue::Array(values) => {
            let mut map = HashMap::new();
            let mut index = 0;
            while index + 1 < values.len() {
                map.insert(
                    value_to_text(&values[index]),
                    value_to_text(&values[index + 1]),
                );
                index += 2;
            }
            map
        }
        _ => HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_connection(readonly: bool) -> ConnectionRecord {
        ConnectionRecord {
            id: "test".into(),
            kind: "redis".into(),
            protocol: String::new(),
            name: "redis-test".into(),
            host: "127.0.0.1".into(),
            port: 6391,
            database_name: "0".into(),
            username: String::new(),
            auth_mode: "password".into(),
            environment: "dev".into(),
            tags: Vec::new(),
            readonly,
            favorite: false,
            use_tls: false,
            tls_verify: true,
            ssh_enabled: false,
            ssh_host: String::new(),
            ssh_port: 22,
            ssh_username: String::new(),
            schema_registry_url: String::new(),
            group_id: String::new(),
            client_id: String::new(),
            notes: String::new(),
            last_checked_at: None,
            last_connected_at: None,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    #[test]
    fn prefixes_are_grouped_consistently() {
        assert_eq!(prefix_for_key("session:2048"), "session:*");
        assert_eq!(prefix_for_key("orders.stream"), "orders.*");
        assert_eq!(prefix_for_key("plain"), "misc");
    }

    #[test]
    fn type_filter_normalization_accepts_supported_redis_types() {
        assert_eq!(normalize_type_filter(Some("stream")), Some("stream".into()));
        assert_eq!(normalize_type_filter(Some("ALL")), None);
        assert_eq!(normalize_type_filter(Some("custom")), None);
    }

    #[test]
    fn empty_selected_key_clears_detail_selection() {
        let keys = vec![RedisKeySummary {
            key: "session:2048".into(),
            key_type: "string".into(),
            ttl_seconds: Some(60),
            size: Some(12),
        }];

        assert_eq!(resolve_selected_key_name(Some(String::new()), &keys), None);
        assert_eq!(
            resolve_selected_key_name(None, &keys),
            Some("session:2048".into())
        );
    }

    #[test]
    fn inject_selected_key_summary_keeps_selected_key_visible() {
        let mut keys = vec![RedisKeySummary {
            key: "session:2048".into(),
            key_type: "string".into(),
            ttl_seconds: Some(60),
            size: Some(12),
        }];
        let selected = RedisKeyDetail {
            key: "new:key".into(),
            key_type: "string".into(),
            ttl_seconds: Some(90),
            size: Some(7),
            encoding: Some("embstr".into()),
            preview: "created".into(),
            preview_language: "text".into(),
            raw_value_base64: Some(BASE64_STANDARD.encode("created")),
            truncated: false,
            rows: Vec::new(),
            editable: true,
        };

        if keys.iter().all(|item| item.key != selected.key) {
            if keys.len() >= 1 {
                keys.pop();
            }
            keys.insert(
                0,
                RedisKeySummary {
                    key: selected.key.clone(),
                    key_type: selected.key_type.clone(),
                    ttl_seconds: selected.ttl_seconds,
                    size: selected.size,
                },
            );
        }

        assert_eq!(keys.first().map(|item| item.key.as_str()), Some("new:key"));
    }

    #[test]
    fn collection_editor_inputs_are_parsed() {
        let hash = parse_hash_entries(r#"{"version":"v2","items":48}"#).expect("hash");
        let list = parse_string_array(r#"["job-1","job-2"]"#, "invalid").expect("list");
        let set = parse_string_array(r#"["feature.a","feature.b"]"#, "invalid").expect("set");
        let zset =
            parse_sorted_set_members(r#"[{"member":"us-east","score":91.2}]"#).expect("zset");

        assert!(hash
            .iter()
            .any(|(field, value)| field == "version" && value == "v2"));
        assert_eq!(list.len(), 2);
        assert_eq!(set[1], "feature.b");
        assert_eq!(zset[0].0, "us-east");
        assert_eq!(zset[0].1, 91.2);
    }

    #[test]
    fn stream_entries_are_parsed_into_rows() {
        let value = RedisValue::Array(vec![RedisValue::Array(vec![
            RedisValue::BulkString(b"1735738000000-0".to_vec()),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"orderId".to_vec()),
                RedisValue::BulkString(b"ord_482901".to_vec()),
                RedisValue::BulkString(b"status".to_vec()),
                RedisValue::BulkString(b"created".to_vec()),
            ]),
        ])]);

        let entries = parse_stream_entries(&value);
        let rows = stream_rows(&entries);
        let preview = stream_preview(&entries);

        assert_eq!(entries.len(), 1);
        assert_eq!(rows[0].label, "1735738000000-0");
        assert!(rows[0].value.contains("orderId=ord_482901"));
        assert!(preview.contains("\"status\": \"created\""));
    }

    #[test]
    fn slowlog_entries_are_parsed_with_command_and_client() {
        let value = RedisValue::Array(vec![RedisValue::Array(vec![
            RedisValue::Int(81),
            RedisValue::Int(1_744_536_011),
            RedisValue::Int(14_231),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"EVALSHA".to_vec()),
                RedisValue::BulkString(b"9b6f".to_vec()),
                RedisValue::BulkString(b"2".to_vec()),
            ]),
            RedisValue::BulkString(b"127.0.0.1:58234".to_vec()),
            RedisValue::BulkString(b"redis-worker".to_vec()),
        ])]);

        let entries = parse_slowlog_entries(&value);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "81");
        assert!(entries[0].command.starts_with("EVALSHA 9b6f 2"));
        assert_eq!(
            entries[0].client_address.as_deref(),
            Some("127.0.0.1:58234")
        );
        assert_eq!(entries[0].client_name.as_deref(), Some("redis-worker"));
    }

    #[test]
    #[ignore = "requires docker redis on localhost:6391"]
    fn live_redis_roundtrip_works() {
        let record = test_connection(false);
        let mut connection = open_connection(&record, None).expect("connect");

        redis::cmd("FLUSHDB")
            .query::<()>(&mut connection)
            .expect("flush");
        redis::cmd("SET")
            .arg("session:2048")
            .arg(r#"{"userId":2048}"#)
            .query::<()>(&mut connection)
            .expect("set");
        redis::cmd("EXPIRE")
            .arg("session:2048")
            .arg(120)
            .query::<bool>(&mut connection)
            .expect("expire");
        redis::cmd("HSET")
            .arg("cache:feed")
            .arg("version")
            .arg("v1")
            .arg("items")
            .arg("24")
            .query::<i64>(&mut connection)
            .expect("hset");
        redis::cmd("HSET")
            .arg("profile:meta")
            .arg("version")
            .arg("v1")
            .arg("items")
            .arg("24")
            .query::<i64>(&mut connection)
            .expect("hset");
        redis::cmd("RPUSH")
            .arg("queue:emails")
            .arg("msg_1001")
            .arg("msg_1002")
            .query::<i64>(&mut connection)
            .expect("rpush");
        redis::cmd("SADD")
            .arg("flags:beta")
            .arg("feature.dashboard")
            .arg("feature.checkout")
            .query::<i64>(&mut connection)
            .expect("sadd");
        redis::cmd("ZADD")
            .arg("scores:region")
            .arg("91.2")
            .arg("us-east")
            .arg("88.7")
            .arg("ap-southeast")
            .query::<i64>(&mut connection)
            .expect("zadd");
        redis::cmd("XADD")
            .arg("orders.stream")
            .arg("*")
            .arg("orderId")
            .arg("ord_482901")
            .arg("status")
            .arg("created")
            .query::<String>(&mut connection)
            .expect("xadd");
        create_key(&record, None, "string", "new:key", "created", Some(90)).expect("create key");
        create_key(
            &record,
            None,
            "hash",
            "new:hash",
            r#"{"field":"value","updatedBy":"desktop-tool"}"#,
            Some(75),
        )
        .expect("create hash");
        create_key(
            &record,
            None,
            "list",
            "new:list",
            r#"["job-1","job-2"]"#,
            None,
        )
        .expect("create list");
        create_key(
            &record,
            None,
            "set",
            "new:set",
            r#"["feature.dashboard","feature.checkout"]"#,
            None,
        )
        .expect("create set");
        create_key(
            &record,
            None,
            "zset",
            "new:zset",
            r#"[{"member":"us-east","score":91.2},{"member":"eu-west","score":83.5}]"#,
            Some(45),
        )
        .expect("create zset");

        let browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("session:2048".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("load browser");

        assert!(browser.loaded_count >= 3);
        assert_eq!(
            browser.selected_key.as_ref().map(|key| key.key.as_str()),
            Some("session:2048")
        );

        set_string_value(&record, None, "session:2048", r#"{"userId":4096}"#)
            .expect("update string");
        save_key_value(
            &record,
            None,
            "profile:meta",
            r#"{"version":"v2","items":48}"#,
        )
        .expect("update hash");
        save_key_value(&record, None, "queue:emails", r#"["msg_2001","msg_2002"]"#)
            .expect("update list");
        save_key_value(
            &record,
            None,
            "flags:beta",
            r#"["feature.dashboard","feature.search"]"#,
        )
        .expect("update set");
        save_key_value(
            &record,
            None,
            "scores:region",
            r#"[{"member":"us-east","score":99.1},{"member":"eu-west","score":83.5}]"#,
        )
        .expect("update zset");
        update_key_ttl(&record, None, "session:2048", Some(30)).expect("update ttl");
        delete_key(&record, None, "cache:feed").expect("delete key");

        let browser_after = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("new:key".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload browser");

        assert_eq!(
            browser_after
                .selected_key
                .as_ref()
                .map(|key| key.key.as_str()),
            Some("new:key")
        );
        assert!(browser_after
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("created"))
            .unwrap_or(false));
        let created_hash_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("new:hash".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload created hash");
        assert_eq!(
            created_hash_browser
                .selected_key
                .as_ref()
                .map(|key| key.key_type.as_str()),
            Some("hash")
        );
        assert!(created_hash_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("\"field\": \"value\""))
            .unwrap_or(false));
        assert!(created_hash_browser
            .selected_key
            .as_ref()
            .and_then(|key| key.ttl_seconds)
            .map(|ttl| ttl > 0)
            .unwrap_or(false));
        let list_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("queue:emails".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload list");
        let hash_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("profile:meta".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload hash");
        let created_list_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("new:list".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload created list");
        assert_eq!(
            created_list_browser
                .selected_key
                .as_ref()
                .map(|key| key.key_type.as_str()),
            Some("list")
        );
        assert!(created_list_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("job-1"))
            .unwrap_or(false));
        assert!(hash_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("\"version\": \"v2\""))
            .unwrap_or(false));
        assert!(list_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("msg_2001"))
            .unwrap_or(false));
        let set_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("flags:beta".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload set");
        let created_set_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("new:set".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload created set");
        assert_eq!(
            created_set_browser
                .selected_key
                .as_ref()
                .map(|key| key.key_type.as_str()),
            Some("set")
        );
        assert!(created_set_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("feature.checkout"))
            .unwrap_or(false));
        assert!(set_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("feature.search"))
            .unwrap_or(false));
        let zset_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("scores:region".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload zset");
        let created_zset_browser = load_browser(
            &record,
            None,
            String::new(),
            20,
            Some("new:zset".into()),
            None,
            None,
            Vec::new(),
        )
        .expect("reload created zset");
        assert_eq!(
            created_zset_browser
                .selected_key
                .as_ref()
                .map(|key| key.key_type.as_str()),
            Some("zset")
        );
        assert!(created_zset_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("\"member\": \"eu-west\""))
            .unwrap_or(false));
        assert!(created_zset_browser
            .selected_key
            .as_ref()
            .and_then(|key| key.ttl_seconds)
            .map(|ttl| ttl > 0)
            .unwrap_or(false));
        assert!(zset_browser
            .selected_key
            .as_ref()
            .map(|key| key.preview.contains("\"member\": \"eu-west\""))
            .unwrap_or(false));
        assert!(format!("{:?}", browser_after.resources).contains("new:hash"));
        assert!(format!("{:?}", browser_after.resources).contains("new:list"));
        assert!(format!("{:?}", browser_after.resources).contains("new:set"));
        assert!(format!("{:?}", browser_after.resources).contains("new:zset"));
        assert!(browser_after
            .resources
            .iter()
            .all(|node| !format!("{node:?}").contains("cache:feed")));
    }
}
