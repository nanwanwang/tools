use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use chrono::{DateTime, Utc};
use redis::{Client, Connection, Value as RedisValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use url::Url;

use crate::models::{
    ConnectionHealth, ConnectionRecord, RedisActionInput, RedisActionResult, RedisBrowseResult,
    RedisBulkDeleteResult, RedisCapabilitySnapshot, RedisCommandResult, RedisCommandTable,
    RedisInfoRow, RedisKeyDetail, RedisKeySummary, RedisSlowlogEntry, RedisStreamConsumerGroup,
    RedisStreamState, RedisWorkbenchResult, ResourceNode, WorkspaceMetric,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowseCursorState {
    scan_cursor: u64,
    buffered_keys: Vec<String>,
}

impl Default for BrowseCursorState {
    fn default() -> Self {
        Self {
            scan_cursor: 0,
            buffered_keys: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct ParsedStreamEntry {
    id: String,
    fields: Vec<(String, String)>,
}

pub fn browse_keys(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    pattern: String,
    type_filter: String,
    limit: usize,
    cursor: Option<String>,
    view_mode: String,
) -> Result<RedisBrowseResult, String> {
    if connection.ssh_enabled {
        return Ok(unsupported_browse_result(
            connection,
            database,
            pattern,
            limit,
            cursor,
            view_mode,
            "SSH tunnel connections are not supported in this Redis slice.".into(),
        ));
    }

    let mut client = open_connection(connection, secret, Some(database))?;
    let info = read_info(&mut client);
    let capability = load_capability_snapshot(connection, &mut client, &info);
    let db_size: u64 = redis::cmd("DBSIZE").query(&mut client).unwrap_or(0);
    let config_rows = build_config_rows(&mut client);
    let server_rows = build_server_rows(&info);
    let info_rows = build_info_rows(connection, database, db_size, &info, &capability);

    if !capability.browser_supported {
        return Ok(RedisBrowseResult {
            connection_id: connection.id.clone(),
            database,
            pattern,
            limit,
            cursor: cursor.unwrap_or_else(|| "0".into()),
            next_cursor: None,
            loaded_count: 0,
            scanned_count: 0,
            has_more: false,
            view_mode,
            type_filter: normalize_type_filter(&type_filter),
            metrics: build_metrics(connection.readonly, db_size, &[]),
            resources: vec![ResourceNode {
                id: format!("db:{database}"),
                label: format!("db{database}"),
                kind: "database".into(),
                meta: Some("Unsupported target".into()),
                children: Some(Vec::new()),
                expandable: None,
            }],
            key_summaries: Vec::new(),
            diagnostics: capability.diagnostics.clone(),
            info_rows,
            server_rows,
            config_rows,
            capability,
        });
    }

    let normalized_type_filter = normalize_type_filter(&type_filter);
    let (keys, next_cursor, scanned_count) = collect_keys(
        &mut client,
        &pattern,
        limit.max(1),
        cursor.clone(),
        &normalized_type_filter,
    )?;
    let has_more = next_cursor.is_some();

    Ok(RedisBrowseResult {
        connection_id: connection.id.clone(),
        database,
        pattern,
        limit,
        cursor: cursor.unwrap_or_else(|| "0".into()),
        next_cursor: next_cursor.clone(),
        loaded_count: keys.len(),
        scanned_count,
        has_more,
        view_mode,
        type_filter: normalized_type_filter.clone(),
        metrics: build_metrics(connection.readonly, db_size, &keys),
        resources: build_resources(database, &keys),
        key_summaries: keys.clone(),
        diagnostics: build_diagnostics(connection, &keys, has_more, &capability),
        info_rows,
        server_rows,
        config_rows,
        capability,
    })
}

pub fn get_key_detail(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    key: &str,
) -> Result<RedisKeyDetail, String> {
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;
    load_key_detail(&mut client, key)
}

pub fn execute_action(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    input: RedisActionInput,
) -> Result<RedisActionResult, String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;

    match input.action.as_str() {
        "create-key" => {
            let key = input.key.ok_or_else(|| "Key name is required.".to_string())?;
            let key_type = input
                .key_type
                .ok_or_else(|| "Key type is required.".to_string())?;
            let value = input
                .value
                .ok_or_else(|| "Key value is required.".to_string())?;
            create_key_on_connection(&mut client, &key_type, &key, &value, input.ttl_seconds)?;
            Ok(RedisActionResult {
                message: format!("{key} created."),
            })
        }
        "save-value" => {
            let key = input.key.ok_or_else(|| "Key name is required.".to_string())?;
            let value = input
                .value
                .ok_or_else(|| "Key value is required.".to_string())?;
            save_key_value_on_connection(&mut client, &key, &value)?;
            Ok(RedisActionResult {
                message: format!("{key} updated."),
            })
        }
        "update-ttl" => {
            let key = input.key.ok_or_else(|| "Key name is required.".to_string())?;
            update_key_ttl_on_connection(&mut client, &key, input.ttl_seconds)?;
            Ok(RedisActionResult {
                message: if input.ttl_seconds.is_some() {
                    format!("{key} TTL updated.")
                } else {
                    format!("{key} TTL removed.")
                },
            })
        }
        "delete-key" => {
            let key = input.key.ok_or_else(|| "Key name is required.".to_string())?;
            delete_key_on_connection(&mut client, &key)?;
            Ok(RedisActionResult {
                message: format!("{key} deleted."),
            })
        }
        _ => Err("Unsupported Redis action.".into()),
    }
}

pub fn bulk_delete(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    pattern: String,
    type_filter: String,
    dry_run: bool,
) -> Result<RedisBulkDeleteResult, String> {
    if !dry_run {
        ensure_writable(connection)?;
    }

    let started = Instant::now();
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;

    let normalized_type_filter = normalize_type_filter(&type_filter);
    let matched_keys = collect_all_matching_keys(&mut client, &pattern, &normalized_type_filter)?;
    let sample_keys = matched_keys.iter().take(10).cloned().collect::<Vec<_>>();

    let deleted = if dry_run {
        0
    } else {
        let mut deleted = 0_usize;
        for key in &matched_keys {
            deleted += redis::cmd("DEL")
                .arg(key)
                .query::<u64>(&mut client)
                .map_err(|error| error.to_string())? as usize;
        }
        deleted
    };

    Ok(RedisBulkDeleteResult {
        pattern,
        type_filter: normalized_type_filter,
        dry_run,
        matched: matched_keys.len(),
        deleted,
        sample_keys,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

pub fn run_cli_command(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    statement: &str,
) -> Result<RedisCommandResult, String> {
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;
    execute_statement(connection, &mut client, statement)
}

pub fn run_workbench_query(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    input: String,
) -> Result<RedisWorkbenchResult, String> {
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;

    let statements = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| execute_statement(connection, &mut client, line))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RedisWorkbenchResult { statements })
}

pub fn get_slowlog(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    limit: usize,
) -> Result<Vec<RedisSlowlogEntry>, String> {
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;
    Ok(read_slowlog_entries(&mut client, limit.max(1)))
}

pub fn get_stream_state(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    key: &str,
    count: usize,
) -> Result<RedisStreamState, String> {
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;
    load_stream_state(&mut client, key, count.max(1))
}

pub fn json_get(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    key: &str,
) -> Result<String, String> {
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;
    json_get_on_connection(&mut client, key)
}

pub fn json_set(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: u8,
    key: &str,
    value: &str,
) -> Result<(), String> {
    ensure_writable(connection)?;
    let mut client = open_connection(connection, secret, Some(database))?;
    ensure_standalone_supported(connection, &mut client)?;
    json_set_on_connection(&mut client, key, value)
}

pub fn run_redis_health_check(
    connection: &ConnectionRecord,
    secret: Option<String>,
) -> ConnectionHealth {
    let checked_at = Utc::now().to_rfc3339();

    if connection.ssh_enabled {
        return ConnectionHealth {
            status: "degraded".into(),
            summary: "SSH tunnel connections are saved, but not supported yet.".into(),
            details: vec![
                format!("Target {}:{} is configured behind SSH.", connection.host, connection.port),
                "Redis health checks currently require direct standalone access.".into(),
            ],
            latency_ms: None,
            checked_at,
        };
    }

    let started = Instant::now();
    let mut client = match open_connection(
        connection,
        secret,
        Some(connection.database_name.parse::<u8>().unwrap_or(0)),
    ) {
        Ok(client) => client,
        Err(error) => {
            return ConnectionHealth {
                status: "unreachable".into(),
                summary: "Redis handshake failed.".into(),
                details: vec![error],
                latency_ms: None,
                checked_at,
            }
        }
    };

    let ping_result = redis::cmd("PING").query::<String>(&mut client);
    let info = read_info(&mut client);
    let latency_ms = started.elapsed().as_millis() as u64;
    let capability = load_capability_snapshot(connection, &mut client, &info);

    match ping_result {
        Ok(reply) if capability.browser_supported => {
            let mut details = vec![
                format!("PING replied with {reply}."),
                format!(
                    "Connected in {} mode to db{}.",
                    capability.server_mode, connection.database_name
                ),
            ];

            if capability.supports_json {
                details.push("RedisJSON module detected.".into());
            }

            if connection.environment == "production" {
                details.push("Production profile keeps write paths guarded.".into());
                ConnectionHealth {
                    status: "degraded".into(),
                    summary: "Redis is reachable. Production safeguards remain enabled.".into(),
                    details,
                    latency_ms: Some(latency_ms),
                    checked_at,
                }
            } else {
                ConnectionHealth {
                    status: "healthy".into(),
                    summary: "Redis handshake and command checks succeeded.".into(),
                    details,
                    latency_ms: Some(latency_ms),
                    checked_at,
                }
            }
        }
        Ok(_) => ConnectionHealth {
            status: "degraded".into(),
            summary: "Redis is reachable, but this deployment mode is not supported yet.".into(),
            details: capability.diagnostics,
            latency_ms: Some(latency_ms),
            checked_at,
        },
        Err(error) => ConnectionHealth {
            status: "unreachable".into(),
            summary: "Redis handshake failed.".into(),
            details: vec![error.to_string()],
            latency_ms: None,
            checked_at,
        },
    }
}

fn unsupported_browse_result(
    connection: &ConnectionRecord,
    database: u8,
    pattern: String,
    limit: usize,
    cursor: Option<String>,
    view_mode: String,
    reason: String,
) -> RedisBrowseResult {
    let capability = RedisCapabilitySnapshot {
        connection_id: connection.id.clone(),
        server_mode: "unknown".into(),
        db_count: None,
        module_names: Vec::new(),
        supports_json: false,
        supports_slowlog: false,
        readonly: connection.readonly,
        browser_supported: false,
        unsupported_reason: Some(reason.clone()),
        diagnostics: vec![reason.clone()],
    };

    RedisBrowseResult {
        connection_id: connection.id.clone(),
        database,
        pattern,
        limit,
        cursor: cursor.unwrap_or_else(|| "0".into()),
        next_cursor: None,
        loaded_count: 0,
        scanned_count: 0,
        has_more: false,
        view_mode,
        type_filter: "all".into(),
        metrics: build_metrics(connection.readonly, 0, &[]),
        resources: vec![ResourceNode {
            id: format!("db:{database}"),
            label: format!("db{database}"),
            kind: "database".into(),
            meta: Some("Unsupported target".into()),
            children: Some(Vec::new()),
        }],
        key_summaries: Vec::new(),
        diagnostics: capability.diagnostics.clone(),
        info_rows: Vec::new(),
        server_rows: Vec::new(),
        config_rows: Vec::new(),
        capability,
    }
}

fn load_capability_snapshot(
    connection: &ConnectionRecord,
    client: &mut Connection,
    info: &HashMap<String, String>,
) -> RedisCapabilitySnapshot {
    let mut diagnostics = Vec::new();
    let cluster_enabled = info.get("cluster_enabled").map(String::as_str) == Some("1");
    let server_mode = if cluster_enabled {
        "cluster".to_string()
    } else {
        match info.get("redis_mode").map(String::as_str) {
            Some("standalone") => "standalone".to_string(),
            Some("sentinel") => "sentinel".to_string(),
            Some(other) => other.to_string(),
            None => "unknown".to_string(),
        }
    };
    let module_names = read_module_names(client);
    let supports_json = module_names.iter().any(|name| {
        let normalized = name.to_ascii_lowercase();
        normalized.contains("rejson") || normalized == "json"
    });
    let supports_slowlog = redis::cmd("SLOWLOG")
        .arg("LEN")
        .query::<u64>(client)
        .is_ok();
    let db_count = query_optional::<Vec<String>>(client, "CONFIG", &["GET".into(), "databases".into()])
        .and_then(|values| values.get(1).cloned())
        .and_then(|value| value.parse::<u32>().ok());

    if connection.ssh_enabled {
        diagnostics.push("SSH tunnel connections are not supported in this version.".into());
    }

    if server_mode != "standalone" {
        diagnostics.push(format!(
            "Detected {server_mode} mode. Only standalone Redis is supported right now."
        ));
    }

    if supports_json {
        diagnostics.push("RedisJSON module detected.".into());
    } else {
        diagnostics.push("RedisJSON module not detected.".into());
    }

    if !supports_slowlog {
        diagnostics.push("Slow Log commands are not available for this target.".into());
    }

    let unsupported_reason = if connection.ssh_enabled {
        Some("SSH tunnel connections are not supported yet.".into())
    } else if server_mode != "standalone" {
        Some(format!(
            "Only standalone Redis is supported in this version. Current mode: {server_mode}."
        ))
    } else {
        None
    };

    RedisCapabilitySnapshot {
        connection_id: connection.id.clone(),
        server_mode,
        db_count,
        module_names,
        supports_json,
        supports_slowlog,
        readonly: connection.readonly,
        browser_supported: unsupported_reason.is_none(),
        unsupported_reason,
        diagnostics,
    }
}

fn collect_keys(
    connection: &mut Connection,
    pattern: &str,
    limit: usize,
    cursor: Option<String>,
    type_filter: &str,
) -> Result<(Vec<RedisKeySummary>, Option<String>, usize), String> {
    let mut state = decode_cursor(cursor);
    let mut summaries = Vec::new();
    let mut scanned_count = 0_usize;

    while summaries.len() < limit && !state.buffered_keys.is_empty() {
        let key = state.buffered_keys.remove(0);
        let summary = load_key_summary(connection, &key)?;
        if matches_type_filter(&summary.key_type, type_filter) {
            summaries.push(summary);
        }
    }

    loop {
        if summaries.len() >= limit {
            break;
        }

        let mut command = redis::cmd("SCAN");
        command.arg(state.scan_cursor);
        if !pattern.trim().is_empty() {
            command.arg("MATCH").arg(pattern);
        }
        command.arg("COUNT").arg(150);

        let (next_scan_cursor, batch): (u64, Vec<String>) = command
            .query(connection)
            .map_err(|error| error.to_string())?;
        scanned_count += batch.len();
        state.scan_cursor = next_scan_cursor;

        for key in batch {
            let summary = load_key_summary(connection, &key)?;
            if matches_type_filter(&summary.key_type, type_filter) {
                if summaries.len() < limit {
                    summaries.push(summary);
                } else {
                    state.buffered_keys.push(key);
                }
            }
        }

        if state.scan_cursor == 0 {
            break;
        }
    }

    let next_cursor = if state.scan_cursor != 0 || !state.buffered_keys.is_empty() {
        Some(encode_cursor(&state))
    } else {
        None
    };

    Ok((summaries, next_cursor, scanned_count))
}

fn collect_all_matching_keys(
    connection: &mut Connection,
    pattern: &str,
    type_filter: &str,
) -> Result<Vec<String>, String> {
    let mut cursor = 0_u64;
    let mut matched = Vec::new();

    loop {
        let mut command = redis::cmd("SCAN");
        command.arg(cursor);
        if !pattern.trim().is_empty() {
            command.arg("MATCH").arg(pattern);
        }
        command.arg("COUNT").arg(250);

        let (next_cursor, batch): (u64, Vec<String>) = command
            .query(connection)
            .map_err(|error| error.to_string())?;
        cursor = next_cursor;

        for key in batch {
            let summary = load_key_summary(connection, &key)?;
            if matches_type_filter(&summary.key_type, type_filter) {
                matched.push(summary.key);
            }
        }

        if cursor == 0 {
            break;
        }
    }

    Ok(matched)
}

fn decode_cursor(cursor: Option<String>) -> BrowseCursorState {
    let Some(cursor) = cursor.filter(|value| !value.trim().is_empty()) else {
        return BrowseCursorState::default();
    };

    if let Ok(scan_cursor) = cursor.parse::<u64>() {
        return BrowseCursorState {
            scan_cursor,
            buffered_keys: Vec::new(),
        };
    }

    serde_json::from_str::<BrowseCursorState>(&cursor).unwrap_or_default()
}

fn encode_cursor(state: &BrowseCursorState) -> String {
    serde_json::to_string(state).unwrap_or_else(|_| "0".into())
}

fn load_key_summary(connection: &mut Connection, key: &str) -> Result<RedisKeySummary, String> {
    let key_type = read_key_type(connection, key)?;
    let ttl_seconds = read_ttl(connection, key)?;
    let size = read_size(connection, key, &key_type)?;
    let namespace = prefix_for_key(key);

    Ok(RedisKeySummary {
        id: format!("key:{key}"),
        key: key.to_string(),
        key_type: key_type.clone(),
        ttl_seconds,
        size,
        namespace: namespace.clone(),
        display_name: key.to_string(),
        meta: key_meta(&key_type, ttl_seconds, size),
    })
}

fn load_key_detail(connection: &mut Connection, key: &str) -> Result<RedisKeyDetail, String> {
    let key_type = read_key_type(connection, key)?;
    let ttl_seconds = read_ttl(connection, key)?;
    let size = read_size(connection, key, &key_type)?;
    let encoding = query_optional::<String>(
        connection,
        "OBJECT",
        &[String::from("ENCODING"), key.to_string()],
    );

    match key_type.as_str() {
        "string" => {
            let value: Vec<u8> = redis::cmd("GET")
                .arg(key)
                .query(connection)
                .map_err(|error| error.to_string())?;
            let raw = String::from_utf8_lossy(&value).into_owned();
            let pretty = pretty_or_raw(&raw);
            Ok(build_detail(
                key,
                &key_type,
                ttl_seconds,
                size,
                encoding,
                Vec::new(),
                true,
                false,
                "json_or_text",
                &raw,
                &pretty,
                None,
            ))
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
                .collect::<JsonMap<_, _>>();
            let raw = serde_json::to_string(&preview_map).unwrap_or_else(|_| "{}".into());
            let pretty = serde_json::to_string_pretty(&preview_map).unwrap_or_else(|_| "{}".into());
            Ok(build_detail(
                key,
                &key_type,
                ttl_seconds,
                size,
                encoding,
                rows,
                true,
                false,
                "json",
                &raw,
                &pretty,
                None,
            ))
        }
        "list" => {
            let values: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg(99)
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
            let raw = serde_json::to_string(&values).unwrap_or_else(|_| "[]".into());
            let pretty = serde_json::to_string_pretty(&values).unwrap_or_else(|_| "[]".into());
            Ok(build_detail(
                key,
                &key_type,
                ttl_seconds,
                size,
                encoding,
                rows,
                true,
                false,
                "json",
                &raw,
                &pretty,
                None,
            ))
        }
        "set" => {
            let mut values: Vec<String> = redis::cmd("SMEMBERS")
                .arg(key)
                .query(connection)
                .map_err(|error| error.to_string())?;
            values.sort();
            let rows = values
                .iter()
                .map(|value| RedisInfoRow {
                    label: "member".into(),
                    value: value.clone(),
                    secondary: None,
                })
                .collect::<Vec<_>>();
            let raw = serde_json::to_string(&values).unwrap_or_else(|_| "[]".into());
            let pretty = serde_json::to_string_pretty(&values).unwrap_or_else(|_| "[]".into());
            Ok(build_detail(
                key,
                &key_type,
                ttl_seconds,
                size,
                encoding,
                rows,
                true,
                false,
                "json",
                &raw,
                &pretty,
                None,
            ))
        }
        "zset" => {
            let values: Vec<String> = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg(99)
                .arg("WITHSCORES")
                .query(connection)
                .map_err(|error| error.to_string())?;
            let rows = pair_rows(&values);
            let preview = rows
                .iter()
                .map(|row| serde_json::json!({ "member": row.label, "score": row.value }))
                .collect::<Vec<_>>();
            let raw = serde_json::to_string(&preview).unwrap_or_else(|_| "[]".into());
            let pretty = serde_json::to_string_pretty(&preview).unwrap_or_else(|_| "[]".into());
            Ok(build_detail(
                key,
                &key_type,
                ttl_seconds,
                size,
                encoding,
                rows,
                true,
                false,
                "json",
                &raw,
                &pretty,
                None,
            ))
        }
        "json" => {
            let raw = json_get_on_connection(connection, key)?;
            let pretty = pretty_json_value(&raw)?;
            Ok(build_detail(
                key,
                &key_type,
                ttl_seconds,
                size,
                encoding,
                json_rows(&pretty),
                true,
                false,
                "json",
                &raw,
                &pretty,
                None,
            ))
        }
        "stream" => {
            let stream_state = load_stream_state(connection, key, 20)?;
            let raw = serde_json::to_string(
                &stream_state
                    .entries
                    .iter()
                    .map(|entry| {
                        serde_json::json!({
                            "id": entry.label,
                            "summary": entry.value,
                            "detail": entry.secondary,
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| "[]".into());
            let pretty = serde_json::to_string_pretty(
                &stream_state
                    .entries
                    .iter()
                    .map(|entry| {
                        serde_json::json!({
                            "id": entry.label,
                            "summary": entry.value,
                            "detail": entry.secondary,
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| "[]".into());
            Ok(build_detail(
                key,
                &key_type,
                ttl_seconds,
                size,
                encoding,
                stream_state.entries.clone(),
                false,
                true,
                "json",
                &raw,
                &pretty,
                Some(stream_state),
            ))
        }
        _ => {
            let raw = "Unsupported Redis type preview.".to_string();
            Ok(build_detail(
                key,
                "unknown",
                ttl_seconds,
                size,
                encoding,
                Vec::new(),
                false,
                false,
                "text",
                &raw,
                &raw,
                None,
            ))
        }
    }
}

fn build_detail(
    key: &str,
    key_type: &str,
    ttl_seconds: Option<i64>,
    size: Option<usize>,
    encoding: Option<String>,
    rows: Vec<RedisInfoRow>,
    editable: bool,
    can_refresh: bool,
    preview_language: &str,
    raw: &str,
    pretty: &str,
    stream_state: Option<RedisStreamState>,
) -> RedisKeyDetail {
    let format_previews = build_format_previews(raw, pretty);
    RedisKeyDetail {
        key: key.to_string(),
        key_type: key_type.to_string(),
        ttl_seconds,
        size,
        encoding,
        rows,
        editable,
        can_refresh,
        preview_language: if preview_language == "json_or_text" {
            if raw == pretty {
                "text".into()
            } else {
                "json".into()
            }
        } else {
            preview_language.to_string()
        },
        format_previews,
        available_formats: vec!["pretty".into(), "raw".into(), "ascii".into(), "hex".into()],
        default_format: if raw == pretty { "raw".into() } else { "pretty".into() },
        stream_state,
    }
}

fn build_format_previews(raw: &str, pretty: &str) -> BTreeMap<String, String> {
    let bytes = raw.as_bytes();
    let mut previews = BTreeMap::new();
    previews.insert("pretty".into(), pretty.to_string());
    previews.insert("raw".into(), raw.to_string());
    previews.insert("ascii".into(), ascii_preview(bytes));
    previews.insert("hex".into(), hex_preview(bytes));
    previews
}

fn load_stream_state(
    connection: &mut Connection,
    key: &str,
    count: usize,
) -> Result<RedisStreamState, String> {
    let info_value: RedisValue = redis::cmd("XINFO")
        .arg("STREAM")
        .arg(key)
        .query(connection)
        .map_err(|error| error.to_string())?;
    let info_map = value_pairs_to_map(&info_value);
    let raw_entries: RedisValue = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(count)
        .query(connection)
        .map_err(|error| error.to_string())?;
    let groups_value: RedisValue = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(key)
        .query(connection)
        .unwrap_or(RedisValue::Array(Vec::new()));

    Ok(RedisStreamState {
        key: key.to_string(),
        length: info_map.get("length").and_then(|value| value.parse::<u64>().ok()),
        radix_tree_keys: info_map
            .get("radix-tree-keys")
            .and_then(|value| value.parse::<u64>().ok()),
        radix_tree_nodes: info_map
            .get("radix-tree-nodes")
            .and_then(|value| value.parse::<u64>().ok()),
        last_generated_id: info_map.get("last-generated-id").cloned(),
        suggested_refresh_seconds: Some(5),
        groups: parse_stream_groups(&groups_value),
        entries: stream_rows(&parse_stream_entries(&raw_entries)),
    })
}

fn parse_stream_groups(value: &RedisValue) -> Vec<RedisStreamConsumerGroup> {
    let RedisValue::Array(entries) = unwrap_attribute(value) else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let row = value_pairs_to_map(entry);
            let name = row.get("name")?.clone();
            Some(RedisStreamConsumerGroup {
                name,
                consumers: row
                    .get("consumers")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
                pending: row
                    .get("pending")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0),
                last_delivered_id: row.get("last-delivered-id").cloned(),
                lag: row.get("lag").and_then(|value| value.parse::<u64>().ok()),
            })
        })
        .collect()
}

fn json_get_on_connection(connection: &mut Connection, key: &str) -> Result<String, String> {
    redis::cmd("JSON.GET")
        .arg(key)
        .query(connection)
        .map_err(|error| error.to_string())
}

fn json_set_on_connection(
    connection: &mut Connection,
    key: &str,
    value: &str,
) -> Result<(), String> {
    serde_json::from_str::<JsonValue>(value)
        .map_err(|_| "RedisJSON values must be valid JSON.".to_string())?;

    redis::cmd("JSON.SET")
        .arg(key)
        .arg("$")
        .arg(value)
        .query::<String>(connection)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn create_key_on_connection(
    connection: &mut Connection,
    key_type: &str,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    validate_create_request(key, ttl_seconds)?;
    ensure_key_absent(connection, key)?;

    match normalize_key_type(key_type).as_str() {
        "string" => create_string_key_on_connection(connection, key, value, ttl_seconds),
        "hash" => create_hash_key(connection, key, value, ttl_seconds),
        "list" => create_list_key(connection, key, value, ttl_seconds),
        "set" => create_set_key(connection, key, value, ttl_seconds),
        "zset" => create_zset_key(connection, key, value, ttl_seconds),
        "json" => create_json_key(connection, key, value, ttl_seconds),
        _ => Err("Unsupported Redis key type.".into()),
    }
}

fn save_key_value_on_connection(
    connection: &mut Connection,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let key_type = read_key_type(connection, key)?;

    match key_type.as_str() {
        "string" => set_string_value_on_connection(connection, key, value),
        "hash" => replace_hash_entries(connection, key, value),
        "list" => replace_list_elements(connection, key, value),
        "set" => replace_set_members(connection, key, value),
        "zset" => replace_sorted_set_members(connection, key, value),
        "json" => replace_json_value(connection, key, value),
        "stream" => Err("Stream editing is not supported in this version.".into()),
        _ => Err("This Redis type is not editable in this version.".into()),
    }
}

fn update_key_ttl_on_connection(
    connection: &mut Connection,
    key: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    if let Some(ttl_seconds) = ttl_seconds {
        if ttl_seconds <= 0 {
            return Err("TTL must be a positive number of seconds.".into());
        }

        redis::cmd("EXPIRE")
            .arg(key)
            .arg(ttl_seconds)
            .query::<bool>(connection)
            .map_err(|error| error.to_string())?;
    } else {
        redis::cmd("PERSIST")
            .arg(key)
            .query::<bool>(connection)
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

fn delete_key_on_connection(connection: &mut Connection, key: &str) -> Result<(), String> {
    redis::cmd("DEL")
        .arg(key)
        .query::<u64>(connection)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn execute_statement(
    connection: &ConnectionRecord,
    client: &mut Connection,
    statement: &str,
) -> Result<RedisCommandResult, String> {
    let tokens = parse_command(statement)?;
    let command_name = tokens
        .first()
        .ok_or_else(|| "Command cannot be empty.".to_string())?
        .to_ascii_uppercase();
    let started = Instant::now();

    if connection.readonly && is_write_command(&command_name) {
        return Ok(RedisCommandResult {
            statement: statement.to_string(),
            summary: "Blocked by read-only mode.".into(),
            duration_ms: started.elapsed().as_millis() as u64,
            raw_output: String::new(),
            json_output: None,
            table: None,
            error: Some("This connection is read-only. The command was not executed.".into()),
        });
    }

    let mut command = redis::cmd(&tokens[0]);
    for token in tokens.iter().skip(1) {
        command.arg(token);
    }

    let result = command.query::<RedisValue>(client);
    let duration_ms = started.elapsed().as_millis() as u64;

    match result {
        Ok(value) => {
            let raw_output = value_to_text(&value);
            let json_output = serde_json::to_string_pretty(&value_to_json(&value)).ok();
            let table = value_to_table(&value);
            Ok(RedisCommandResult {
                statement: statement.to_string(),
                summary: build_command_summary(&value, &table),
                duration_ms,
                raw_output,
                json_output,
                table,
                error: None,
            })
        }
        Err(error) => Ok(RedisCommandResult {
            statement: statement.to_string(),
            summary: "Command returned an error.".into(),
            duration_ms,
            raw_output: String::new(),
            json_output: None,
            table: None,
            error: Some(error.to_string()),
        }),
    }
}

fn build_command_summary(value: &RedisValue, table: &Option<RedisCommandTable>) -> String {
    if let Some(table) = table {
        return format!("Returned {} row(s).", table.rows.len());
    }

    match unwrap_attribute(value) {
        RedisValue::Nil => "Command returned no data.".into(),
        RedisValue::Array(values) => format!("Returned {} value(s).", values.len()),
        RedisValue::Map(entries) => format!("Returned {} field(s).", entries.len()),
        RedisValue::Set(values) => format!("Returned {} member(s).", values.len()),
        _ => "Command executed successfully.".into(),
    }
}

fn parse_command(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaping = false;

    for character in input.chars() {
        if escaping {
            current.push(character);
            escaping = false;
            continue;
        }

        if character == '\\' {
            escaping = true;
            continue;
        }

        if let Some(active_quote) = quote {
            if character == active_quote {
                quote = None;
            } else {
                current.push(character);
            }
            continue;
        }

        if character == '"' || character == '\'' {
            quote = Some(character);
            continue;
        }

        if character.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }

        current.push(character);
    }

    if escaping {
        current.push('\\');
    }

    if quote.is_some() {
        return Err("Unclosed quote in command.".into());
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    if tokens.is_empty() {
        return Err("Command cannot be empty.".into());
    }

    Ok(tokens)
}

fn is_write_command(command: &str) -> bool {
    matches!(
        command,
        "APPEND"
            | "BITFIELD"
            | "BITOP"
            | "BLMOVE"
            | "BLMPOP"
            | "BLPOP"
            | "BRPOP"
            | "COPY"
            | "DECR"
            | "DEL"
            | "EVAL"
            | "EVALSHA"
            | "EXPIRE"
            | "FLUSHALL"
            | "FLUSHDB"
            | "GETDEL"
            | "GETEX"
            | "HDEL"
            | "HINCRBY"
            | "HSET"
            | "HSETNX"
            | "INCR"
            | "INCRBY"
            | "JSON.ARRAPPEND"
            | "JSON.ARRINSERT"
            | "JSON.CLEAR"
            | "JSON.DEL"
            | "JSON.FORGET"
            | "JSON.MERGE"
            | "JSON.NUMINCRBY"
            | "JSON.SET"
            | "LINSERT"
            | "LMOVE"
            | "LPOP"
            | "LPUSH"
            | "LPUSHX"
            | "LREM"
            | "LSET"
            | "MSET"
            | "PERSIST"
            | "PEXPIRE"
            | "PSETEX"
            | "RENAME"
            | "RESTORE"
            | "RPOP"
            | "RPUSH"
            | "SADD"
            | "SET"
            | "SETEX"
            | "SETNX"
            | "SMOVE"
            | "SPOP"
            | "SREM"
            | "UNLINK"
            | "XACK"
            | "XADD"
            | "XCLAIM"
            | "XDEL"
            | "XGROUP"
            | "XTRIM"
            | "ZADD"
            | "ZINCRBY"
            | "ZPOPMAX"
            | "ZPOPMIN"
            | "ZREM"
    )
}

fn ensure_writable(connection: &ConnectionRecord) -> Result<(), String> {
    if connection.readonly {
        return Err("This connection is read-only.".into());
    }

    if connection.ssh_enabled {
        return Err("SSH tunnel connections are not supported yet.".into());
    }

    Ok(())
}

fn ensure_standalone_supported(
    connection: &ConnectionRecord,
    client: &mut Connection,
) -> Result<(), String> {
    if connection.ssh_enabled {
        return Err("SSH tunnel connections are not supported yet.".into());
    }

    let info = read_info(client);
    let capability = load_capability_snapshot(connection, client, &info);
    if let Some(reason) = capability.unsupported_reason {
        return Err(reason);
    }

    Ok(())
}

fn open_connection(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database_override: Option<u8>,
) -> Result<Connection, String> {
    if connection.kind != "redis" {
        return Err("This command only supports Redis connections.".into());
    }

    if connection.ssh_enabled {
        return Err("SSH tunnel connections are not supported yet.".into());
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

    let database_index =
        database_override.unwrap_or_else(|| connection.database_name.parse::<u8>().unwrap_or(0));
    url.set_path(&format!("/{database_index}"));

    if let Some(secret) = secret.filter(|value| !value.trim().is_empty()) {
        if !connection.username.trim().is_empty() {
            url.set_username(connection.username.trim())
                .map_err(|_| "Invalid Redis username.".to_string())?;
        }
        url.set_password(Some(secret.as_str()))
            .map_err(|_| "Invalid Redis password.".to_string())?;
    }

    if connection.use_tls && !connection.tls_verify {
        url.set_fragment(Some("insecure"));
    }

    let client = Client::open(url.as_str()).map_err(|error| error.to_string())?;
    client.get_connection().map_err(|error| error.to_string())
}

fn read_info(connection: &mut Connection) -> HashMap<String, String> {
    let Ok(raw_info) = redis::cmd("INFO").query::<String>(connection) else {
        return HashMap::new();
    };

    raw_info
        .lines()
        .filter_map(|line| {
            if line.starts_with('#') || line.trim().is_empty() {
                return None;
            }

            line.split_once(':')
                .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

fn read_module_names(connection: &mut Connection) -> Vec<String> {
    let Ok(raw_modules) = redis::cmd("MODULE").arg("LIST").query::<RedisValue>(connection) else {
        return Vec::new();
    };

    let RedisValue::Array(entries) = unwrap_attribute(&raw_modules) else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let row = value_pairs_to_map(entry);
            row.get("name").cloned()
        })
        .collect()
}

fn build_metrics(readonly: bool, db_size: u64, keys: &[RedisKeySummary]) -> Vec<WorkspaceMetric> {
    let namespace_count = keys
        .iter()
        .map(|item| item.namespace.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let ttl_count = keys
        .iter()
        .filter(|item| item.ttl_seconds.is_some())
        .count();

    vec![
        WorkspaceMetric {
            label: "Loaded keys".into(),
            value: keys.len().to_string(),
            detail: "Current SCAN page".into(),
            tone: "accent".into(),
        },
        WorkspaceMetric {
            label: "Namespaces".into(),
            value: namespace_count.to_string(),
            detail: format!("{db_size} total keys reported by Redis"),
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

fn build_resources(database: u8, keys: &[RedisKeySummary]) -> Vec<ResourceNode> {
    let mut groups: BTreeMap<String, Vec<&RedisKeySummary>> = BTreeMap::new();

    for key in keys {
        groups.entry(key.namespace.clone()).or_default().push(key);
    }

    let children = groups
        .into_iter()
        .map(|(namespace, grouped_keys)| ResourceNode {
            id: format!("prefix:{namespace}"),
            label: namespace.clone(),
            kind: "prefix".into(),
            meta: Some(format!("{} keys", grouped_keys.len())),
            children: Some(
                grouped_keys
                    .iter()
                    .map(|item| ResourceNode {
                        id: item.id.clone(),
                        label: item.display_name.clone(),
                        kind: item.key_type.clone(),
                        meta: Some(item.meta.clone()),
                        children: None,
                        expandable: None,
                    })
                    .collect(),
            ),
            expandable: None,
        })
        .collect::<Vec<_>>();

    vec![ResourceNode {
        id: format!("db:{database}"),
        label: format!("db{database}"),
        kind: "database".into(),
        meta: Some(format!("{} loaded keys", keys.len())),
        children: Some(children),
        expandable: None,
    }]
}

fn build_diagnostics(
    connection: &ConnectionRecord,
    keys: &[RedisKeySummary],
    has_more: bool,
    capability: &RedisCapabilitySnapshot,
) -> Vec<String> {
    let mut lines = vec![
        format!("Loaded {} keys from Redis using SCAN.", keys.len()),
        if has_more {
            "More keys are available. Continue paging to inspect the rest of the keyspace.".into()
        } else {
            "Current SCAN view reached the end of the filtered keyspace.".into()
        },
    ];

    if connection.readonly {
        lines.push("This connection is read-only. Edit, TTL and delete actions stay disabled.".into());
    } else {
        lines.push("Delete and write actions remain guarded by explicit confirmation.".into());
    }

    lines.extend(capability.diagnostics.clone());
    lines
}

fn build_info_rows(
    connection: &ConnectionRecord,
    database: u8,
    db_size: u64,
    info: &HashMap<String, String>,
    capability: &RedisCapabilitySnapshot,
) -> Vec<RedisInfoRow> {
    vec![
        RedisInfoRow {
            label: "Redis version".into(),
            value: info
                .get("redis_version")
                .cloned()
                .unwrap_or_else(|| "--".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Used memory".into(),
            value: info
                .get("used_memory_human")
                .cloned()
                .unwrap_or_else(|| "--".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Connected clients".into(),
            value: info
                .get("connected_clients")
                .cloned()
                .unwrap_or_else(|| "--".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Selected DB".into(),
            value: database.to_string(),
            secondary: capability.db_count.map(|count| format!("{count} configured DBs")),
        },
        RedisInfoRow {
            label: "Reported keys".into(),
            value: db_size.to_string(),
            secondary: None,
        },
        RedisInfoRow {
            label: "Modules".into(),
            value: if capability.module_names.is_empty() {
                "none".into()
            } else {
                capability.module_names.join(", ")
            },
            secondary: None,
        },
        RedisInfoRow {
            label: "Connection target".into(),
            value: format!("{}:{}", connection.host, connection.port),
            secondary: None,
        },
    ]
}

fn build_server_rows(info: &HashMap<String, String>) -> Vec<RedisInfoRow> {
    let role = info
        .get("role")
        .cloned()
        .unwrap_or_else(|| "standalone".into());
    let mode = if info.get("cluster_enabled").map(String::as_str) == Some("1") {
        "cluster".into()
    } else {
        info.get("redis_mode")
            .cloned()
            .unwrap_or_else(|| "unknown".into())
    };

    vec![
        RedisInfoRow {
            label: "Role".into(),
            value: role,
            secondary: Some(format!("Mode {mode}")),
        },
        RedisInfoRow {
            label: "Uptime".into(),
            value: info
                .get("uptime_in_days")
                .cloned()
                .unwrap_or_else(|| "--".into()),
            secondary: Some("days".into()),
        },
        RedisInfoRow {
            label: "Ops/sec".into(),
            value: info
                .get("instantaneous_ops_per_sec")
                .cloned()
                .unwrap_or_else(|| "--".into()),
            secondary: None,
        },
        RedisInfoRow {
            label: "Hit rate".into(),
            value: hit_rate_label(info).unwrap_or_else(|| "--".into()),
            secondary: None,
        },
    ]
}

fn build_config_rows(connection: &mut Connection) -> Vec<RedisInfoRow> {
    let config = query_optional::<Vec<String>>(connection, "CONFIG", &["GET".into(), "*".into()])
        .unwrap_or_default();
    let mut rows = Vec::new();
    let mut index = 0;

    while index + 1 < config.len() {
        let key = config[index].clone();
        let value = config[index + 1].clone();
        if matches!(
            key.as_str(),
            "databases"
                | "maxmemory"
                | "maxmemory-policy"
                | "appendonly"
                | "save"
                | "slowlog-log-slower-than"
                | "slowlog-max-len"
        ) {
            rows.push(RedisInfoRow {
                label: key,
                value,
                secondary: None,
            });
        }
        index += 2;
    }

    rows
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
    let elements = parse_string_array(value, "List values must be a JSON array.")?;
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
    let members = parse_string_array(value, "Set values must be a JSON array.")?;
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

fn create_json_key(
    connection: &mut Connection,
    key: &str,
    value: &str,
    ttl_seconds: Option<i64>,
) -> Result<(), String> {
    serde_json::from_str::<JsonValue>(value)
        .map_err(|_| "RedisJSON values must be valid JSON.".to_string())?;

    redis::cmd("JSON.SET")
        .arg(key)
        .arg("$")
        .arg(value)
        .arg("NX")
        .query::<String>(connection)
        .map_err(|error| error.to_string())?;

    if let Some(ttl_seconds) = ttl_seconds {
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(ttl_seconds)
            .query::<bool>(connection)
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

fn append_expire(pipeline: &mut redis::Pipeline, key: &str, ttl_seconds: Option<i64>) {
    if let Some(ttl_seconds) = ttl_seconds {
        pipeline.cmd("EXPIRE").arg(key).arg(ttl_seconds);
    }
}

fn set_string_value_on_connection(
    connection: &mut Connection,
    key: &str,
    value: &str,
) -> Result<(), String> {
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
    let elements = parse_string_array(value, "List values must be a JSON array.")?;
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
    let members = parse_string_array(value, "Set values must be a JSON array.")?;
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

fn replace_json_value(connection: &mut Connection, key: &str, value: &str) -> Result<(), String> {
    let ttl_before = read_ttl(connection, key)?;
    json_set_on_connection(connection, key, value)?;
    if let Some(ttl_seconds) = ttl_before.filter(|ttl| *ttl > 0) {
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(ttl_seconds)
            .query::<bool>(connection)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn parse_hash_entries(value: &str) -> Result<Vec<(String, String)>, String> {
    let parsed = serde_json::from_str::<JsonValue>(value)
        .map_err(|_| "Hash values must be a JSON object.".to_string())?;

    let JsonValue::Object(entries) = parsed else {
        return Err("Hash values must be a JSON object.".into());
    };

    if entries.is_empty() {
        return Err("Hash values cannot be empty. Delete the key instead.".into());
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
        return Err("This collection cannot be empty. Delete the key instead.".into());
    }

    Ok(items.into_iter().map(json_value_to_redis_string).collect())
}

fn parse_sorted_set_members(value: &str) -> Result<Vec<(String, f64)>, String> {
    let parsed = serde_json::from_str::<JsonValue>(value)
        .map_err(|_| "ZSet values must be a JSON array of { member, score } objects.".to_string())?;
    let JsonValue::Array(items) = parsed else {
        return Err("ZSet values must be a JSON array of { member, score } objects.".into());
    };

    if items.is_empty() {
        return Err("This collection cannot be empty. Delete the key instead.".into());
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

fn read_key_type(connection: &mut Connection, key: &str) -> Result<String, String> {
    let key_type: String = redis::cmd("TYPE")
        .arg(key)
        .query(connection)
        .map_err(|error| error.to_string())?;
    Ok(normalize_key_type(&key_type))
}

fn normalize_key_type(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.contains("rejson") || normalized == "json" {
        "json".into()
    } else if matches!(
        normalized.as_str(),
        "string" | "hash" | "list" | "set" | "zset" | "stream"
    ) {
        normalized
    } else {
        "unknown".into()
    }
}

fn normalize_type_filter(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "all" {
        "all".into()
    } else {
        normalize_key_type(&normalized)
    }
}

fn matches_type_filter(key_type: &str, filter: &str) -> bool {
    filter == "all" || key_type == filter
}

fn read_ttl(connection: &mut Connection, key: &str) -> Result<Option<i64>, String> {
    let ttl: i64 = redis::cmd("TTL")
        .arg(key)
        .query(connection)
        .map_err(|error| error.to_string())?;

    if ttl >= 0 {
        Ok(Some(ttl))
    } else {
        Ok(None)
    }
}

fn read_size(
    connection: &mut Connection,
    key: &str,
    key_type: &str,
) -> Result<Option<usize>, String> {
    if let Some(memory_usage) = query_optional::<usize>(
        connection,
        "MEMORY",
        &["USAGE".into(), key.to_string()],
    ) {
        return Ok(Some(memory_usage));
    }

    let size = match key_type {
        "string" => query_optional::<usize>(connection, "STRLEN", &[key.to_string()]),
        "hash" => query_optional::<usize>(connection, "HLEN", &[key.to_string()]),
        "list" => query_optional::<usize>(connection, "LLEN", &[key.to_string()]),
        "set" => query_optional::<usize>(connection, "SCARD", &[key.to_string()]),
        "zset" => query_optional::<usize>(connection, "ZCARD", &[key.to_string()]),
        "stream" => query_optional::<usize>(connection, "XLEN", &[key.to_string()]),
        "json" => query_optional::<usize>(connection, "JSON.STRLEN", &[key.to_string(), "$".into()]),
        _ => None,
    };

    Ok(size)
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

fn key_meta(key_type: &str, ttl_seconds: Option<i64>, size: Option<usize>) -> String {
    let ttl_label = ttl_seconds
        .map(|ttl| format!("TTL {ttl}s"))
        .unwrap_or_else(|| "No TTL".into());
    let size_label = size
        .map(|value| format!("size {value}"))
        .unwrap_or_else(|| "size --".into());
    format!("{key_type} | {ttl_label} | {size_label}")
}

fn read_slowlog_entries(connection: &mut Connection, limit: usize) -> Vec<RedisSlowlogEntry> {
    let raw: Result<RedisValue, _> = redis::cmd("SLOWLOG")
        .arg("GET")
        .arg(limit.max(1))
        .query(connection);

    raw.map(|value| parse_slowlog_entries(&value))
        .unwrap_or_default()
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
            let started_at = value_as_i64(&parts[1]).map(unix_seconds_to_iso)?;
            let duration_micros = value_as_i64(&parts[2])? as u64;
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

fn parse_stream_fields(value: &RedisValue) -> Vec<(String, String)> {
    match unwrap_attribute(value) {
        RedisValue::Array(values) => {
            let as_text = values.iter().map(value_to_text).collect::<Vec<_>>();
            let mut pairs = Vec::new();
            let mut index = 0;
            while index + 1 < as_text.len() {
                pairs.push((as_text[index].clone(), as_text[index + 1].clone()));
                index += 2;
            }
            pairs
        }
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(key, value)| (value_to_text(key), value_to_text(value)))
            .collect(),
        _ => Vec::new(),
    }
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

fn json_rows(pretty: &str) -> Vec<RedisInfoRow> {
    let Ok(parsed) = serde_json::from_str::<JsonValue>(pretty) else {
        return Vec::new();
    };

    let JsonValue::Object(entries) = parsed else {
        return Vec::new();
    };

    entries
        .into_iter()
        .map(|(label, value)| RedisInfoRow {
            label,
            value: match value {
                JsonValue::String(text) => text,
                other => serde_json::to_string(&other).unwrap_or_default(),
            },
            secondary: None,
        })
        .collect()
}

fn pretty_or_raw(raw: &str) -> String {
    match serde_json::from_str::<JsonValue>(raw) {
        Ok(json_value) => serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| raw.to_string()),
        Err(_) => raw.to_string(),
    }
}

fn pretty_json_value(raw: &str) -> Result<String, String> {
    let json_value = serde_json::from_str::<JsonValue>(raw)
        .map_err(|_| "RedisJSON values must be valid JSON.".to_string())?;
    serde_json::to_string_pretty(&json_value).map_err(|error| error.to_string())
}

fn ascii_preview(bytes: &[u8]) -> String {
    bytes.iter()
        .map(|byte| {
            if (0x20..=0x7e).contains(byte) {
                *byte as char
            } else {
                '.'
            }
        })
        .collect()
}

fn hex_preview(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn value_to_json(value: &RedisValue) -> JsonValue {
    match unwrap_attribute(value) {
        RedisValue::Nil => JsonValue::Null,
        RedisValue::Int(number) => JsonValue::Number((*number).into()),
        RedisValue::BulkString(bytes) => {
            JsonValue::String(String::from_utf8_lossy(bytes).into_owned())
        }
        RedisValue::Array(values) => JsonValue::Array(values.iter().map(value_to_json).collect()),
        RedisValue::SimpleString(text) => JsonValue::String(text.clone()),
        RedisValue::Okay => JsonValue::String("OK".into()),
        RedisValue::Map(entries) => {
            let mut object = JsonMap::new();
            for (key, value) in entries {
                object.insert(value_to_text(key), value_to_json(value));
            }
            JsonValue::Object(object)
        }
        RedisValue::Set(values) => JsonValue::Array(values.iter().map(value_to_json).collect()),
        RedisValue::Double(number) => serde_json::Number::from_f64(*number)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        RedisValue::Boolean(flag) => JsonValue::Bool(*flag),
        RedisValue::VerbatimString { text, .. } => JsonValue::String(text.clone()),
        RedisValue::BigNumber(number) => JsonValue::String(number.to_string()),
        RedisValue::Push { data, .. } => JsonValue::Array(data.iter().map(value_to_json).collect()),
        RedisValue::ServerError(error) => JsonValue::String(format!("{error:?}")),
        RedisValue::Attribute { .. } => unreachable!("attributes are unwrapped above"),
    }
}

fn value_to_table(value: &RedisValue) -> Option<RedisCommandTable> {
    match unwrap_attribute(value) {
        RedisValue::Map(entries) => Some(RedisCommandTable {
            columns: vec!["key".into(), "value".into()],
            rows: entries
                .iter()
                .map(|(key, value)| vec![value_to_text(key), value_to_text(value)])
                .collect(),
        }),
        RedisValue::Array(values) => {
            if values.iter().all(|value| matches!(unwrap_attribute(value), RedisValue::Map(_))) {
                let mut columns = BTreeSet::new();
                let rows = values
                    .iter()
                    .map(|value| value_pairs_to_map(value))
                    .collect::<Vec<_>>();
                for row in &rows {
                    for key in row.keys() {
                        columns.insert(key.clone());
                    }
                }
                let columns = columns.into_iter().collect::<Vec<_>>();
                return Some(RedisCommandTable {
                    rows: rows
                        .iter()
                        .map(|row| {
                            columns
                                .iter()
                                .map(|column| row.get(column).cloned().unwrap_or_default())
                                .collect()
                        })
                        .collect(),
                    columns,
                });
            }

            Some(RedisCommandTable {
                columns: vec!["value".into()],
                rows: values.iter().map(|value| vec![value_to_text(value)]).collect(),
            })
        }
        _ => None,
    }
}

fn value_pairs_to_map(value: &RedisValue) -> BTreeMap<String, String> {
    match unwrap_attribute(value) {
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(key, value)| (value_to_text(key), value_to_text(value)))
            .collect(),
        RedisValue::Array(entries) => {
            let values = entries.iter().map(value_to_text).collect::<Vec<_>>();
            let mut map = BTreeMap::new();
            let mut index = 0;
            while index + 1 < values.len() {
                map.insert(values[index].clone(), values[index + 1].clone());
                index += 2;
            }
            map
        }
        _ => BTreeMap::new(),
    }
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
        RedisValue::Push { data, .. } => data.iter().map(value_to_text).collect::<Vec<_>>().join(" "),
        RedisValue::ServerError(error) => format!("{error:?}"),
        RedisValue::Attribute { .. } => unreachable!("attributes are unwrapped above"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ConnectionRecord;

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
    fn cursor_roundtrip_keeps_buffered_keys() {
        let state = BrowseCursorState {
            scan_cursor: 48,
            buffered_keys: vec!["session:2".into(), "session:3".into()],
        };
        let encoded = encode_cursor(&state);
        let decoded = decode_cursor(Some(encoded));

        assert_eq!(decoded.scan_cursor, 48);
        assert_eq!(decoded.buffered_keys.len(), 2);
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

        assert_eq!(entries.len(), 1);
        assert_eq!(rows[0].label, "1735738000000-0");
        assert!(rows[0].value.contains("orderId=ord_482901"));
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
    fn command_parser_supports_quotes() {
        let tokens = parse_command(r#"HSET user:1 "first name" "Ada Lovelace""#).expect("tokens");
        assert_eq!(tokens[0], "HSET");
        assert_eq!(tokens[2], "first name");
        assert_eq!(tokens[3], "Ada Lovelace");
    }

    #[test]
    fn read_only_mode_blocks_write_commands() {
        let connection = test_connection(true);
        assert!(is_write_command("SET"));
        assert!(connection.readonly);
    }
}
