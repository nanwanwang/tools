use std::time::Instant;

use base64::prelude::*;
use chrono::Utc;
use serde_json::{Map, Number, Value};
use taos::sync::*;
use url::Url;

use crate::models::{
    ConnectionHealth, ConnectionRecord, RedisInfoRow, ResourceNode, TdengineField,
    TdengineObjectDetail, TdengineQueryColumn, TdengineQueryResult,
};

struct SqlInspection {
    statement: String,
    verb: String,
    database: Option<String>,
    has_limit: bool,
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn strip_comments(sql: &str) -> String {
    let mut output = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        if index + 1 < bytes.len() && bytes[index] == b'/' && bytes[index + 1] == b'*' {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
            {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }

        if index + 1 < bytes.len() && bytes[index] == b'-' && bytes[index + 1] == b'-' {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }

        if bytes[index] == b'#' {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }

        output.push(bytes[index] as char);
        index += 1;
    }

    output.trim().to_string()
}

fn split_statements(sql: &str) -> Vec<String> {
    strip_comments(sql)
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn extract_use_database(statement: &str) -> Option<String> {
    let mut parts = statement.trim().split_whitespace();
    let verb = parts.next()?;
    if !verb.eq_ignore_ascii_case("use") {
        return None;
    }

    let database = parts.next()?.trim().trim_end_matches(';');
    Some(database.trim_matches('`').to_string())
}

fn inspect_sql(sql: &str) -> Result<SqlInspection, String> {
    let statements = split_statements(sql);
    if statements.is_empty() {
        return Err("SQL cannot be empty.".into());
    }

    if statements.len() > 1 {
        return Err("Only one SQL statement can be executed at a time.".into());
    }

    let statement = statements[0].clone();
    let verb = statement
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_uppercase();

    if verb.is_empty() {
        return Err("The SQL statement could not be parsed.".into());
    }

    let allowed = ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "USE"];
    if !allowed.contains(&verb.as_str()) {
        return Err(format!(
            "{verb} is blocked in the TDengine query workspace."
        ));
    }

    Ok(SqlInspection {
        has_limit: statement.to_lowercase().contains(" limit "),
        database: extract_use_database(&statement),
        statement,
        verb,
    })
}

fn encode_auth_part(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn build_dsn(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database_override: Option<&str>,
) -> Result<String, String> {
    let scheme = match connection.protocol.as_str() {
        "native" => "taos",
        _ => {
            if connection.use_tls {
                "taoswss"
            } else {
                "taosws"
            }
        }
    };

    let username = encode_auth_part(if connection.username.is_empty() {
        "root"
    } else {
        &connection.username
    });
    let password = encode_auth_part(secret.as_deref().unwrap_or(""));
    let host = if connection.host.trim().is_empty() {
        "127.0.0.1"
    } else {
        connection.host.trim()
    };

    let mut url = Url::parse(&format!(
        "{scheme}://{username}:{password}@{host}:{}",
        connection.port
    ))
    .map_err(|error| error.to_string())?;

    let database = match database_override {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => Some(value.trim()),
        None => {
            let configured = connection.database_name.trim();
            if configured.is_empty() {
                None
            } else {
                Some(configured)
            }
        }
    };

    if let Some(database_name) = database {
        url.set_path(&format!("/{}", database_name));
    }

    Ok(url.to_string())
}

fn connection_error_message(error: impl std::fmt::Display, protocol: &str) -> String {
    let message = error.to_string();
    let lowercase = message.to_lowercase();

    if protocol == "native"
        && (lowercase.contains("libtaos")
            || lowercase.contains("taos.dll")
            || lowercase.contains("load library")
            || lowercase.contains("client library"))
    {
        return "TDengine native mode requires a matching TDengine client installation.".into();
    }

    message
}

fn open_connection(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database_override: Option<&str>,
) -> Result<Taos, String> {
    let dsn = build_dsn(connection, secret, database_override)?;
    let builder = TaosBuilder::from_dsn(&dsn)
        .map_err(|error| connection_error_message(error, &connection.protocol))?;

    builder
        .build()
        .map_err(|error| connection_error_message(error, &connection.protocol))
}

fn catalog_node(
    id: String,
    label: String,
    kind: &str,
    meta: Option<String>,
    expandable: bool,
) -> ResourceNode {
    ResourceNode {
        id,
        label,
        kind: kind.to_string(),
        meta,
        children: None,
        expandable: Some(expandable),
    }
}

fn tdengine_node_id(
    database: &str,
    object_kind: &str,
    object_name: Option<&str>,
    supertable: Option<&str>,
) -> String {
    let mut parts = vec!["td".to_string(), object_kind.to_string(), database.to_string()];

    if object_kind == "child-table" {
        if let Some(parent) = supertable {
            parts.push(parent.to_string());
        }
    }

    if let Some(name) = object_name {
        parts.push(name.to_string());
    }

    parts
        .into_iter()
        .map(|part| url::form_urlencoded::byte_serialize(part.as_bytes()).collect::<String>())
        .collect::<Vec<_>>()
        .join("|")
}

fn borrowed_value_to_json(value: BorrowedValue<'_>) -> Value {
    match value {
        BorrowedValue::Null(_) => Value::Null,
        BorrowedValue::Bool(value) => Value::Bool(value),
        BorrowedValue::TinyInt(value) => Value::Number(Number::from(value)),
        BorrowedValue::SmallInt(value) => Value::Number(Number::from(value)),
        BorrowedValue::Int(value) => Value::Number(Number::from(value)),
        BorrowedValue::BigInt(value) => Value::Number(Number::from(value)),
        BorrowedValue::Float(value) => Number::from_f64(value as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        BorrowedValue::Double(value) => Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        BorrowedValue::VarChar(value) => Value::String(value.to_string()),
        BorrowedValue::Timestamp(value) => Value::String(value.to_datetime_with_tz().to_rfc3339()),
        BorrowedValue::NChar(value) => Value::String(value.to_string()),
        BorrowedValue::UTinyInt(value) => Value::Number(Number::from(value)),
        BorrowedValue::USmallInt(value) => Value::Number(Number::from(value)),
        BorrowedValue::UInt(value) => Value::Number(Number::from(value)),
        BorrowedValue::UBigInt(value) => Value::Number(Number::from(value)),
        BorrowedValue::Json(value) => serde_json::from_slice(value.as_ref())
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(value.as_ref()).into_owned())),
        BorrowedValue::VarBinary(value) => {
            Value::String(BASE64_STANDARD.encode(value.as_ref()))
        }
        BorrowedValue::Decimal(value) => Value::String(value.to_string()),
        BorrowedValue::Blob(value) => Value::String(BASE64_STANDARD.encode(value)),
        BorrowedValue::MediumBlob(value) => Value::String(BASE64_STANDARD.encode(value)),
        BorrowedValue::Geometry(value) => {
            Value::String(BASE64_STANDARD.encode(value.as_ref()))
        }
    }
}

fn fetch_rows<F: Fetchable>(
    result: &mut F,
    max_rows: Option<usize>,
) -> Result<(Vec<TdengineQueryColumn>, Vec<Map<String, Value>>, bool), String> {
    let columns = result
        .fields()
        .iter()
        .map(|field| TdengineQueryColumn {
            name: field.name().to_string(),
            column_type: field.ty().name().to_string(),
        })
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut truncated = false;

    for row in result.rows() {
        let row = row.map_err(|error| error.to_string())?;
        if let Some(limit) = max_rows {
            if rows.len() >= limit {
                truncated = true;
                break;
            }
        }

        let mut map = Map::new();
        for (name, value) in row {
            map.insert(name.to_string(), borrowed_value_to_json(value));
        }
        rows.push(map);
    }

    Ok((columns, rows, truncated))
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Array(value) => Some(Value::Array(value.clone()).to_string()),
        Value::Object(value) => Some(Value::Object(value.clone()).to_string()),
    }
}

fn first_string_column(row: &Map<String, Value>) -> Option<String> {
    row.values().find_map(value_as_string)
}

fn describe_fields(rows: &[Map<String, Value>]) -> (Vec<TdengineField>, Vec<TdengineField>) {
    let mut fields = Vec::new();
    let mut tags = Vec::new();

    for row in rows {
        let name = row
            .get("Field")
            .or_else(|| row.get("field"))
            .and_then(value_as_string)
            .unwrap_or_default();
        if name.is_empty() {
            continue;
        }

        let note = row
            .get("Note")
            .or_else(|| row.get("note"))
            .and_then(value_as_string);
        let field = TdengineField {
            name,
            field_type: row
                .get("Type")
                .or_else(|| row.get("type"))
                .and_then(value_as_string)
                .unwrap_or_default(),
            length: row
                .get("Length")
                .or_else(|| row.get("length"))
                .and_then(|value| value.as_u64())
                .map(|value| value as u32),
            note: note.clone(),
        };

        if note
            .as_deref()
            .unwrap_or_default()
            .to_ascii_uppercase()
            .contains("TAG")
        {
            tags.push(field);
        } else {
            fields.push(field);
        }
    }

    (fields, tags)
}

fn preview_sql(database: &str, object_name: &str, fields: &[TdengineField]) -> String {
    let target = format!(
        "{}.{}",
        quote_identifier(database),
        quote_identifier(object_name)
    );
    if let Some(time_field) = fields
        .iter()
        .find(|field| field.field_type.eq_ignore_ascii_case("TIMESTAMP"))
    {
        return format!(
            "select * from {target} order by {} desc limit 200",
            quote_identifier(&time_field.name)
        );
    }

    format!("select * from {target} limit 200")
}

fn preview_meta_row(fields: &[TdengineField]) -> RedisInfoRow {
    let secondary = if fields
        .iter()
        .any(|field| field.field_type.eq_ignore_ascii_case("TIMESTAMP"))
    {
        Some("Ordered by timestamp desc when available.".into())
    } else {
        Some("Fallback to LIMIT 200.".into())
    };

    RedisInfoRow {
        label: "Preview".into(),
        value: "Latest 200 rows".into(),
        secondary,
    }
}

fn split_sql_arguments(input: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut depth = 0usize;
    let chars = input.chars().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let ch = chars[index];

        match ch {
            '\'' if !in_double => {
                if in_single && index + 1 < chars.len() && chars[index + 1] == '\'' {
                    current.push(ch);
                    current.push(chars[index + 1]);
                    index += 1;
                } else {
                    in_single = !in_single;
                    current.push(ch);
                }
            }
            '"' if !in_single => {
                in_double = !in_double;
                current.push(ch);
            }
            '(' if !in_single && !in_double => {
                depth += 1;
                current.push(ch);
            }
            ')' if !in_single && !in_double && depth > 0 => {
                depth -= 1;
                current.push(ch);
            }
            ',' if !in_single && !in_double && depth == 0 => {
                let value = current.trim();
                if !value.is_empty() {
                    values.push(value.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }

        index += 1;
    }

    let value = current.trim();
    if !value.is_empty() {
        values.push(value.to_string());
    }

    values
}

fn unquote_sql_token(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let starts = trimmed.chars().next().unwrap_or_default();
        let ends = trimmed.chars().last().unwrap_or_default();
        if (starts == '\'' && ends == '\'')
            || (starts == '"' && ends == '"')
            || (starts == '`' && ends == '`')
        {
            return trimmed[1..trimmed.len() - 1].replace("''", "'");
        }
    }

    trimmed.to_string()
}

fn parse_child_table_context(ddl: &str) -> Option<(String, Vec<String>)> {
    let uppercase = ddl.to_ascii_uppercase();
    let using_index = uppercase.find(" USING ")?;
    let tags_index = uppercase[using_index + 7..].find(" TAGS")?;
    let using_segment = ddl[using_index + 7..using_index + 7 + tags_index].trim();
    let supertable = unquote_sql_token(using_segment.rsplit('.').next().unwrap_or(using_segment));

    let tags_open = uppercase.find(" TAGS (")?;
    let values_segment = ddl[tags_open + 7..].trim();
    let values_end = values_segment.rfind(')')?;
    let raw_values = &values_segment[..values_end];

    Some((
        supertable,
        split_sql_arguments(raw_values)
            .into_iter()
            .map(|value| unquote_sql_token(&value))
            .collect(),
    ))
}

fn child_table_count(
    taos: &Taos,
    connection: &ConnectionRecord,
    database: &str,
    object_name: &str,
) -> Option<usize> {
    let sql = format!(
        "select count(*) as child_count from information_schema.ins_tables where db_name = '{}' and stable_name = '{}'",
        database.replace('\'', "''"),
        object_name.replace('\'', "''"),
    );
    let mut result = taos
        .query(sql)
        .map_err(|error| connection_error_message(error, &connection.protocol))
        .ok()?;
    let (_, rows, _) = fetch_rows(&mut result, Some(1)).ok()?;
    let row = rows.into_iter().next()?;

    row.get("child_count")
        .and_then(|value| value.as_u64())
        .map(|value| value as usize)
        .or_else(|| first_string_column(&row)?.parse::<usize>().ok())
}

pub fn load_catalog(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: Option<String>,
    supertable: Option<String>,
) -> Result<Vec<ResourceNode>, String> {
    let database_name = database.unwrap_or_default();
    let taos = open_connection(
        connection,
        secret,
        if database_name.is_empty() {
            Some("")
        } else {
            Some(database_name.as_str())
        },
    )?;

    if database_name.is_empty() {
        let mut result = taos
            .query("show databases")
            .map_err(|error| connection_error_message(error, &connection.protocol))?;
        let (_, rows, _) = fetch_rows(&mut result, None)?;
        return Ok(rows
            .into_iter()
            .filter_map(|row| {
                first_string_column(&row).map(|name| {
                    catalog_node(
                        tdengine_node_id(&name, "database", None, None),
                        name,
                        "database",
                        None,
                        true,
                    )
                })
            })
            .collect());
    }

    if let Some(parent_stable) = supertable {
        let sql = format!(
            "select table_name from information_schema.ins_tables where db_name = '{}' and stable_name = '{}' order by table_name",
            database_name.replace('\'', "''"),
            parent_stable.replace('\'', "''")
        );
        let mut result = taos
            .query(sql)
            .map_err(|error| connection_error_message(error, &connection.protocol))?;
        let (_, rows, _) = fetch_rows(&mut result, None)?;
        return Ok(rows
            .into_iter()
            .filter_map(|row| {
                first_string_column(&row).map(|name| {
                    catalog_node(
                        tdengine_node_id(&database_name, "child-table", Some(&name), Some(&parent_stable)),
                        name,
                        "child-table",
                        None,
                        false,
                    )
                })
            })
            .collect());
    }

    let mut stables_result = taos
        .query("show stables")
        .map_err(|error| connection_error_message(error, &connection.protocol))?;
    let (_, stable_rows, _) = fetch_rows(&mut stables_result, None)?;
    let mut nodes = stable_rows
        .into_iter()
        .filter_map(|row| {
            first_string_column(&row).map(|name| {
                catalog_node(
                    tdengine_node_id(&database_name, "supertable", Some(&name), None),
                    name,
                    "supertable",
                    None,
                    true,
                )
            })
        })
        .collect::<Vec<_>>();

    let mut tables_result = taos
        .query("show normal tables")
        .map_err(|error| connection_error_message(error, &connection.protocol))?;
    let (_, table_rows, _) = fetch_rows(&mut tables_result, None)?;
    nodes.extend(table_rows.into_iter().filter_map(|row| {
        first_string_column(&row).map(|name| {
            catalog_node(
                tdengine_node_id(&database_name, "table", Some(&name), None),
                name,
                "table",
                None,
                false,
            )
        })
    }));

    Ok(nodes)
}

pub fn load_object_detail(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: &str,
    object_name: &str,
    object_kind: &str,
) -> Result<TdengineObjectDetail, String> {
    let taos = open_connection(connection, secret, Some(database))?;
    let target = format!(
        "{}.{}",
        quote_identifier(database),
        quote_identifier(object_name)
    );
    let describe_sql = format!("describe {target}");
    let mut describe_result = taos
        .query(&describe_sql)
        .map_err(|error| connection_error_message(error, &connection.protocol))?;
    let (_, describe_rows, _) = fetch_rows(&mut describe_result, None)?;
    let (fields, tag_columns) = describe_fields(&describe_rows);

    let show_create_sql = if object_kind == "supertable" {
        format!("show create stable {target}")
    } else {
        format!("show create table {target}")
    };
    let ddl = taos
        .query(&show_create_sql)
        .ok()
        .and_then(|mut result| fetch_rows(&mut result, Some(1)).ok())
        .and_then(|(_, rows, _)| rows.into_iter().next())
        .and_then(|row| row.values().filter_map(value_as_string).last());
    let preview_meta = preview_meta_row(&fields);
    let child_count = if object_kind == "supertable" {
        child_table_count(&taos, connection, database, object_name)
    } else {
        None
    };
    let child_context = if object_kind == "child-table" {
        ddl.as_deref().and_then(parse_child_table_context)
    } else {
        None
    };
    let tag_value_rows = child_context
        .as_ref()
        .map(|(_, values)| {
            tag_columns
                .iter()
                .zip(values.iter())
                .map(|(field, value)| RedisInfoRow {
                    label: field.name.clone(),
                    value: value.clone(),
                    secondary: Some(field.field_type.clone()),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut meta_rows = vec![
        RedisInfoRow {
            label: "Type".into(),
            value: object_kind.to_string(),
            secondary: None,
        },
        RedisInfoRow {
            label: "Database".into(),
            value: database.to_string(),
            secondary: None,
        },
        RedisInfoRow {
            label: "Columns".into(),
            value: fields.len().to_string(),
            secondary: None,
        },
        RedisInfoRow {
            label: "Tag Columns".into(),
            value: tag_columns.len().to_string(),
            secondary: None,
        },
        preview_meta,
    ];

    if let Some(count) = child_count {
        meta_rows.push(RedisInfoRow {
            label: "Child Tables".into(),
            value: count.to_string(),
            secondary: None,
        });
    }

    if let Some((parent, _)) = child_context.as_ref() {
        meta_rows.push(RedisInfoRow {
            label: "Supertable".into(),
            value: parent.clone(),
            secondary: None,
        });
    }

    if !tag_value_rows.is_empty() {
        meta_rows.push(RedisInfoRow {
            label: "Tag Values".into(),
            value: tag_value_rows.len().to_string(),
            secondary: Some("Parsed from CREATE TABLE".into()),
        });
    }

    Ok(TdengineObjectDetail {
        database: database.to_string(),
        object_name: object_name.to_string(),
        object_kind: object_kind.to_string(),
        preview_sql: preview_sql(database, object_name, &fields),
        fields,
        tag_columns,
        tag_value_rows,
        ddl,
        meta_rows,
    })
}

pub fn execute_query(
    connection: &ConnectionRecord,
    secret: Option<String>,
    database: &str,
    sql: &str,
    max_rows: usize,
) -> Result<TdengineQueryResult, String> {
    let inspected = inspect_sql(sql)?;
    let current_database = if database.trim().is_empty() {
        connection.database_name.as_str()
    } else {
        database.trim()
    };

    if inspected.verb == "USE" {
        let next_database = inspected.database.unwrap_or_default();
        let taos = open_connection(connection, secret, None)?;
        taos.query(format!("use {}", quote_identifier(&next_database)))
            .map_err(|error| connection_error_message(error, &connection.protocol))?;
        return Ok(TdengineQueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            duration_ms: 0,
            truncated: false,
            database: next_database,
            error: None,
        });
    }

    let taos = open_connection(
        connection,
        secret,
        if current_database.trim().is_empty() {
            None
        } else {
            Some(current_database)
        },
    )?;

    let started = Instant::now();
    let mut result = taos
        .query(&inspected.statement)
        .map_err(|error| connection_error_message(error, &connection.protocol))?;
    let (columns, rows, truncated) = fetch_rows(
        &mut result,
        if inspected.has_limit {
            None
        } else {
            Some(max_rows)
        },
    )?;

    Ok(TdengineQueryResult {
        row_count: rows.len(),
        duration_ms: started.elapsed().as_millis() as u64,
        truncated,
        database: current_database.to_string(),
        error: None,
        columns,
        rows,
    })
}

pub fn run_health_check(
    connection: &ConnectionRecord,
    secret: Option<String>,
) -> ConnectionHealth {
    let checked_at = Utc::now().to_rfc3339();
    let target = format!("{}:{}", connection.host, connection.port);
    let started = Instant::now();
    let mut details = vec![format!(
        "TDengine {} target {target}.",
        if connection.protocol == "native" {
            "native"
        } else {
            "WebSocket"
        }
    )];

    match open_connection(connection, secret, Some("")) {
        Ok(taos) => match taos.server_version() {
            Ok(version) => {
                let latency_ms = started.elapsed().as_millis() as u64;
                details.push(format!("Server version {version}."));
                if connection.protocol == "native" {
                    details.push(
                        "Native mode depends on a matching TDengine client installation."
                            .into(),
                    );
                } else {
                    details.push("WebSocket handshake completed through the TDengine adapter.".into());
                }

                ConnectionHealth {
                    status: if connection.environment == "production" {
                        "degraded".into()
                    } else {
                        "healthy".into()
                    },
                    summary: if connection.environment == "production" {
                        "TDengine responded. Production safeguards stay enabled.".into()
                    } else {
                        "TDengine connection succeeded.".into()
                    },
                    details,
                    latency_ms: Some(latency_ms),
                    checked_at,
                }
            }
            Err(error) => ConnectionHealth {
                status: "unreachable".into(),
                summary: "TDengine handshake failed after connection setup.".into(),
                details: vec![connection_error_message(error, &connection.protocol)],
                latency_ms: None,
                checked_at,
            },
        },
        Err(error) => ConnectionHealth {
            status: "unreachable".into(),
            summary: "TDengine connection failed.".into(),
            details: vec![error],
            latency_ms: None,
            checked_at,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_dsn, extract_use_database, inspect_sql, parse_child_table_context,
        preview_meta_row, preview_sql,
    };
    use crate::models::ConnectionRecord;

    fn connection(protocol: &str) -> ConnectionRecord {
        ConnectionRecord {
            id: "td-1".into(),
            kind: "tdengine".into(),
            protocol: protocol.into(),
            name: "td".into(),
            host: "127.0.0.1".into(),
            port: if protocol == "native" { 6030 } else { 6041 },
            database_name: "power".into(),
            username: "root".into(),
            auth_mode: "password".into(),
            environment: "dev".into(),
            tags: Vec::new(),
            readonly: true,
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
            created_at: "2026-04-18T10:00:00.000Z".into(),
            updated_at: "2026-04-18T10:00:00.000Z".into(),
        }
    }

    #[test]
    fn builds_protocol_specific_dsn() {
        let ws = build_dsn(&connection("ws"), Some("secret".into()), None).unwrap();
        let native = build_dsn(&connection("native"), Some("secret".into()), Some("ops")).unwrap();

        assert!(ws.starts_with("taosws://"));
        assert!(ws.contains(":6041/"));
        assert!(native.starts_with("taos://"));
        assert!(native.contains("/ops"));
    }

    #[test]
    fn blocks_write_sql() {
        assert!(inspect_sql("select * from power.meter_events").is_ok());
        assert!(inspect_sql("use `power`").is_ok());
        assert!(inspect_sql("insert into power.meter_events values(now, 'x', 'y', 1)").is_err());
        assert!(inspect_sql("select 1; show databases").is_err());
    }

    #[test]
    fn extracts_use_database_and_preview_sql() {
        assert_eq!(extract_use_database("use `metrics`"), Some("metrics".into()));
        assert!(preview_sql(
            "power",
            "meter_events",
            &[crate::models::TdengineField {
                name: "ts".into(),
                field_type: "TIMESTAMP".into(),
                length: Some(8),
                note: None,
            }]
        )
        .contains("order by"));
    }

    #[test]
    fn parses_child_table_context_from_ddl() {
        let context = parse_child_table_context(
            "CREATE TABLE `power`.`d1001` USING `power`.`meters` TAGS (1, 'Shanghai-A')",
        )
        .unwrap();

        assert_eq!(context.0, "meters");
        assert_eq!(context.1, vec!["1".to_string(), "Shanghai-A".to_string()]);
    }

    #[test]
    fn preview_meta_uses_timestamp_hint() {
        let row = preview_meta_row(&[crate::models::TdengineField {
            name: "ts".into(),
            field_type: "TIMESTAMP".into(),
            length: Some(8),
            note: None,
        }]);

        assert_eq!(row.label, "Preview");
        assert_eq!(row.value, "Latest 200 rows");
        assert!(row.secondary.unwrap_or_default().contains("timestamp"));
    }
}
