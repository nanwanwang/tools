use std::fs;
use std::path::PathBuf;

use chrono::Utc;
use keyring::{Entry, Error as KeyringError};
use rusqlite::{params, Connection, OptionalExtension};
use tauri::{AppHandle, Manager};
use uuid::Uuid;

use crate::models::{ConnectionInput, ConnectionRecord};

const DB_FILE: &str = "middleware-studio.db";
const KEYCHAIN_SERVICE: &str = "middleware-studio";

pub struct Store {
    db_path: PathBuf,
}

impl Store {
    pub fn new(app: &AppHandle) -> Result<Self, String> {
        let app_dir = app
            .path()
            .app_data_dir()
            .map_err(|error| error.to_string())?;
        fs::create_dir_all(&app_dir).map_err(|error| error.to_string())?;

        let store = Self {
            db_path: app_dir.join(DB_FILE),
        };
        store.init()?;
        Ok(store)
    }

    fn connect(&self) -> Result<Connection, String> {
        Connection::open(&self.db_path).map_err(|error| error.to_string())
    }

    fn init(&self) -> Result<(), String> {
        let connection = self.connect()?;
        connection
            .execute_batch(
                "
                create table if not exists connections (
                    id text primary key,
                    kind text not null,
                    protocol text not null default '',
                    name text not null,
                    host text not null,
                    port integer not null,
                    database_name text not null,
                    username text not null,
                    auth_mode text not null,
                    environment text not null,
                    tags_json text not null,
                    readonly integer not null,
                    favorite integer not null,
                    use_tls integer not null,
                    tls_verify integer not null,
                    ssh_enabled integer not null,
                    ssh_host text not null,
                    ssh_port integer not null,
                    ssh_username text not null,
                    schema_registry_url text not null,
                    group_id text not null,
                    client_id text not null,
                    notes text not null,
                    last_checked_at text,
                    last_connected_at text,
                    created_at text not null,
                    updated_at text not null
                );
                ",
            )
            .map_err(|error| error.to_string())?;

        ensure_protocol_column(&connection)
    }

    pub fn list_connections(&self) -> Result<Vec<ConnectionRecord>, String> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare(
                "
                select
                    id, kind, protocol, name, host, port, database_name, username, auth_mode,
                    environment, tags_json, readonly, favorite, use_tls, tls_verify,
                    ssh_enabled, ssh_host, ssh_port, ssh_username, schema_registry_url,
                    group_id, client_id, notes, last_checked_at, last_connected_at,
                    created_at, updated_at
                from connections
                order by favorite desc, coalesce(last_connected_at, updated_at) desc, updated_at desc
                ",
            )
            .map_err(|error| error.to_string())?;

        let rows = statement
            .query_map([], Self::map_connection)
            .map_err(|error| error.to_string())?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row.map_err(|error| error.to_string())?);
        }

        Ok(items)
    }

    pub fn get_connection(&self, id: &str) -> Result<ConnectionRecord, String> {
        let connection = self.connect()?;
        connection
            .query_row(
                "
                select
                    id, kind, protocol, name, host, port, database_name, username, auth_mode,
                    environment, tags_json, readonly, favorite, use_tls, tls_verify,
                    ssh_enabled, ssh_host, ssh_port, ssh_username, schema_registry_url,
                    group_id, client_id, notes, last_checked_at, last_connected_at,
                    created_at, updated_at
                from connections
                where id = ?1
                ",
                [id],
                Self::map_connection,
            )
            .optional()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Connection not found.".to_string())
    }

    pub fn get_secret(&self, id: &str) -> Result<Option<String>, String> {
        let entry = Entry::new(KEYCHAIN_SERVICE, &format!("connection:{id}"))
            .map_err(|error| error.to_string())?;

        match entry.get_password() {
            Ok(secret) => Ok(Some(secret)),
            Err(KeyringError::NoEntry) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn save_connection(
        &self,
        input: ConnectionInput,
        secret: Option<String>,
    ) -> Result<ConnectionRecord, String> {
        let connection = self.connect()?;
        let existing = input
            .id
            .as_deref()
            .map(|id| self.get_connection(id))
            .transpose()?;

        let id = input.id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let protocol = if input.kind == "tdengine" {
            input.protocol.trim().to_string()
        } else {
            String::new()
        };
        let now = Utc::now().to_rfc3339();
        let created_at = existing
            .as_ref()
            .map(|record| record.created_at.clone())
            .unwrap_or_else(|| now.clone());
        let favorite = existing
            .as_ref()
            .map(|record| record.favorite)
            .unwrap_or(false);
        let last_checked_at = existing
            .as_ref()
            .and_then(|record| record.last_checked_at.clone());
        let last_connected_at = existing
            .as_ref()
            .and_then(|record| record.last_connected_at.clone());
        let tags_json = serde_json::to_string(&input.tags).map_err(|error| error.to_string())?;

        connection
            .execute(
                "
                insert into connections (
                    id, kind, protocol, name, host, port, database_name, username, auth_mode,
                    environment, tags_json, readonly, favorite, use_tls, tls_verify, ssh_enabled,
                    ssh_host, ssh_port, ssh_username, schema_registry_url, group_id, client_id,
                    notes, last_checked_at, last_connected_at, created_at, updated_at
                ) values (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                    ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                    ?18, ?19, ?20, ?21, ?22, ?23,
                    ?24, ?25, ?26, ?27
                )
                on conflict(id) do update set
                    kind = excluded.kind,
                    protocol = excluded.protocol,
                    name = excluded.name,
                    host = excluded.host,
                    port = excluded.port,
                    database_name = excluded.database_name,
                    username = excluded.username,
                    auth_mode = excluded.auth_mode,
                    environment = excluded.environment,
                    tags_json = excluded.tags_json,
                    readonly = excluded.readonly,
                    favorite = excluded.favorite,
                    use_tls = excluded.use_tls,
                    tls_verify = excluded.tls_verify,
                    ssh_enabled = excluded.ssh_enabled,
                    ssh_host = excluded.ssh_host,
                    ssh_port = excluded.ssh_port,
                    ssh_username = excluded.ssh_username,
                    schema_registry_url = excluded.schema_registry_url,
                    group_id = excluded.group_id,
                    client_id = excluded.client_id,
                    notes = excluded.notes,
                    last_checked_at = excluded.last_checked_at,
                    last_connected_at = excluded.last_connected_at,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at
                ",
                params![
                    id,
                    input.kind,
                    protocol,
                    input.name,
                    input.host,
                    input.port,
                    input.database_name,
                    input.username,
                    input.auth_mode,
                    input.environment,
                    tags_json,
                    bool_to_int(input.readonly),
                    bool_to_int(favorite),
                    bool_to_int(input.use_tls),
                    bool_to_int(input.tls_verify),
                    bool_to_int(input.ssh_enabled),
                    input.ssh_host,
                    input.ssh_port,
                    input.ssh_username,
                    input.schema_registry_url,
                    input.group_id,
                    input.client_id,
                    input.notes,
                    last_checked_at,
                    last_connected_at,
                    created_at,
                    now
                ],
            )
            .map_err(|error| error.to_string())?;

        if let Some(secret_value) = secret.filter(|value| !value.trim().is_empty()) {
            let entry = Entry::new(KEYCHAIN_SERVICE, &format!("connection:{id}"))
                .map_err(|error| error.to_string())?;
            entry
                .set_password(secret_value.as_str())
                .map_err(|error| error.to_string())?;
        }

        self.get_connection(&id)
    }

    pub fn delete_connection(&self, id: &str) -> Result<(), String> {
        let connection = self.connect()?;
        connection
            .execute("delete from connections where id = ?1", [id])
            .map_err(|error| error.to_string())?;

        if let Ok(entry) = Entry::new(KEYCHAIN_SERVICE, &format!("connection:{id}")) {
            let _ = entry.delete_credential();
        }

        Ok(())
    }

    pub fn toggle_favorite(&self, id: &str, favorite: bool) -> Result<ConnectionRecord, String> {
        let connection = self.connect()?;
        let now = Utc::now().to_rfc3339();
        connection
            .execute(
                "update connections set favorite = ?2, updated_at = ?3 where id = ?1",
                params![id, bool_to_int(favorite), now],
            )
            .map_err(|error| error.to_string())?;
        self.get_connection(id)
    }

    pub fn touch_connection(&self, id: &str) -> Result<(), String> {
        let connection = self.connect()?;
        let now = Utc::now().to_rfc3339();
        connection
            .execute(
                "update connections set last_connected_at = ?2, updated_at = ?2 where id = ?1",
                params![id, now],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn update_last_checked(&self, id: &str, checked_at: &str) -> Result<(), String> {
        let connection = self.connect()?;
        connection
            .execute(
                "update connections set last_checked_at = ?2, updated_at = ?2 where id = ?1",
                params![id, checked_at],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn map_connection(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConnectionRecord> {
        let tags_json: String = row.get(10)?;
        let tags = serde_json::from_str(&tags_json).unwrap_or_default();

        Ok(ConnectionRecord {
            id: row.get(0)?,
            kind: row.get(1)?,
            protocol: row.get(2)?,
            name: row.get(3)?,
            host: row.get(4)?,
            port: row.get(5)?,
            database_name: row.get(6)?,
            username: row.get(7)?,
            auth_mode: row.get(8)?,
            environment: row.get(9)?,
            tags,
            readonly: row.get::<_, i64>(11)? == 1,
            favorite: row.get::<_, i64>(12)? == 1,
            use_tls: row.get::<_, i64>(13)? == 1,
            tls_verify: row.get::<_, i64>(14)? == 1,
            ssh_enabled: row.get::<_, i64>(15)? == 1,
            ssh_host: row.get(16)?,
            ssh_port: row.get(17)?,
            ssh_username: row.get(18)?,
            schema_registry_url: row.get(19)?,
            group_id: row.get(20)?,
            client_id: row.get(21)?,
            notes: row.get(22)?,
            last_checked_at: row.get(23)?,
            last_connected_at: row.get(24)?,
            created_at: row.get(25)?,
            updated_at: row.get(26)?,
        })
    }
}

fn bool_to_int(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn ensure_protocol_column(connection: &Connection) -> Result<(), String> {
    let mut statement = connection
        .prepare("pragma table_info(connections)")
        .map_err(|error| error.to_string())?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| error.to_string())?;

    let mut has_protocol = false;
    for column in columns {
        if column.map_err(|error| error.to_string())? == "protocol" {
            has_protocol = true;
            break;
        }
    }

    if !has_protocol {
        connection
            .execute(
                "alter table connections add column protocol text not null default ''",
                [],
            )
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::ensure_protocol_column;

    #[test]
    fn adds_protocol_column_to_legacy_schema() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "
                create table connections (
                    id text primary key,
                    kind text not null,
                    name text not null
                );
                ",
            )
            .unwrap();

        ensure_protocol_column(&connection).unwrap();

        let mut statement = connection.prepare("pragma table_info(connections)").unwrap();
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(|column| column.unwrap())
            .collect::<Vec<_>>();

        assert!(columns.contains(&"protocol".to_string()));
    }
}
