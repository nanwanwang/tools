use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInput {
    pub id: Option<String>,
    pub kind: String,
    pub protocol: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    pub username: String,
    pub auth_mode: String,
    pub environment: String,
    pub tags: Vec<String>,
    pub readonly: bool,
    pub use_tls: bool,
    pub tls_verify: bool,
    pub ssh_enabled: bool,
    pub ssh_host: String,
    pub ssh_port: u16,
    pub ssh_username: String,
    pub schema_registry_url: String,
    pub group_id: String,
    pub client_id: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionRecord {
    pub id: String,
    pub kind: String,
    pub protocol: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    pub username: String,
    pub auth_mode: String,
    pub environment: String,
    pub tags: Vec<String>,
    pub readonly: bool,
    pub favorite: bool,
    pub use_tls: bool,
    pub tls_verify: bool,
    pub ssh_enabled: bool,
    pub ssh_host: String,
    pub ssh_port: u16,
    pub ssh_username: String,
    pub schema_registry_url: String,
    pub group_id: String,
    pub client_id: String,
    pub notes: String,
    pub last_checked_at: Option<String>,
    pub last_connected_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionHealth {
    pub status: String,
    pub summary: String,
    pub details: Vec<String>,
    pub latency_ms: Option<u64>,
    pub checked_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceNode {
    pub id: String,
    pub label: String,
    pub kind: String,
    pub meta: Option<String>,
    pub children: Option<Vec<ResourceNode>>,
    pub expandable: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceMetric {
    pub label: String,
    pub value: String,
    pub detail: String,
    pub tone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspacePanel {
    pub eyebrow: String,
    pub title: String,
    pub description: String,
    pub content: String,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceAction {
    pub title: String,
    pub description: String,
    pub tone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisInfoRow {
    pub label: String,
    pub value: String,
    pub secondary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisSlowlogEntry {
    pub id: String,
    pub started_at: String,
    pub duration_micros: u64,
    pub command: String,
    pub client_address: Option<String>,
    pub client_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisValueViewMode {
    pub id: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisBulkActionState {
    pub can_delete: bool,
    pub preferred_strategy: String,
    pub requires_confirmation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisKeyDetail {
    pub key: String,
    pub key_type: String,
    pub ttl_seconds: Option<i64>,
    pub size: Option<usize>,
    pub encoding: Option<String>,
    pub preview: String,
    pub preview_language: String,
    pub rows: Vec<RedisInfoRow>,
    pub editable: bool,
    pub raw_value_base64: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisBrowser {
    pub connection_id: String,
    pub pattern: String,
    pub search_mode: String,
    pub search_partial: bool,
    pub limit: usize,
    pub loaded_count: usize,
    pub has_more: bool,
    pub metrics: Vec<WorkspaceMetric>,
    pub resources: Vec<ResourceNode>,
    pub selected_key: Option<RedisKeyDetail>,
    pub selected_key_ids: Vec<String>,
    pub bulk_action_state: RedisBulkActionState,
    pub value_view_modes: Vec<RedisValueViewMode>,
    pub diagnostics: Vec<String>,
    pub info_rows: Vec<RedisInfoRow>,
    pub server_rows: Vec<RedisInfoRow>,
    pub config_rows: Vec<RedisInfoRow>,
    pub slowlog_entries: Vec<RedisSlowlogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisBulkDeleteResult {
    pub requested_count: usize,
    pub deleted_count: usize,
    pub failed_keys: Vec<String>,
    pub strategy: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisStreamEntry {
    pub id: String,
    pub fields: Vec<RedisInfoRow>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisStreamConsumer {
    pub name: String,
    pub pending: u64,
    pub idle_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisStreamGroup {
    pub name: String,
    pub consumers: usize,
    pub pending: u64,
    pub last_delivered_id: String,
    pub lag: Option<u64>,
    pub consumer_details: Vec<RedisStreamConsumer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisStreamData {
    pub key: String,
    pub cursor: Option<String>,
    pub page_size: usize,
    pub filter: String,
    pub entries: Vec<RedisStreamEntry>,
    pub groups: Vec<RedisStreamGroup>,
    pub diagnostics: Vec<String>,
    pub can_write: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisCliRow {
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisCliResponse {
    pub command: String,
    pub response_mode: String,
    pub raw: String,
    pub json: Option<String>,
    pub rows: Vec<RedisCliRow>,
    pub execution_ms: u64,
    pub is_write: bool,
    pub requires_confirmation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisMonitorCommand {
    pub at: String,
    pub command: String,
    pub client: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisMonitorSession {
    pub session_id: String,
    pub connection_id: String,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisMonitorSnapshot {
    pub session_id: String,
    pub running: bool,
    pub polled_at: String,
    pub metrics: Vec<RedisInfoRow>,
    pub slowlog_entries: Vec<RedisSlowlogEntry>,
    pub command_samples: Vec<RedisMonitorCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisHelperEntry {
    pub command: String,
    pub summary: String,
    pub syntax: String,
    pub example: String,
    pub applicable_types: Vec<String>,
    pub risk_level: String,
    pub related_commands: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSnapshot {
    pub connection_id: String,
    pub title: String,
    pub subtitle: String,
    pub capability_tags: Vec<String>,
    pub metrics: Vec<WorkspaceMetric>,
    pub resources: Vec<ResourceNode>,
    pub panels: Vec<WorkspacePanel>,
    pub actions: Vec<WorkspaceAction>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TdengineField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub length: Option<u32>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TdengineObjectDetail {
    pub database: String,
    pub object_name: String,
    pub object_kind: String,
    pub fields: Vec<TdengineField>,
    pub tag_columns: Vec<TdengineField>,
    pub tag_value_rows: Vec<RedisInfoRow>,
    pub ddl: Option<String>,
    pub preview_sql: String,
    pub meta_rows: Vec<RedisInfoRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TdengineQueryColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TdengineQueryResult {
    pub columns: Vec<TdengineQueryColumn>,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub row_count: usize,
    pub duration_ms: u64,
    pub truncated: bool,
    pub database: String,
    pub error: Option<String>,
}
