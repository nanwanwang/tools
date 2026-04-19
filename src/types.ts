export type MiddlewareKind = "redis" | "kafka" | "mysql" | "postgres" | "tdengine";

export type TdengineProtocol = "ws" | "native";

export type ConnectionProtocol = "" | TdengineProtocol;

export type EnvironmentKind = "local" | "dev" | "staging" | "production";

export type WorkspaceTab = "overview" | "explorer" | "actions" | "diagnostics";

export type HealthStatus = "healthy" | "degraded" | "unreachable";

export type RedisKeyType = "string" | "hash" | "list" | "set" | "zset" | "stream" | "json" | "unknown";

export type RedisBrowseViewMode = "tree" | "list";

export type RedisValueFormat = "auto" | "utf8" | "gbk" | "json" | "hex" | "ascii" | "base64" | "raw";

export type RedisCommandDisplayMode = "json" | "table" | "raw";

export type RedisSearchMode = "pattern" | "fuzzy";

export type RedisServerMode = "standalone" | "cluster" | "sentinel" | "unknown";

export interface ConnectionRecord {
  id: string;
  kind: MiddlewareKind;
  protocol: ConnectionProtocol;
  name: string;
  host: string;
  port: number;
  databaseName: string;
  username: string;
  authMode: string;
  environment: EnvironmentKind;
  tags: string[];
  readonly: boolean;
  favorite: boolean;
  useTls: boolean;
  tlsVerify: boolean;
  sshEnabled: boolean;
  sshHost: string;
  sshPort: number;
  sshUsername: string;
  schemaRegistryUrl: string;
  groupId: string;
  clientId: string;
  notes: string;
  lastCheckedAt: string | null;
  lastConnectedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ConnectionDraft {
  id?: string;
  kind: MiddlewareKind;
  protocol: ConnectionProtocol;
  name: string;
  host: string;
  port: number;
  databaseName: string;
  username: string;
  password: string;
  authMode: string;
  environment: EnvironmentKind;
  tagsInput: string;
  readonly: boolean;
  useTls: boolean;
  tlsVerify: boolean;
  sshEnabled: boolean;
  sshHost: string;
  sshPort: number;
  sshUsername: string;
  schemaRegistryUrl: string;
  groupId: string;
  clientId: string;
  notes: string;
}

export interface ConnectionHealth {
  status: HealthStatus;
  summary: string;
  details: string[];
  latencyMs: number | null;
  checkedAt: string;
}

export interface ResourceNode {
  id: string;
  label: string;
  kind: string;
  meta?: string;
  children?: ResourceNode[];
  expandable?: boolean;
}

export interface WorkspaceMetric {
  label: string;
  value: string;
  detail: string;
  tone: "accent" | "neutral" | "success" | "danger";
}

export interface WorkspacePanel {
  eyebrow: string;
  title: string;
  description: string;
  content: string;
  language: "json" | "sql" | "text";
}

export interface WorkspaceAction {
  title: string;
  description: string;
  tone: "accent" | "neutral" | "danger";
}

export interface RedisInfoRow {
  label: string;
  value: string;
  secondary?: string | null;
}

export interface RedisSlowlogEntry {
  id: string;
  startedAt: string;
  durationMicros: number;
  command: string;
  clientAddress?: string | null;
  clientName?: string | null;
}

export interface RedisCapabilitySnapshot {
  connectionId: string;
  serverMode: RedisServerMode;
  dbCount: number | null;
  moduleNames: string[];
  supportsJson: boolean;
  supportsSlowlog: boolean;
  readonly: boolean;
  browserSupported: boolean;
  unsupportedReason: string | null;
  diagnostics: string[];
}

export interface RedisKeySummary {
  id: string;
  key: string;
  keyType: RedisKeyType;
  ttlSeconds: number | null;
  size: number | null;
  namespace: string;
  displayName: string;
  meta: string;
}

export interface RedisStreamConsumerGroup {
  name: string;
  consumers: number;
  pending: number;
  lastDeliveredId: string | null;
  lag: number | null;
}

export interface RedisStreamState {
  key: string;
  length: number | null;
  radixTreeKeys: number | null;
  radixTreeNodes: number | null;
  lastGeneratedId: string | null;
  suggestedRefreshSeconds: number | null;
  groups: RedisStreamConsumerGroup[];
  entries: RedisInfoRow[];
}

export type RedisCreateKeyType = "string" | "hash" | "list" | "set" | "zset";

export interface RedisCreateKeyInput {
  type: RedisCreateKeyType;
  key: string;
  value: string;
  ttlSeconds: number | null;
}

export interface RedisKeyDetail {
  key: string;
  keyType: RedisKeyType;
  ttlSeconds: number | null;
  size: number | null;
  encoding: string | null;
  rows: RedisInfoRow[];
  editable: boolean;
  canRefresh: boolean;
  previewLanguage: "json" | "text";
  formatPreviews: Record<RedisValueFormat, string>;
  availableFormats: RedisValueFormat[];
  defaultFormat: RedisValueFormat;
  streamState: RedisStreamState | null;
}

export interface RedisBrowseData {
  connectionId: string;
  database: number;
  pattern: string;
  searchMode: RedisSearchMode;
  searchPartial: boolean;
  limit: number;
  cursor: string;
  nextCursor: string | null;
  loadedCount: number;
  scannedCount: number;
  hasMore: boolean;
  viewMode: RedisBrowseViewMode;
  typeFilter: RedisKeyType | "all";
  metrics: WorkspaceMetric[];
  resources: ResourceNode[];
  keySummaries: RedisKeySummary[];
  diagnostics: string[];
  infoRows: RedisInfoRow[];
  serverRows: RedisInfoRow[];
  configRows: RedisInfoRow[];
  capability: RedisCapabilitySnapshot;
}

export interface RedisMonitorCommand {
  at: string;
  command: string;
  client?: string | null;
}

export interface RedisMonitorSnapshot {
  sessionId: string;
  running: boolean;
  polledAt: string;
  metrics: RedisInfoRow[];
  slowlogEntries: RedisSlowlogEntry[];
  commandSamples: RedisMonitorCommand[];
}

export interface RedisHelperEntry {
  command: string;
  summary: string;
  syntax: string;
  example: string;
  applicableTypes: string[];
  riskLevel: "safe" | "guarded" | "danger" | string;
  relatedCommands: string[];
}

export interface RedisCommandTable {
  columns: string[];
  rows: string[][];
}

export interface RedisCommandResult {
  statement: string;
  summary: string;
  durationMs: number;
  rawOutput: string;
  jsonOutput: string | null;
  table: RedisCommandTable | null;
  error: string | null;
}

export interface RedisWorkbenchResult {
  statements: RedisCommandResult[];
}

export interface RedisBulkDeleteResult {
  pattern: string;
  typeFilter: RedisKeyType | "all";
  dryRun: boolean;
  matched: number;
  deleted: number;
  sampleKeys: string[];
  durationMs: number;
}

export interface RedisActionInput {
  action: "create-key" | "save-value" | "update-ttl" | "delete-key";
  key?: string;
  keyType?: RedisCreateKeyType;
  value?: string;
  ttlSeconds?: number | null;
}

export interface RedisActionResult {
  message: string;
}

export interface WorkspaceSnapshot {
  connectionId: string;
  title: string;
  subtitle: string;
  capabilityTags: string[];
  metrics: WorkspaceMetric[];
  resources: ResourceNode[];
  panels: WorkspacePanel[];
  actions: WorkspaceAction[];
  diagnostics: string[];
}

export type TdengineObjectKind = "database" | "supertable" | "child-table" | "table";

export interface TdengineField {
  name: string;
  type: string;
  length: number | null;
  note?: string | null;
}

export interface TdengineObjectDetail {
  database: string;
  objectName: string;
  objectKind: TdengineObjectKind;
  fields: TdengineField[];
  tagColumns: TdengineField[];
  tagValueRows: RedisInfoRow[];
  ddl: string | null;
  previewSql: string;
  metaRows: RedisInfoRow[];
}

export interface TdengineQueryColumn {
  name: string;
  type: string;
}

export type TdengineCell = string | number | boolean | null | Record<string, unknown> | unknown[];

export type TdengineQueryRow = Record<string, TdengineCell>;

export interface TdengineQueryResult {
  columns: TdengineQueryColumn[];
  rows: TdengineQueryRow[];
  rowCount: number;
  durationMs: number;
  truncated: boolean;
  database: string;
  error: string | null;
}

export interface TdengineQueryTab {
  id: string;
  title: string;
  database: string;
  sql: string;
  isRunning: boolean;
  result: TdengineQueryResult | null;
  objectName?: string | null;
  objectKind?: TdengineObjectKind | null;
}

export interface TdengineSavedQuery {
  id: string;
  title: string;
  database: string;
  sql: string;
  updatedAt: string;
}

export interface TdengineSqlSuggestion {
  id: string;
  label: string;
  detail: string;
  sql: string;
  database: string;
  kind: "command" | "database" | "object" | "detail" | "favorite";
}
