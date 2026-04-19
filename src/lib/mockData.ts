import type {
  ConnectionDraft,
  ConnectionHealth,
  ConnectionProtocol,
  ConnectionRecord,
  EnvironmentKind,
  MiddlewareKind,
  ResourceNode,
  TdengineProtocol,
  WorkspaceSnapshot,
  WorkspaceMetric,
} from "../types";
import { translateStatic, type AppLanguage } from "../i18n";

type LegacyRedisSlowlogEntry = {
  id: string;
  startedAt: string;
  durationMicros: number;
  command: string;
  clientAddress?: string | null;
  clientName?: string | null;
};

type LegacyRedisKeyDetail = {
  key: string;
  keyType: string;
  ttlSeconds: number | null;
  size: number | null;
  encoding: string | null;
  preview: string;
  previewLanguage: "json" | "text";
  rows: Array<{ label: string; value: string; secondary?: string | null }>;
  editable: boolean;
};

type LegacyRedisBrowserData = {
  connectionId: string;
  pattern: string;
  limit: number;
  loadedCount: number;
  hasMore: boolean;
  metrics: WorkspaceMetric[];
  resources: ResourceNode[];
  selectedKey: LegacyRedisKeyDetail | null;
  diagnostics: string[];
  infoRows: Array<{ label: string; value: string; secondary?: string | null }>;
  serverRows: Array<{ label: string; value: string; secondary?: string | null }>;
  configRows: Array<{ label: string; value: string; secondary?: string | null }>;
  slowlogEntries: LegacyRedisSlowlogEntry[];
};

export const defaultPorts: Record<MiddlewareKind, number> = {
  redis: 6379,
  kafka: 9092,
  mysql: 3306,
  postgres: 5432,
  tdengine: 6041,
};

export const tdengineProtocolPorts: Record<TdengineProtocol, number> = {
  ws: 6041,
  native: 6030,
};

export function getDefaultPort(kind: MiddlewareKind, protocol: ConnectionProtocol = "") {
  if (kind === "tdengine") {
    return tdengineProtocolPorts[(protocol || "ws") as TdengineProtocol];
  }

  return defaultPorts[kind];
}

export const kindLabels: Record<MiddlewareKind, string> = {
  redis: "Redis",
  kafka: "Kafka",
  mysql: "MySQL",
  postgres: "PostgreSQL",
  tdengine: "TDengine",
};

export const environmentLabels: Record<EnvironmentKind, string> = {
  local: "Local",
  dev: "Dev",
  staging: "Staging",
  production: "Production",
};

export const authModes: Record<MiddlewareKind, { value: string; label: string }[]> = {
  redis: [
    { value: "password", label: "Password / ACL" },
    { value: "tls", label: "TLS + Password" },
    { value: "ssh", label: "Password + SSH" },
  ],
  kafka: [
    { value: "plaintext", label: "PLAINTEXT" },
    { value: "ssl", label: "SSL" },
    { value: "sasl_plaintext", label: "SASL_PLAINTEXT" },
    { value: "sasl_ssl", label: "SASL_SSL" },
  ],
  mysql: [
    { value: "password", label: "Password" },
    { value: "tls", label: "TLS" },
    { value: "ssh", label: "SSH Tunnel" },
  ],
  postgres: [
    { value: "password", label: "Password" },
    { value: "tls", label: "TLS" },
    { value: "ssh", label: "SSH Tunnel" },
  ],
  tdengine: [{ value: "password", label: "Password" }],
};

export function createEmptyDraft(kind: MiddlewareKind = "redis"): ConnectionDraft {
  const protocol = kind === "tdengine" ? "ws" : "";

  return {
    kind,
    protocol,
    name: "",
    host: "127.0.0.1",
    port: getDefaultPort(kind, protocol),
    databaseName: kind === "redis" ? "0" : kind === "kafka" ? "" : kind === "tdengine" ? "" : "app",
    username: kind === "redis" ? "default" : kind === "tdengine" ? "root" : "",
    password: "",
    authMode: authModes[kind][0].value,
    environment: "dev",
    tagsInput: "",
    readonly: false,
    useTls: false,
    tlsVerify: true,
    sshEnabled: false,
    sshHost: "",
    sshPort: 22,
    sshUsername: "",
    schemaRegistryUrl: "",
    groupId: "",
    clientId: "",
    notes: "",
  };
}

export function draftFromConnection(connection: ConnectionRecord): ConnectionDraft {
  return {
    id: connection.id,
    kind: connection.kind,
    protocol: connection.protocol,
    name: connection.name,
    host: connection.host,
    port: connection.port,
    databaseName: connection.databaseName,
    username: connection.username,
    password: "",
    authMode: connection.authMode,
    environment: connection.environment,
    tagsInput: connection.tags.join(", "),
    readonly: connection.readonly,
    useTls: connection.useTls,
    tlsVerify: connection.tlsVerify,
    sshEnabled: connection.sshEnabled,
    sshHost: connection.sshHost,
    sshPort: connection.sshPort,
    sshUsername: connection.sshUsername,
    schemaRegistryUrl: connection.schemaRegistryUrl,
    groupId: connection.groupId,
    clientId: connection.clientId,
    notes: connection.notes,
  };
}

export function buildPreviewHealth(connection: ConnectionRecord, language: AppLanguage = "en-US"): ConnectionHealth {
  const checkedAt = new Date().toISOString();
  const details = [
    language === "zh-CN"
      ? `目标 ${connection.host}:${connection.port} 已准备好进行桌面运行时检查。`
      : `Target ${connection.host}:${connection.port} prepared for desktop runtime checks.`,
  ];
  const localHost = /localhost|127\.0\.0\.1/i.test(connection.host);

  if (connection.environment === "production") {
    details.push(language === "zh-CN" ? "生产配置会把写操作放在显式确认之后。" : "Production profile keeps write actions behind confirmation.");
    return {
      status: "degraded",
      summary:
        language === "zh-CN"
          ? "预览模式会把生产目标标记为受保护，直到执行真实健康检查。"
          : "Preview mode marks production targets as guarded until a live check runs.",
      details,
      latencyMs: 48,
      checkedAt,
    };
  }

  return {
    status: localHost ? "healthy" : "degraded",
    summary: localHost
      ? language === "zh-CN"
        ? "预览模式下看起来本地端点是可达的。"
        : "Local endpoint looks reachable in preview mode."
      : language === "zh-CN"
        ? "没有桌面运行时的情况下，预览模式无法验证远程目标。"
        : "Preview mode cannot verify the remote target without the desktop runtime.",
    details,
    latencyMs: localHost ? 12 : null,
    checkedAt,
  };
}

function makeResourceNode(id: string, label: string, kind: string, meta?: string, children?: ResourceNode[]) {
  return { id, label, kind, meta, children };
}

function buildRedisResources(connection: ConnectionRecord) {
  return [
    makeResourceNode(`db:${connection.databaseName || "0"}`, `db${connection.databaseName || "0"}`, "database", "14.2k keys", [
      makeResourceNode("group:sessions", "sessions:*", "prefix", "5.8k keys", [
        makeResourceNode("key:session:2048", "session:2048", "string", "TTL 1h"),
        makeResourceNode("key:session:9042", "session:9042", "string", "TTL 15m"),
      ]),
      makeResourceNode("group:cache", "cache:*", "prefix", "3.1k keys", [
        makeResourceNode("key:cache:feed", "cache:feed", "hash", "9 fields"),
        makeResourceNode("key:cache:flags", "cache:flags", "set", "18 members"),
      ]),
      makeResourceNode("group:stream", "orders.stream", "stream", "12 groups"),
    ]),
  ];
}

function buildKafkaResources() {
  return [
    makeResourceNode("topic:orders", "orders.created", "topic", "12 partitions", [
      makeResourceNode("partition:0", "partition-0", "partition", "Latest offset 482901"),
      makeResourceNode("partition:1", "partition-1", "partition", "Latest offset 481123"),
    ]),
    makeResourceNode("topic:billing", "billing.reconciled", "topic", "8 partitions"),
    makeResourceNode("group:checkout", "checkout-service", "consumer-group", "Lag 124"),
    makeResourceNode("group:analytics", "analytics-worker", "consumer-group", "Lag 0"),
  ];
}

function buildDatabaseResources(connection: ConnectionRecord) {
  const schemaName = connection.kind === "postgres" ? "public" : connection.databaseName || "app";

  return [
    makeResourceNode(`schema:${schemaName}`, schemaName, "schema", "18 tables", [
      makeResourceNode("table:orders", "orders", "table", "4.2M rows"),
      makeResourceNode("table:payments", "payments", "table", "2.1M rows"),
      makeResourceNode("table:users", "users", "table", "670k rows"),
      makeResourceNode("view:order_health", "order_health", "view", "materialized summary"),
    ]),
  ];
}

function buildTdengineResources(connection: ConnectionRecord) {
  const databaseName = connection.databaseName || "power";

  return [
    makeResourceNode(`td:db:${databaseName}`, databaseName, "database", "2 supertables"),
    makeResourceNode(`td:db:log`, "log", "database", "1 normal table"),
  ];
}

export function buildWorkspaceSnapshot(connection: ConnectionRecord, language: AppLanguage = "en-US"): WorkspaceSnapshot {
  const t = (key: string, values?: Record<string, string | number | null | undefined>) => translateStatic(language, key, values);
  const baseCapabilities = [t("preview.baseCapabilityTls"), t("preview.baseCapabilitySsh"), t("preview.baseCapabilityReadOnly")];

  if (connection.kind === "redis") {
    return {
      connectionId: connection.id,
      title: language === "zh-CN" ? "Redis 浏览器" : "Redis browser",
      subtitle: language === "zh-CN" ? "按前缀分组的 key 浏览、TTL 控制和 stream 检查。" : "Prefix-aware key explorer with TTL controls and stream inspection.",
      capabilityTags: [...baseCapabilities, language === "zh-CN" ? "SCAN 分页" : "SCAN paging", language === "zh-CN" ? "TTL 编辑" : "TTL edit", language === "zh-CN" ? "Pub/Sub 预览" : "Pub/Sub preview"],
      metrics: [
        { label: language === "zh-CN" ? "在线 key" : "Live keys", value: "14.2k", detail: language === "zh-CN" ? "基于 SCAN 浏览" : "SCAN-backed browse", tone: "accent" },
        { label: language === "zh-CN" ? "热点前缀" : "Hot prefixes", value: "28", detail: language === "zh-CN" ? "按命名空间分组" : "Grouped by namespace", tone: "neutral" },
        { label: language === "zh-CN" ? "写入保护" : "Write safety", value: connection.readonly ? (language === "zh-CN" ? "锁定" : "Locked") : (language === "zh-CN" ? "受控" : "Guarded"), detail: language === "zh-CN" ? "危险操作会再次确认" : "Danger actions ask again", tone: connection.readonly ? "success" : "danger" },
      ],
      resources: buildRedisResources(connection),
      panels: [
        {
          eyebrow: language === "zh-CN" ? "值预览" : "Value Preview",
          title: "session:2048",
          description: language === "zh-CN" ? "结构化 JSON 预览，TTL 元数据保持近处可见。" : "Structured JSON preview with TTL metadata kept nearby.",
          language: "json",
          content: `{\n  "userId": 2048,\n  "region": "ap-southeast-1",\n  "flags": ["beta", "priority"],\n  "expiresIn": 3600\n}`,
        },
        {
          eyebrow: language === "zh-CN" ? "运行说明" : "Operational Notes",
          title: language === "zh-CN" ? "Key 卫生" : "Key hygiene",
          description: language === "zh-CN" ? "把大 key、即将过期的 session 和 stream 积压放在同一面板里。" : "Show large keys, expiring sessions and stream backlog in one panel.",
          language: "text",
          content: "SCAN cursor: 481\nSlowlog threshold: 10000us\nTop prefix: sessions:*",
        },
      ],
      actions: [
        { title: language === "zh-CN" ? "编辑值" : "Edit value", description: language === "zh-CN" ? "打开结构化编辑器，同时保留原始 TTL 可见。" : "Open structured editor and keep the original TTL visible.", tone: "accent" },
        { title: language === "zh-CN" ? "确认后删除" : "Delete with confirm", description: language === "zh-CN" ? "单删和批量删除都要经过显式确认。" : "Single and batch delete stay behind an explicit confirmation step.", tone: "danger" },
        { title: language === "zh-CN" ? "检查 stream" : "Inspect streams", description: language === "zh-CN" ? "查看 consumer group、pending 数和最新消息。" : "Review consumer groups, pending counts and recent records.", tone: "neutral" },
      ],
      diagnostics: [
        language === "zh-CN" ? "大实例应使用 SCAN 分页，而不是 KEYS。" : "Use SCAN paging instead of KEYS for large instances.",
        language === "zh-CN" ? "日志和导出文件里应隐藏密码与 ACL 密钥。" : "Mask passwords and ACL secrets in logs and exported bundles.",
        language === "zh-CN" ? "生产别名要用红色强调并标记为受保护目标。" : "Treat production aliases as guarded targets with red emphasis.",
      ],
    };
  }

  if (connection.kind === "kafka") {
    return {
      connectionId: connection.id,
      title: language === "zh-CN" ? "Kafka 工作区" : "Kafka workspace",
      subtitle: language === "zh-CN" ? "在一个桌面界面里完成消费、检查和发送消息。" : "Consume, inspect and publish messages without leaving the desktop shell.",
      capabilityTags: [...baseCapabilities, language === "zh-CN" ? "Offset 跳转" : "Offset jumps", language === "zh-CN" ? "JSON 格式化" : "JSON formatting", "Schema Registry"],
      metrics: [
        { label: "Topics", value: "42", detail: "Searchable and partition-aware", tone: "accent" },
        { label: "Consumer groups", value: "17", detail: "Lag sorted", tone: "neutral" },
        { label: "Guard rails", value: "Offset reset off", detail: "Bulk reset disabled by default", tone: "success" },
      ],
      resources: buildKafkaResources(),
      panels: [
        {
          eyebrow: "Message Preview",
          title: "orders.created",
          description: "Readable JSON, offset position and headers in the same tab.",
          language: "json",
          content: `{\n  "orderId": "ord_482901",\n  "customerId": "cus_7712",\n  "total": 188.92,\n  "currency": "USD"\n}`,
        },
        {
          eyebrow: "Producer Draft",
          title: "Safe publish",
          description: "Keep headers, key and partition choice visible before sending.",
          language: "text",
          content: "Partition: auto\nHeaders: content-type=application/json\nSchema: avro subject connected",
        },
      ],
      actions: [
        { title: "Consume by range", description: "Jump by partition, offset or time window.", tone: "accent" },
        { title: "Publish sample", description: "Draft JSON payloads with validation hints before send.", tone: "neutral" },
        { title: "Review lag", description: "Surface consumer groups that drift from the head offset.", tone: "danger" },
      ],
      diagnostics: [
        "Separate security mode from schema settings to keep connection forms readable.",
        "Show broker/auth/version failures as plain-language diagnostics.",
        "Keep high-volume message panes virtualized to avoid UI stalls.",
      ],
    };
  }

  if (connection.kind === "tdengine") {
    return {
      connectionId: connection.id,
      title: language === "zh-CN" ? "TDengine 工作区" : "TDengine workspace",
      subtitle: language === "zh-CN" ? "带协议感知诊断的时序目录、SQL 标签页和结果表格。" : "Time-series catalog, SQL tabs and result grids with protocol-aware diagnostics.",
      capabilityTags: [...baseCapabilities, "WebSocket", "Native", language === "zh-CN" ? "结果导出" : "Result export"],
      metrics: [
        { label: "Databases", value: "2", detail: "Lazy-loaded catalog", tone: "accent" },
        { label: "Query tabs", value: "3", detail: "Per-tab database context", tone: "neutral" },
        {
          label: "Execution mode",
          value: "Read only",
          detail: "DDL and writes blocked in the first version",
          tone: "success",
        },
      ],
      resources: buildTdengineResources(connection),
      panels: [
        {
          eyebrow: "Catalog",
          title: "Supertables and child tables",
          description: "Database nodes expand into supertables, normal tables and lazy child-table lists.",
          language: "text",
          content: "power\n  meters\n    d1001\n    d1002\n  meter_events",
        },
        {
          eyebrow: "SQL",
          title: "Recent data preview",
          description: "Recent rows stay capped and exportable without turning the app into a full admin console.",
          language: "sql",
          content: "select * from power.d1001 order by ts desc limit 200;",
        },
      ],
      actions: [
        { title: "Run read-only SQL", description: "Allow SELECT, SHOW, DESCRIBE, EXPLAIN and USE only.", tone: "accent" },
        { title: "Export result", description: "Download the current grid as CSV or JSON.", tone: "neutral" },
        { title: "Check protocol", description: "Health check uses a real TDengine handshake, not a plain TCP socket.", tone: "danger" },
      ],
      diagnostics: [
        "WebSocket and native connections should be chosen explicitly in the connection profile.",
        "Native mode depends on a matching TDengine client installed on the local machine.",
        "Query tabs keep their own database context so browsing one database does not reset another tab.",
      ],
    };
  }

  return {
    connectionId: connection.id,
    title: `${kindLabels[connection.kind]} ${language === "zh-CN" ? "浏览器" : "explorer"}`,
    subtitle: language === "zh-CN" ? "Schema 浏览、查询标签页和可导出的结果集。" : "Schema browser, query tabs and export-ready result sets.",
    capabilityTags: [...baseCapabilities, language === "zh-CN" ? "结果导出" : "Result export", language === "zh-CN" ? "执行计划" : "Explain plans", language === "zh-CN" ? "可取消查询" : "Cancelable queries"],
    metrics: [
      { label: "Schemas", value: connection.kind === "postgres" ? "6" : "1", detail: "Tree navigation", tone: "accent" },
      { label: "Active tabs", value: "3", detail: "Pinned query history", tone: "neutral" },
      { label: "Write mode", value: connection.readonly ? "Read only" : "Confirm first", detail: "DDL/DML guarded", tone: connection.readonly ? "success" : "danger" },
    ],
    resources: buildDatabaseResources(connection),
    panels: [
      {
        eyebrow: "Query Pad",
        title: "Recent SQL",
        description: "Hold multi-tab query work with explain plans and export actions nearby.",
        language: "sql",
        content: `select id, status, created_at\nfrom orders\nwhere created_at >= now() - interval '7 days'\norder by created_at desc\nlimit 50;`,
      },
      {
        eyebrow: "Execution Plan",
        title: "Explain snapshot",
        description: "Highlight scan type, index usage and rows examined without leaving the result tab.",
        language: "text",
        content: "Index Scan using orders_created_at_idx\nRows: 50\nBuffers: shared hit=312",
      },
    ],
    actions: [
      { title: "Open query tab", description: "Keep SQL editor, result grid and history in a single workspace.", tone: "accent" },
      { title: "Export results", description: "Save current selection as CSV or JSON.", tone: "neutral" },
      { title: "Protect writes", description: "Ask again before DDL or large DML statements run.", tone: "danger" },
    ],
    diagnostics: [
      "Keep long-running queries cancelable from the UI.",
      "Separate schema tree fetches from result pagination to keep scrolling smooth.",
      "Mask passwords, tokens and certificate values in logs.",
    ],
  };
}

export function findNode(nodes: ResourceNode[], id: string | null | undefined): ResourceNode | null {
  if (!id) {
    return null;
  }

  for (const node of nodes) {
    if (node.id === id) {
      return node;
    }

    if (node.children?.length) {
      const nested = findNode(node.children, id);
      if (nested) {
        return nested;
      }
    }
  }

  return null;
}

const mockRedisKeys: LegacyRedisKeyDetail[] = [
  {
    key: "session:2048",
    keyType: "string",
    ttlSeconds: 3600,
    size: 86,
    encoding: "embstr",
    previewLanguage: "json",
    preview: `{\n  "userId": 2048,\n  "region": "ap-southeast-1",\n  "flags": ["beta", "priority"]\n}`,
    rows: [],
    editable: true,
  },
  {
    key: "cache:feed",
    keyType: "hash",
    ttlSeconds: 180,
    size: 9,
    encoding: "listpack",
    previewLanguage: "json",
    preview: `{\n  "version": "v3",\n  "items": 48,\n  "window": "15m"\n}`,
    rows: [
      { label: "version", value: "v3" },
      { label: "items", value: "48" },
      { label: "window", value: "15m" },
    ],
    editable: true,
  },
  {
    key: "queue:emails",
    keyType: "list",
    ttlSeconds: null,
    size: 42,
    encoding: "quicklist",
    previewLanguage: "json",
    preview: `[\n  "msg_1001",\n  "msg_1002",\n  "msg_1003"\n]`,
    rows: [
      { label: "0", value: "msg_1001" },
      { label: "1", value: "msg_1002" },
      { label: "2", value: "msg_1003" },
    ],
    editable: true,
  },
  {
    key: "flags:beta",
    keyType: "set",
    ttlSeconds: null,
    size: 18,
    encoding: "hashtable",
    previewLanguage: "json",
    preview: `[\n  "feature.dashboard",\n  "feature.checkout"\n]`,
    rows: [
      { label: "member", value: "feature.dashboard" },
      { label: "member", value: "feature.checkout" },
    ],
    editable: true,
  },
  {
    key: "scores:region",
    keyType: "zset",
    ttlSeconds: null,
    size: 6,
    encoding: "skiplist",
    previewLanguage: "json",
    preview: `[\n  { "member": "us-east", "score": 91.2 },\n  { "member": "ap-southeast", "score": 88.7 }\n]`,
    rows: [
      { label: "us-east", value: "91.2" },
      { label: "ap-southeast", value: "88.7" },
    ],
    editable: true,
  },
  {
    key: "orders.stream",
    keyType: "stream",
    ttlSeconds: null,
    size: 12,
    encoding: "stream",
    previewLanguage: "json",
    preview:
      '[\n  {\n    "id": "1735738000000-0",\n    "fields": {\n      "orderId": "ord_482901",\n      "status": "created"\n    }\n  },\n  {\n    "id": "1735738004000-0",\n    "fields": {\n      "orderId": "ord_482902",\n      "status": "paid"\n    }\n  }\n]',
    rows: [
      { label: "1735738000000-0", value: "orderId=ord_482901", secondary: "status=created" },
      { label: "1735738004000-0", value: "orderId=ord_482902", secondary: "status=paid" },
    ],
    editable: false,
  },
];

function prefixForRedisKey(key: string) {
  if (key.includes(":")) {
    return `${key.split(":")[0]}:*`;
  }

  if (key.includes(".")) {
    return `${key.split(".")[0]}.*`;
  }

  return "misc";
}

function redisKeyMeta(detail: LegacyRedisKeyDetail) {
  const ttlLabel = detail.ttlSeconds === null ? "No TTL" : `TTL ${detail.ttlSeconds}s`;
  return `${detail.keyType} | ${ttlLabel}`;
}

function buildRedisResourcesFromKeys(databaseName: string, details: LegacyRedisKeyDetail[]): ResourceNode[] {
  const groups = new Map<string, LegacyRedisKeyDetail[]>();

  for (const detail of details) {
    const prefix = prefixForRedisKey(detail.key);
    const current = groups.get(prefix) ?? [];
    current.push(detail);
    groups.set(prefix, current);
  }

  const prefixNodes = [...groups.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([prefix, groupDetails]) =>
      makeResourceNode(
        `prefix:${prefix}`,
        prefix,
        "prefix",
        `${groupDetails.length} keys`,
        groupDetails
          .slice()
          .sort((left, right) => left.key.localeCompare(right.key))
          .map((detail) => makeResourceNode(`key:${detail.key}`, detail.key, detail.keyType, redisKeyMeta(detail))),
      ),
    );

  return [makeResourceNode(`db:${databaseName || "0"}`, `db${databaseName || "0"}`, "database", `${details.length} loaded`, prefixNodes)];
}

function redisMetrics(details: LegacyRedisKeyDetail[], readonly: boolean): WorkspaceMetric[] {
  const expiring = details.filter((detail) => detail.ttlSeconds !== null).length;
  const prefixes = new Set(details.map((detail) => prefixForRedisKey(detail.key)));

  return [
    { label: "Loaded keys", value: String(details.length), detail: "SCAN-backed page", tone: "accent" },
    { label: "Namespaces", value: String(prefixes.size), detail: "Grouped by prefix", tone: "neutral" },
    {
      label: "Write safety",
      value: readonly ? "Locked" : "Guarded",
      detail: expiring ? `${expiring} keys with TTL` : "No expiring keys in view",
      tone: readonly ? "success" : "danger",
    },
  ];
}

function buildRedisServerRows(connection: ConnectionRecord) {
  return [
    { label: "Role", value: connection.environment === "production" ? "guarded" : "standalone" },
    { label: "Uptime", value: "18 days", secondary: "Preview sample" },
    { label: "Ops/sec", value: "432", secondary: "Instantaneous" },
    { label: "Hit rate", value: "96.4%", secondary: "Preview estimate" },
  ];
}

function buildRedisConfigRows(): { label: string; value: string; secondary?: string }[] {
  return [
    { label: "maxmemory", value: "0", secondary: "No eviction memory cap" },
    { label: "maxmemory-policy", value: "noeviction" },
    { label: "appendonly", value: "no" },
    { label: "save", value: "3600 1 300 100 60 10000" },
    { label: "slowlog-log-slower-than", value: "10000", secondary: "microseconds" },
    { label: "slowlog-max-len", value: "128" },
  ];
}

function buildRedisSlowlogEntries(): LegacyRedisSlowlogEntry[] {
  return [
    {
      id: "81",
      startedAt: "2026-04-13T09:20:11.000Z",
      durationMicros: 14231,
      command: "EVALSHA 9b6f... 2 session:2048 session:index",
      clientAddress: "127.0.0.1:58234",
      clientName: "preview-worker",
    },
    {
      id: "80",
      startedAt: "2026-04-13T09:18:06.000Z",
      durationMicros: 11844,
      command: "ZRANGE scores:region 0 49 WITHSCORES",
      clientAddress: "127.0.0.1:58234",
      clientName: null,
    },
  ];
}

export function buildMockRedisBrowser(
  connection: ConnectionRecord,
  pattern = "",
  limit = 80,
  selectedKey?: string | null,
): LegacyRedisBrowserData {
  const normalizedPattern = pattern.trim().toLowerCase();
  const filtered = normalizedPattern
    ? mockRedisKeys.filter((detail) => detail.key.toLowerCase().includes(normalizedPattern))
    : mockRedisKeys;
  const visible = filtered.slice(0, limit);
  const activeDetail = visible.find((detail) => detail.key === selectedKey) ?? visible[0] ?? null;

  return {
    connectionId: connection.id,
    pattern,
    limit,
    loadedCount: visible.length,
    hasMore: filtered.length > visible.length,
    metrics: redisMetrics(visible, connection.readonly),
    resources: buildRedisResourcesFromKeys(connection.databaseName, visible),
    selectedKey: activeDetail,
    diagnostics: [
      "Preview mode uses local mock data instead of a live Redis socket.",
      "String keys can be edited in the desktop runtime.",
      "Delete and TTL changes stay behind explicit confirmation.",
    ],
    infoRows: [
      { label: "Redis version", value: "7.2.x" },
      { label: "Used memory", value: "128 MB" },
      { label: "Connected clients", value: "14" },
      { label: "DB", value: connection.databaseName || "0" },
    ],
    serverRows: buildRedisServerRows(connection),
    configRows: buildRedisConfigRows(),
    slowlogEntries: buildRedisSlowlogEntries(),
  };
}

export function redisResourceIdToKey(resourceId: string | null | undefined) {
  if (!resourceId || !resourceId.startsWith("key:")) {
    return null;
  }

  return resourceId.slice(4);
}
