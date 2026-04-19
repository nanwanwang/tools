import type {
  ConnectionRecord,
  RedisBrowseData,
  RedisBulkDeleteResult,
  RedisCapabilitySnapshot,
  RedisCommandResult,
  RedisCommandTable,
  RedisHelperEntry,
  RedisInfoRow,
  RedisKeyDetail,
  RedisKeySummary,
  RedisKeyType,
  RedisMonitorSnapshot,
  RedisSearchMode,
  RedisSlowlogEntry,
  RedisStreamState,
  RedisValueFormat,
  RedisWorkbenchResult,
  ResourceNode,
  WorkspaceMetric,
} from "../types";
import { splitRedisStatements } from "./redisStatements";

interface MockRedisRecord {
  key: string;
  keyType: RedisKeyType;
  ttlSeconds: number | null;
  size: number;
  encoding: string;
  raw: string;
  rows: RedisInfoRow[];
}

const commandSamples = ["GET", "SET", "HGETALL", "JSON.GET", "XRANGE", "SCAN", "TTL", "DEL"];

const mockRedisRecords: MockRedisRecord[] = [
  {
    key: "session:2048",
    keyType: "string",
    ttlSeconds: 3600,
    size: 86,
    encoding: "embstr",
    raw: '{"userId":2048,"region":"ap-southeast-1","flags":["beta","priority"]}',
    rows: [],
  },
  {
    key: "cache:feed",
    keyType: "hash",
    ttlSeconds: 180,
    size: 9,
    encoding: "listpack",
    raw: '{"version":"v3","items":"48","window":"15m"}',
    rows: [
      { label: "version", value: "v3" },
      { label: "items", value: "48" },
      { label: "window", value: "15m" },
    ],
  },
  {
    key: "queue:emails",
    keyType: "list",
    ttlSeconds: null,
    size: 42,
    encoding: "quicklist",
    raw: '["msg_1001","msg_1002","msg_1003"]',
    rows: [
      { label: "0", value: "msg_1001" },
      { label: "1", value: "msg_1002" },
      { label: "2", value: "msg_1003" },
    ],
  },
  {
    key: "flags:beta",
    keyType: "set",
    ttlSeconds: null,
    size: 18,
    encoding: "hashtable",
    raw: '["feature.checkout","feature.dashboard"]',
    rows: [
      { label: "member", value: "feature.checkout" },
      { label: "member", value: "feature.dashboard" },
    ],
  },
  {
    key: "scores:region",
    keyType: "zset",
    ttlSeconds: null,
    size: 6,
    encoding: "skiplist",
    raw: '[{"member":"us-east","score":91.2},{"member":"ap-southeast","score":88.7}]',
    rows: [
      { label: "us-east", value: "91.2" },
      { label: "ap-southeast", value: "88.7" },
    ],
  },
  {
    key: "profile:json",
    keyType: "json",
    ttlSeconds: null,
    size: 144,
    encoding: "ReJSON-RL",
    raw: '{"id":1,"name":"Ada","roles":["owner","editor"],"prefs":{"theme":"amber","alerts":true}}',
    rows: [
      { label: "id", value: "1" },
      { label: "name", value: "Ada" },
      { label: "roles", value: '["owner","editor"]' },
    ],
  },
  {
    key: "orders.stream",
    keyType: "stream",
    ttlSeconds: null,
    size: 12,
    encoding: "stream",
    raw: '[{"id":"1735738000000-0","fields":{"orderId":"ord_482901","status":"created"}},{"id":"1735738004000-0","fields":{"orderId":"ord_482902","status":"paid"}}]',
    rows: [
      { label: "1735738000000-0", value: "orderId=ord_482901 | status=created", secondary: "2 fields" },
      { label: "1735738004000-0", value: "orderId=ord_482902 | status=paid", secondary: "2 fields" },
    ],
  },
];

function namespaceForKey(key: string) {
  if (key.includes(":")) {
    return `${key.split(":")[0]}:*`;
  }

  if (key.includes(".")) {
    return `${key.split(".")[0]}.*`;
  }

  return "misc";
}

function escapePatternCharacter(value: string) {
  return value.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
}

function matchesRedisPattern(key: string, pattern: string) {
  if (!pattern.trim()) {
    return true;
  }

  const expression = `^${pattern
    .split("")
    .map((character) => {
      if (character === "*") {
        return ".*";
      }
      if (character === "?") {
        return ".";
      }
      return escapePatternCharacter(character);
    })
    .join("")}$`;

  return new RegExp(expression, "i").test(key);
}

function prettify(raw: string) {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function asciiPreview(raw: string) {
  return raw.replace(/[^\x20-\x7E]/g, ".");
}

function hexPreview(raw: string) {
  return [...new TextEncoder().encode(raw)].map((value) => value.toString(16).padStart(2, "0").toUpperCase()).join(" ");
}

function formatPreviews(raw: string) {
  const bytes = new TextEncoder().encode(raw);
  const utf8 = new TextDecoder().decode(bytes);
  let gbk = utf8;
  try {
    gbk = new TextDecoder("gbk").decode(bytes);
  } catch {
    gbk = utf8;
  }

  return {
    auto: prettify(raw),
    utf8,
    gbk,
    json: prettify(raw),
    hex: hexPreview(raw),
    ascii: asciiPreview(raw),
    base64: typeof btoa === "function" ? btoa(raw) : raw,
    raw,
  } satisfies Record<RedisValueFormat, string>;
}

function summaryFromRecord(record: MockRedisRecord): RedisKeySummary {
  return {
    id: `key:${record.key}`,
    key: record.key,
    keyType: record.keyType,
    ttlSeconds: record.ttlSeconds,
    size: record.size,
    namespace: namespaceForKey(record.key),
    displayName: record.key,
    meta: `${record.keyType} | ${record.ttlSeconds === null ? "No TTL" : `TTL ${record.ttlSeconds}s`} | size ${record.size}`,
  };
}

function resourcesFromRecords(database: number, records: MockRedisRecord[]): ResourceNode[] {
  const groups = new Map<string, MockRedisRecord[]>();

  for (const record of records) {
    const namespace = namespaceForKey(record.key);
    const items = groups.get(namespace) ?? [];
    items.push(record);
    groups.set(namespace, items);
  }

  const children = [...groups.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([namespace, items]) => ({
      id: `prefix:${namespace}`,
      label: namespace,
      kind: "prefix",
      meta: `${items.length} keys`,
      children: items
        .slice()
        .sort((left, right) => left.key.localeCompare(right.key))
        .map((item) => ({
          id: `key:${item.key}`,
          label: item.key,
          kind: item.keyType,
          meta: summaryFromRecord(item).meta,
        })),
    }));

  return [
    {
      id: `db:${database}`,
      label: `db${database}`,
      kind: "database",
      meta: `${records.length} loaded keys`,
      children,
    },
  ];
}

function browseMetrics(records: MockRedisRecord[], readonly: boolean): WorkspaceMetric[] {
  const namespaces = new Set(records.map((record) => namespaceForKey(record.key)));
  const ttlCount = records.filter((record) => record.ttlSeconds !== null).length;

  return [
    { label: "Loaded keys", value: String(records.length), detail: "Preview SCAN page", tone: "accent" },
    { label: "Namespaces", value: String(namespaces.size), detail: "Grouped by prefix", tone: "neutral" },
    {
      label: "Write safety",
      value: readonly ? "Locked" : "Guarded",
      detail: ttlCount ? `${ttlCount} keys with TTL` : "No expiring keys in view",
      tone: readonly ? "success" : "danger",
    },
  ];
}

function commandTable(columns: string[], rows: string[][]): RedisCommandTable {
  return { columns, rows };
}

function commandResult(statement: string, rawOutput: string, jsonOutput: string | null, table: RedisCommandTable | null, error: string | null): RedisCommandResult {
  return {
    statement,
    summary: error ? "Command returned an error." : table ? `Returned ${table.rows.length} row(s).` : "Command executed successfully.",
    durationMs: 14,
    rawOutput,
    jsonOutput,
    table,
    error,
  };
}

export function buildMockRedisCapability(connection: ConnectionRecord): RedisCapabilitySnapshot {
  return {
    connectionId: connection.id,
    serverMode: connection.sshEnabled ? "unknown" : "standalone",
    dbCount: 16,
    moduleNames: ["ReJSON"],
    supportsJson: true,
    supportsSlowlog: true,
    readonly: connection.readonly,
    browserSupported: !connection.sshEnabled,
    unsupportedReason: connection.sshEnabled ? "SSH tunnel connections are not supported in preview mode." : null,
    diagnostics: connection.sshEnabled
      ? ["SSH tunnel preview stays visible, but Redis commands are disabled."]
      : ["Preview mode uses local Redis samples instead of a live Redis socket.", "RedisJSON and Slow Log are shown with static demo data."],
  };
}

export function buildMockRedisBrowse(
  connection: ConnectionRecord,
  options: {
    database: number;
    pattern: string;
    limit: number;
    cursor?: string | null;
    typeFilter: RedisKeyType | "all";
    viewMode: "tree" | "list";
    searchMode?: RedisSearchMode;
  },
): RedisBrowseData {
  const capability = buildMockRedisCapability(connection);
  const normalizedPattern = options.pattern.trim().toLowerCase();
  const searchMode = options.searchMode ?? "pattern";
  const filtered = mockRedisRecords.filter((record) => {
    const matchesPattern =
      searchMode === "fuzzy"
        ? !normalizedPattern || record.key.toLowerCase().includes(normalizedPattern)
        : matchesRedisPattern(record.key, options.pattern.trim());
    const matchesType = options.typeFilter === "all" || record.keyType === options.typeFilter;
    return matchesPattern && matchesType;
  });
  const offset = Number(options.cursor ?? 0) || 0;
  const visible = filtered.slice(offset, offset + options.limit);
  const nextOffset = offset + visible.length;

  return {
    connectionId: connection.id,
    database: options.database,
    pattern: options.pattern,
    searchMode,
    searchPartial: searchMode === "fuzzy" && filtered.length > visible.length,
    limit: options.limit,
    cursor: String(offset),
    nextCursor: nextOffset < filtered.length ? String(nextOffset) : null,
    loadedCount: visible.length,
    scannedCount: filtered.length,
    hasMore: nextOffset < filtered.length,
    viewMode: options.viewMode,
    typeFilter: options.typeFilter,
    metrics: browseMetrics(visible, connection.readonly),
    resources: resourcesFromRecords(options.database, visible),
    keySummaries: visible.map(summaryFromRecord),
    diagnostics: capability.diagnostics,
    infoRows: [
      { label: "Redis version", value: "7.2.x" },
      { label: "Used memory", value: "128 MB" },
      { label: "Connected clients", value: "14" },
      { label: "Selected DB", value: String(options.database), secondary: "16 configured DBs" },
    ],
    serverRows: [
      { label: "Role", value: "master", secondary: "Mode standalone" },
      { label: "Uptime", value: "18", secondary: "days" },
      { label: "Ops/sec", value: "432" },
      { label: "Hit rate", value: "96.4%" },
    ],
    configRows: [
      { label: "databases", value: "16" },
      { label: "maxmemory", value: "0" },
      { label: "maxmemory-policy", value: "noeviction" },
      { label: "slowlog-log-slower-than", value: "10000" },
    ],
    capability,
  };
}

export function buildMockRedisKeyDetail(key: string): RedisKeyDetail {
  const record = mockRedisRecords.find((item) => item.key === key) ?? mockRedisRecords[0];
  const previews = formatPreviews(record.raw);

  return {
    key: record.key,
    keyType: record.keyType,
    ttlSeconds: record.ttlSeconds,
    size: record.size,
    encoding: record.encoding,
    rows: record.rows,
    editable: record.keyType !== "stream",
    canRefresh: record.keyType === "stream",
    previewLanguage: record.keyType === "string" && previews.json === record.raw ? "text" : "json",
    formatPreviews: previews,
    availableFormats: ["auto", "utf8", "gbk", "json", "hex", "ascii", "base64", "raw"],
    defaultFormat: previews.json === record.raw ? "utf8" : "auto",
    streamState: record.keyType === "stream" ? buildMockRedisStreamState(record.key, 20) : null,
  };
}

export function buildMockRedisSlowlog(limit = 10): RedisSlowlogEntry[] {
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
  ].slice(0, limit);
}

export function buildMockRedisStreamState(key: string, count = 20): RedisStreamState {
  return {
    key,
    length: 12,
    radixTreeKeys: 2,
    radixTreeNodes: 4,
    lastGeneratedId: "1735738004000-0",
    suggestedRefreshSeconds: 5,
    groups: [
      { name: "orders-consumer", consumers: 2, pending: 4, lastDeliveredId: "1735738004000-0", lag: 2 },
      { name: "audit-sync", consumers: 1, pending: 0, lastDeliveredId: "1735738004000-0", lag: 0 },
    ],
    entries: mockRedisRecords.find((record) => record.key === key)?.rows.slice(0, count) ?? [],
  };
}

export function buildMockRedisCliResult(statement: string): RedisCommandResult {
  const normalized = statement.trim().toUpperCase();
  if (!statement.trim()) {
    return commandResult(statement, "", null, null, "Command cannot be empty.");
  }

  if (normalized.startsWith("SCAN")) {
    const raw = "0 session:2048 cache:feed profile:json";
    return commandResult(
      statement,
      raw,
      JSON.stringify(["0", ["session:2048", "cache:feed", "profile:json"]], null, 2),
      commandTable(["value"], [["session:2048"], ["cache:feed"], ["profile:json"]]),
      null,
    );
  }

  if (normalized.startsWith("JSON.GET")) {
    const raw = mockRedisRecords.find((record) => record.key === "profile:json")?.raw ?? "{}";
    return commandResult(statement, raw, prettify(raw), null, null);
  }

  if (normalized.startsWith("XRANGE")) {
    const raw = mockRedisRecords.find((record) => record.key === "orders.stream")?.raw ?? "[]";
    return commandResult(
      statement,
      raw,
      prettify(raw),
      commandTable(["id", "summary"], [["1735738000000-0", "orderId=ord_482901 | status=created"], ["1735738004000-0", "orderId=ord_482902 | status=paid"]]),
      null,
    );
  }

  return commandResult(statement, "PONG", JSON.stringify("PONG"), null, null);
}

export function buildMockRedisWorkbenchResult(input: string): RedisWorkbenchResult {
  return {
    statements: splitRedisStatements(input).map((line) => buildMockRedisCliResult(line)),
  };
}

export function buildMockRedisBulkDelete(pattern: string, typeFilter: RedisKeyType | "all", dryRun: boolean): RedisBulkDeleteResult {
  const normalizedPattern = pattern.trim().toLowerCase();
  const searchMode: RedisSearchMode = /[*?]/.test(pattern) ? "pattern" : "fuzzy";
  const matched = mockRedisRecords.filter((record) => {
    const matchesPattern =
      searchMode === "fuzzy"
        ? !normalizedPattern || record.key.toLowerCase().includes(normalizedPattern)
        : matchesRedisPattern(record.key, pattern.trim());
    const matchesType = typeFilter === "all" || record.keyType === typeFilter;
    return matchesPattern && matchesType;
  });

  return {
    pattern,
    typeFilter,
    dryRun,
    matched: matched.length,
    deleted: dryRun ? 0 : matched.length,
    sampleKeys: matched.slice(0, 10).map((record) => record.key),
    durationMs: 18,
  };
}

export function mockRedisCommandSuggestions() {
  return commandSamples;
}

export function buildMockRedisHelperEntries(): RedisHelperEntry[] {
  return [
    {
      command: "GET",
      summary: "Read a string value.",
      syntax: "GET key",
      example: "GET session:2048",
      applicableTypes: ["string"],
      riskLevel: "safe",
      relatedCommands: ["SET", "TTL", "MGET"],
    },
    {
      command: "SET",
      summary: "Write or replace a string value.",
      syntax: "SET key value [EX seconds|PX milliseconds] [NX|XX]",
      example: "SET session:2048 active EX 300",
      applicableTypes: ["string"],
      riskLevel: "guarded",
      relatedCommands: ["GET", "TTL", "EXPIRE"],
    },
    {
      command: "TTL",
      summary: "Check how many seconds remain before a key expires.",
      syntax: "TTL key",
      example: "TTL session:2048",
      applicableTypes: ["all"],
      riskLevel: "safe",
      relatedCommands: ["EXPIRE", "TYPE", "GET"],
    },
    {
      command: "EXPIRE",
      summary: "Set or update the TTL on an existing key.",
      syntax: "EXPIRE key seconds",
      example: "EXPIRE session:2048 600",
      applicableTypes: ["all"],
      riskLevel: "guarded",
      relatedCommands: ["TTL", "PERSIST", "SET"],
    },
    {
      command: "TYPE",
      summary: "Return the Redis data type stored at a key.",
      syntax: "TYPE key",
      example: "TYPE profile:1001",
      applicableTypes: ["all"],
      riskLevel: "safe",
      relatedCommands: ["SCAN", "GET", "HGETALL"],
    },
    {
      command: "SCAN",
      summary: "Iterate keys without blocking Redis.",
      syntax: "SCAN cursor [MATCH pattern] [COUNT count]",
      example: "SCAN 0 MATCH session:* COUNT 200",
      applicableTypes: ["all"],
      riskLevel: "safe",
      relatedCommands: ["TYPE", "TTL", "UNLINK"],
    },
    {
      command: "HGETALL",
      summary: "Read all fields and values from a hash.",
      syntax: "HGETALL key",
      example: "HGETALL user:42",
      applicableTypes: ["hash"],
      riskLevel: "safe",
      relatedCommands: ["HGET", "HSET", "TYPE"],
    },
    {
      command: "LRANGE",
      summary: "Read a slice of items from a list.",
      syntax: "LRANGE key start stop",
      example: "LRANGE jobs:pending 0 49",
      applicableTypes: ["list"],
      riskLevel: "safe",
      relatedCommands: ["LLEN", "LPUSH", "RPUSH"],
    },
    {
      command: "SMEMBERS",
      summary: "Read all members from a set.",
      syntax: "SMEMBERS key",
      example: "SMEMBERS feature:flags",
      applicableTypes: ["set"],
      riskLevel: "safe",
      relatedCommands: ["SADD", "SREM", "SCARD"],
    },
    {
      command: "ZRANGE",
      summary: "Read members from a sorted set by rank.",
      syntax: "ZRANGE key start stop [WITHSCORES]",
      example: "ZRANGE leaderboard 0 19 WITHSCORES",
      applicableTypes: ["zset"],
      riskLevel: "safe",
      relatedCommands: ["ZADD", "ZREVRANGE", "ZSCORE"],
    },
    {
      command: "XADD",
      summary: "Append a message to a Redis stream.",
      syntax: "XADD key * field value [field value ...]",
      example: "XADD orders.stream * orderId ord_1001 status created",
      applicableTypes: ["stream"],
      riskLevel: "guarded",
      relatedCommands: ["XRANGE", "XINFO GROUPS", "XDEL"],
    },
    {
      command: "XRANGE",
      summary: "Read entries from a stream in ID order.",
      syntax: "XRANGE key start end [COUNT count]",
      example: "XRANGE orders.stream - + COUNT 20",
      applicableTypes: ["stream"],
      riskLevel: "safe",
      relatedCommands: ["XREVRANGE", "XADD", "XINFO GROUPS"],
    },
    {
      command: "XINFO GROUPS",
      summary: "Inspect consumer groups for a stream.",
      syntax: "XINFO GROUPS key",
      example: "XINFO GROUPS orders.stream",
      applicableTypes: ["stream"],
      riskLevel: "safe",
      relatedCommands: ["XINFO CONSUMERS", "XPENDING", "XREADGROUP"],
    },
    {
      command: "UNLINK",
      summary: "Delete keys asynchronously when supported.",
      syntax: "UNLINK key [key ...]",
      example: "UNLINK cache:feed cache:flags",
      applicableTypes: ["all"],
      riskLevel: "danger",
      relatedCommands: ["DEL", "EXPIRE"],
    },
    {
      command: "DEL",
      summary: "Delete keys immediately in the foreground thread.",
      syntax: "DEL key [key ...]",
      example: "DEL temp:job:1 temp:job:2",
      applicableTypes: ["all"],
      riskLevel: "danger",
      relatedCommands: ["UNLINK", "EXPIRE", "TTL"],
    },
    {
      command: "INFO",
      summary: "Read Redis server sections and metrics.",
      syntax: "INFO [section]",
      example: "INFO memory",
      applicableTypes: ["all"],
      riskLevel: "safe",
      relatedCommands: ["SLOWLOG GET", "CLIENT LIST", "MEMORY STATS"],
    },
    {
      command: "SLOWLOG GET",
      summary: "Read recent slow commands from Redis.",
      syntax: "SLOWLOG GET [count]",
      example: "SLOWLOG GET 20",
      applicableTypes: ["all"],
      riskLevel: "safe",
      relatedCommands: ["INFO", "MONITOR", "LATENCY LATEST"],
    },
  ];
}

export function buildMockRedisMonitorSnapshot(sessionId = "preview-session"): RedisMonitorSnapshot {
  return {
    sessionId,
    running: true,
    polledAt: new Date("2026-04-14T13:30:00Z").toISOString(),
    metrics: [
      { label: "Ops/sec", value: "182", secondary: "Preview sample" },
      { label: "Memory", value: "128 MB" },
      { label: "Clients", value: "14" },
      { label: "Hit rate", value: "98.4%" },
    ],
    slowlogEntries: buildMockRedisSlowlog(10),
    commandSamples: [
      { at: "2026-04-14T13:29:58.000Z", command: "GET session:2048", client: "127.0.0.1:58234" },
      { at: "2026-04-14T13:29:59.000Z", command: "XADD orders.stream * orderId ord_482903 status packed", client: "preview-worker" },
    ],
  };
}
