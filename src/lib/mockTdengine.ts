import type {
  ConnectionHealth,
  ConnectionRecord,
  RedisInfoRow,
  ResourceNode,
  TdengineField,
  TdengineObjectDetail,
  TdengineObjectKind,
  TdengineQueryResult,
  TdengineQueryRow,
} from "../types";
import {
  buildTdengineNodeId,
  buildTdenginePreviewSql,
  inspectTdengineSql,
  parseTdengineNodeId,
  tdengineCellToString,
} from "./tdengine";

interface MockTdengineTable {
  kind: "table" | "child-table";
  name: string;
  database: string;
  fields: TdengineField[];
  rows: TdengineQueryRow[];
  ddl: string;
  metaRows: RedisInfoRow[];
  supertable?: string;
  tags?: Record<string, string>;
}

interface MockTdengineSupertable {
  name: string;
  database: string;
  fields: TdengineField[];
  tagColumns: TdengineField[];
  ddl: string;
  metaRows: RedisInfoRow[];
  children: MockTdengineTable[];
}

interface MockTdengineDatabase {
  name: string;
  supertables: MockTdengineSupertable[];
  tables: MockTdengineTable[];
}

const databases: MockTdengineDatabase[] = [
  {
    name: "power",
    supertables: [
      {
        name: "meters",
        database: "power",
        fields: [
          { name: "ts", type: "TIMESTAMP", length: 8 },
          { name: "current", type: "FLOAT", length: 4 },
          { name: "voltage", type: "INT", length: 4 },
          { name: "phase", type: "FLOAT", length: 4 },
        ],
        tagColumns: [
          { name: "group_id", type: "INT", length: 4, note: "TAG" },
          { name: "location", type: "BINARY", length: 64, note: "TAG" },
        ],
        ddl:
          "CREATE STABLE `power`.`meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`group_id` INT, `location` BINARY(64))",
        metaRows: [
          { label: "Type", value: "Supertable" },
          { label: "Child tables", value: "2" },
          { label: "Columns", value: "4" },
          { label: "Tag columns", value: "2" },
        ],
        children: [
          {
            kind: "child-table",
            name: "d1001",
            database: "power",
            supertable: "meters",
            fields: [
              { name: "ts", type: "TIMESTAMP", length: 8 },
              { name: "current", type: "FLOAT", length: 4 },
              { name: "voltage", type: "INT", length: 4 },
              { name: "phase", type: "FLOAT", length: 4 },
            ],
            rows: [
              { ts: "2026-04-18T09:12:00Z", current: 11.2, voltage: 221, phase: 0.32 },
              { ts: "2026-04-18T09:11:00Z", current: 10.9, voltage: 220, phase: 0.31 },
              { ts: "2026-04-18T09:10:00Z", current: 10.4, voltage: 219, phase: 0.3 },
            ],
            ddl: "CREATE TABLE `power`.`d1001` USING `power`.`meters` TAGS (1, 'Shanghai-A')",
            metaRows: [
              { label: "Type", value: "Child table" },
              { label: "Supertable", value: "meters" },
              { label: "Rows", value: "3" },
            ],
            tags: {
              group_id: "1",
              location: "Shanghai-A",
            },
          },
          {
            kind: "child-table",
            name: "d1002",
            database: "power",
            supertable: "meters",
            fields: [
              { name: "ts", type: "TIMESTAMP", length: 8 },
              { name: "current", type: "FLOAT", length: 4 },
              { name: "voltage", type: "INT", length: 4 },
              { name: "phase", type: "FLOAT", length: 4 },
            ],
            rows: [
              { ts: "2026-04-18T09:12:00Z", current: 8.4, voltage: 217, phase: 0.29 },
              { ts: "2026-04-18T09:11:00Z", current: 8.1, voltage: 216, phase: 0.28 },
            ],
            ddl: "CREATE TABLE `power`.`d1002` USING `power`.`meters` TAGS (2, 'Shanghai-B')",
            metaRows: [
              { label: "Type", value: "Child table" },
              { label: "Supertable", value: "meters" },
              { label: "Rows", value: "2" },
            ],
            tags: {
              group_id: "2",
              location: "Shanghai-B",
            },
          },
        ],
      },
      {
        name: "weather",
        database: "power",
        fields: [
          { name: "ts", type: "TIMESTAMP", length: 8 },
          { name: "temperature", type: "FLOAT", length: 4 },
          { name: "humidity", type: "FLOAT", length: 4 },
        ],
        tagColumns: [
          { name: "city", type: "BINARY", length: 64, note: "TAG" },
          { name: "station", type: "INT", length: 4, note: "TAG" },
        ],
        ddl:
          "CREATE STABLE `power`.`weather` (`ts` TIMESTAMP, `temperature` FLOAT, `humidity` FLOAT) TAGS (`city` BINARY(64), `station` INT)",
        metaRows: [
          { label: "Type", value: "Supertable" },
          { label: "Child tables", value: "1" },
          { label: "Columns", value: "3" },
          { label: "Tag columns", value: "2" },
        ],
        children: [
          {
            kind: "child-table",
            name: "weather_bund",
            database: "power",
            supertable: "weather",
            fields: [
              { name: "ts", type: "TIMESTAMP", length: 8 },
              { name: "temperature", type: "FLOAT", length: 4 },
              { name: "humidity", type: "FLOAT", length: 4 },
            ],
            rows: [
              { ts: "2026-04-18T09:12:00Z", temperature: 21.5, humidity: 41.2 },
              { ts: "2026-04-18T09:11:00Z", temperature: 21.3, humidity: 40.8 },
            ],
            ddl: "CREATE TABLE `power`.`weather_bund` USING `power`.`weather` TAGS ('Shanghai', 7)",
            metaRows: [
              { label: "Type", value: "Child table" },
              { label: "Supertable", value: "weather" },
              { label: "Rows", value: "2" },
            ],
            tags: {
              city: "Shanghai",
              station: "7",
            },
          },
        ],
      },
    ],
    tables: [
      {
        kind: "table",
        name: "meter_events",
        database: "power",
        fields: [
          { name: "ts", type: "TIMESTAMP", length: 8 },
          { name: "device_id", type: "BINARY", length: 64 },
          { name: "event_type", type: "BINARY", length: 32 },
          { name: "level", type: "INT", length: 4 },
        ],
        rows: [
          { ts: "2026-04-18T09:10:00Z", device_id: "d1001", event_type: "alarm", level: 2 },
          { ts: "2026-04-18T09:06:00Z", device_id: "d1002", event_type: "recovery", level: 1 },
        ],
        ddl:
          "CREATE TABLE `power`.`meter_events` (`ts` TIMESTAMP, `device_id` BINARY(64), `event_type` BINARY(32), `level` INT)",
        metaRows: [
          { label: "Type", value: "Normal table" },
          { label: "Rows", value: "2" },
          { label: "Columns", value: "4" },
        ],
      },
    ],
  },
  {
    name: "ops",
    supertables: [],
    tables: [
      {
        kind: "table",
        name: "audit_log",
        database: "ops",
        fields: [
          { name: "ts", type: "TIMESTAMP", length: 8 },
          { name: "actor", type: "BINARY", length: 64 },
          { name: "action", type: "BINARY", length: 128 },
        ],
        rows: [
          { ts: "2026-04-18T08:00:00Z", actor: "system", action: "health-check" },
          { ts: "2026-04-18T08:05:00Z", actor: "ops", action: "result-export" },
        ],
        ddl: "CREATE TABLE `ops`.`audit_log` (`ts` TIMESTAMP, `actor` BINARY(64), `action` BINARY(128))",
        metaRows: [
          { label: "Type", value: "Normal table" },
          { label: "Rows", value: "2" },
          { label: "Columns", value: "3" },
        ],
      },
    ],
  },
];

function findDatabase(database: string) {
  return databases.find((entry) => entry.name === database) ?? null;
}

function findSupertable(database: string, objectName: string) {
  return findDatabase(database)?.supertables.find((entry) => entry.name === objectName) ?? null;
}

function findTable(database: string, objectName: string) {
  const db = findDatabase(database);
  if (!db) {
    return null;
  }

  return (
    db.tables.find((entry) => entry.name === objectName) ??
    db.supertables.flatMap((entry) => entry.children).find((entry) => entry.name === objectName) ??
    null
  );
}

function databaseNodes() {
  return databases.map<ResourceNode>((database) => ({
    id: buildTdengineNodeId({ database: database.name, objectKind: "database" }),
    label: database.name,
    kind: "database",
    meta: `${database.supertables.length} supertables · ${database.tables.length} tables`,
    expandable: true,
  }));
}

function databaseChildren(database: string) {
  const entry = findDatabase(database);
  if (!entry) {
    throw new Error(`Database ${database} was not found.`);
  }

  return [
    ...entry.supertables.map<ResourceNode>((stable) => ({
      id: buildTdengineNodeId({ database, objectKind: "supertable", objectName: stable.name }),
      label: stable.name,
      kind: "supertable",
      meta: `${stable.children.length} child tables`,
      expandable: true,
    })),
    ...entry.tables.map<ResourceNode>((table) => ({
      id: buildTdengineNodeId({ database, objectKind: "table", objectName: table.name }),
      label: table.name,
      kind: "table",
      meta: `${table.rows.length} preview rows`,
    })),
  ];
}

function buildPreviewMeta(fields: TdengineField[]): RedisInfoRow {
  const hasTimestamp = fields.some((field) => field.type.toUpperCase() === "TIMESTAMP");
  return {
    label: "Preview",
    value: "Latest 200 rows",
    secondary: hasTimestamp ? "Ordered by timestamp desc when available" : "Fallback to LIMIT 200",
  };
}

function buildMockTagValueRows(tagColumns: TdengineField[], tags: Record<string, string> | undefined) {
  return tagColumns.flatMap((field) => {
    const value = tags?.[field.name];
    if (value === undefined) {
      return [];
    }

    return [
      {
        label: field.name,
        value,
        secondary: field.type,
      } satisfies RedisInfoRow,
    ];
  });
}

function supertableChildren(database: string, supertable: string) {
  const entry = findSupertable(database, supertable);
  if (!entry) {
    throw new Error(`Supertable ${database}.${supertable} was not found.`);
  }

  return entry.children.map<ResourceNode>((table) => ({
    id: buildTdengineNodeId({
      database,
      objectKind: "child-table",
      supertable,
      objectName: table.name,
    }),
    label: table.name,
    kind: "child-table",
    meta: `${table.rows.length} preview rows`,
  }));
}

function rowsForStable(stable: MockTdengineSupertable) {
  return stable.children.flatMap((table) =>
    table.rows.map((row) => ({
      tbname: table.name,
      ...row,
    })),
  );
}

function queryRowsForObject(database: string, objectName: string) {
  const stable = findSupertable(database, objectName);
  if (stable) {
    return rowsForStable(stable);
  }

  const table = findTable(database, objectName);
  return table?.rows ?? null;
}

function parseObjectFromSql(sql: string, keyword: "from" | "describe" | "show create table" | "show create stable") {
  const expression =
    keyword === "from"
      ? /\bfrom\s+((`([^`]+)`|([A-Za-z0-9_]+))(?:\.(?:`([^`]+)`|([A-Za-z0-9_]+)))?)/i
      : keyword === "describe"
        ? /^describe\s+((`([^`]+)`|([A-Za-z0-9_]+))(?:\.(?:`([^`]+)`|([A-Za-z0-9_]+)))?)/i
        : new RegExp(`^${keyword}\\s+((\`([^\\\`]+)\`|([A-Za-z0-9_]+))(?:\\.(?:\`([^\\\`]+)\`|([A-Za-z0-9_]+)))?)`, "i");
  const match = sql.trim().match(expression);
  if (!match) {
    return null;
  }

  if (match[5] || match[6]) {
    return {
      database: match[3] ?? match[4] ?? "",
      objectName: match[5] ?? match[6] ?? "",
    };
  }

  return {
    database: null,
    objectName: match[3] ?? match[4] ?? "",
  };
}

function explicitLimit(sql: string) {
  const match = sql.match(/\blimit\s+(\d+)/i);
  const value = Number(match?.[1] ?? "");
  return Number.isFinite(value) && value > 0 ? value : null;
}

function resultFromRows(
  database: string,
  rows: TdengineQueryRow[],
  durationMs: number,
  maxRows: number,
  columns?: string[],
  obeyExplicitLimit = false,
): TdengineQueryResult {
  const columnList = columns ?? Object.keys(rows[0] ?? {});
  const limit = obeyExplicitLimit ? rows.length : maxRows;
  const visibleRows = rows.slice(0, limit);

  return {
    columns: columnList.map((name) => ({
      name,
      type: typeof visibleRows[0]?.[name] === "number" ? "NUMBER" : typeof visibleRows[0]?.[name] === "boolean" ? "BOOL" : "TEXT",
    })),
    rows: visibleRows,
    rowCount: visibleRows.length,
    durationMs,
    truncated: !obeyExplicitLimit && rows.length > visibleRows.length,
    database,
    error: null,
  };
}

export function buildMockTdengineCatalog(database?: string | null, supertable?: string | null) {
  if (!database) {
    return databaseNodes();
  }

  if (!supertable) {
    return databaseChildren(database);
  }

  return supertableChildren(database, supertable);
}

export function buildMockTdengineObjectDetail(
  database: string,
  objectName: string,
  objectKind: TdengineObjectKind,
): TdengineObjectDetail {
  if (objectKind === "supertable") {
    const stable = findSupertable(database, objectName);
    if (!stable) {
      throw new Error(`Supertable ${database}.${objectName} was not found.`);
    }

    return {
      database,
      objectName,
      objectKind,
      fields: stable.fields,
      tagColumns: stable.tagColumns,
      tagValueRows: [],
      ddl: stable.ddl,
      previewSql: buildTdenginePreviewSql(database, objectName, stable.fields),
      metaRows: [...stable.metaRows, { label: "Database", value: database }, buildPreviewMeta(stable.fields)],
    };
  }

  const table = findTable(database, objectName);
  if (!table) {
    throw new Error(`Table ${database}.${objectName} was not found.`);
  }

  const tagColumns =
    table.kind === "child-table" && table.supertable
      ? (findSupertable(database, table.supertable)?.tagColumns ?? []).map((field) => ({
          ...field,
          note: table.tags?.[field.name] ? `TAG = ${table.tags[field.name]}` : field.note ?? "TAG",
        }))
      : [];
  const tagValueRows = buildMockTagValueRows(tagColumns, table.tags);

  return {
    database,
    objectName,
    objectKind,
    fields: table.fields,
    tagColumns,
    tagValueRows,
    ddl: table.ddl,
    previewSql: buildTdenginePreviewSql(database, objectName, table.fields),
    metaRows: [
      ...table.metaRows,
      { label: "Database", value: database },
      ...(tagValueRows.length ? [{ label: "Tag values", value: String(tagValueRows.length) }] : []),
      buildPreviewMeta(table.fields),
    ],
  };
}

export function buildMockTdengineHealth(connection: ConnectionRecord): ConnectionHealth {
  const checkedAt = new Date().toISOString();
  const protocolLabel = connection.protocol === "native" ? "native client" : "WebSocket adapter";

  return {
    status: /localhost|127\.0\.0\.1/i.test(connection.host) ? "healthy" : "degraded",
    summary: `Preview mode treats ${protocolLabel} as reachable without a live TDengine handshake.`,
    details: [
      `${connection.host}:${connection.port} is stored as the current TDengine target.`,
      "Preview mode uses static catalog and query samples instead of a live TDengine server.",
      connection.protocol === "native"
        ? "Native mode still requires a matching TDengine client library in the desktop runtime."
        : "WebSocket mode expects taosAdapter on port 6041 by default.",
    ],
    latencyMs: /localhost|127\.0\.0\.1/i.test(connection.host) ? 16 : null,
    checkedAt,
  };
}

export function buildMockTdengineQueryResult(
  connection: ConnectionRecord,
  database: string,
  sql: string,
  maxRows: number,
): TdengineQueryResult {
  const inspected = inspectTdengineSql(sql);
  if (!inspected.allowed) {
    throw new Error(inspected.reason ?? "The SQL statement is blocked.");
  }

  const activeDatabase = database || connection.databaseName || databases[0]?.name || "";
  const statement = inspected.statement;

  if (inspected.verb === "USE") {
    const nextDatabase = inspected.database ?? "";
    if (!findDatabase(nextDatabase)) {
      throw new Error(`Database ${nextDatabase} was not found.`);
    }

    return {
      columns: [],
      rows: [],
      rowCount: 0,
      durationMs: 8,
      truncated: false,
      database: nextDatabase,
      error: null,
    };
  }

  if (/^show\s+databases/i.test(statement)) {
    return resultFromRows(
      activeDatabase,
      databases.map((entry) => ({
        name: entry.name,
        supertables: entry.supertables.length,
        tables: entry.tables.length,
      })),
      11,
      maxRows,
      ["name", "supertables", "tables"],
    );
  }

  if (/^show\s+stables/i.test(statement)) {
    const entry = findDatabase(activeDatabase);
    if (!entry) {
      throw new Error(`Database ${activeDatabase} was not found.`);
    }

    return resultFromRows(
      activeDatabase,
      entry.supertables.map((stable) => ({
        name: stable.name,
        child_tables: stable.children.length,
      })),
      9,
      maxRows,
      ["name", "child_tables"],
    );
  }

  if (/^show\s+normal\s+tables/i.test(statement)) {
    const entry = findDatabase(activeDatabase);
    if (!entry) {
      throw new Error(`Database ${activeDatabase} was not found.`);
    }

    return resultFromRows(activeDatabase, entry.tables.map((table) => ({ name: table.name })), 9, maxRows, ["name"]);
  }

  if (/^show\s+tables/i.test(statement)) {
    const entry = findDatabase(activeDatabase);
    if (!entry) {
      throw new Error(`Database ${activeDatabase} was not found.`);
    }

    return resultFromRows(
      activeDatabase,
      [
        ...entry.tables.map((table) => ({ name: table.name, kind: "table" })),
        ...entry.supertables.flatMap((stable) => stable.children.map((table) => ({ name: table.name, kind: "child-table" }))),
      ],
      10,
      maxRows,
      ["name", "kind"],
    );
  }

  if (/^describe\b/i.test(statement)) {
    const target = parseObjectFromSql(statement, "describe");
    const databaseName = target?.database ?? activeDatabase;
    const detail = buildMockTdengineObjectDetail(
      databaseName,
      target?.objectName ?? "",
      findSupertable(databaseName, target?.objectName ?? "") ? "supertable" : findTable(databaseName, target?.objectName ?? "")?.kind ?? "table",
    );

    return resultFromRows(
      databaseName,
      [
        ...detail.fields.map((field) => ({
          Field: field.name,
          Type: field.type,
          Length: field.length ?? "",
          Note: field.note ?? "",
        })),
        ...detail.tagColumns.map((field) => ({
          Field: field.name,
          Type: field.type,
          Length: field.length ?? "",
          Note: field.note ?? "TAG",
        })),
      ],
      7,
      maxRows,
      ["Field", "Type", "Length", "Note"],
    );
  }

  if (/^show\s+create\s+(table|stable)/i.test(statement)) {
    const target = parseObjectFromSql(statement, /^show\s+create\s+stable/i.test(statement) ? "show create stable" : "show create table");
    const databaseName = target?.database ?? activeDatabase;
    const detail = /^show\s+create\s+stable/i.test(statement)
      ? buildMockTdengineObjectDetail(databaseName, target?.objectName ?? "", "supertable")
      : buildMockTdengineObjectDetail(
          databaseName,
          target?.objectName ?? "",
          findTable(databaseName, target?.objectName ?? "")?.kind ?? "table",
        );

    return resultFromRows(
      databaseName,
      [{ name: detail.objectName, ddl: detail.ddl ?? "" }],
      6,
      maxRows,
      ["name", "ddl"],
    );
  }

  if (/^explain\b/i.test(statement)) {
    return resultFromRows(
      activeDatabase,
      [
        { step: "SCAN", detail: "Recent data preview uses a descending timestamp scan." },
        { step: "LIMIT", detail: "UI preview keeps only the first page in memory." },
      ],
      5,
      maxRows,
      ["step", "detail"],
    );
  }

  if (/^select\s+server_version\(\)/i.test(statement)) {
    return resultFromRows(activeDatabase, [{ server_version: "3.3.6.0" }], 4, maxRows, ["server_version"]);
  }

  if (/^select\s+database\(\)/i.test(statement)) {
    return resultFromRows(activeDatabase, [{ database: activeDatabase }], 4, maxRows, ["database"]);
  }

  const target = parseObjectFromSql(statement, "from");
  if (!target) {
    return resultFromRows(
      activeDatabase,
      [{ result: "Query executed in preview mode", sql: tdengineCellToString(statement) }],
      6,
      maxRows,
      ["result", "sql"],
    );
  }

  const databaseName = target.database ?? activeDatabase;
  const rows = queryRowsForObject(databaseName, target.objectName ?? "");
  if (!rows) {
    throw new Error(`Object ${databaseName}.${target.objectName ?? ""} was not found.`);
  }

  const obeyExplicitLimit = explicitLimit(statement) !== null;
  return resultFromRows(databaseName, rows, 13, maxRows, undefined, obeyExplicitLimit);
}

export function resolveMockTdengineSelection(nodeId: string) {
  const parsed = parseTdengineNodeId(nodeId);
  if (!parsed || parsed.objectKind === "database" || !parsed.objectName) {
    return null;
  }

  return buildMockTdengineObjectDetail(parsed.database, parsed.objectName, parsed.objectKind);
}
