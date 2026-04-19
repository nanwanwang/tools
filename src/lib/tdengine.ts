import type {
  ResourceNode,
  TdengineCell,
  TdengineField,
  TdengineObjectDetail,
  TdengineQueryRow,
  TdengineObjectKind,
  TdengineSavedQuery,
  TdengineSqlSuggestion,
} from "../types";

const allowedVerbs = new Set(["SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "USE"]);
const blockedVerbs = new Set([
  "INSERT",
  "CREATE",
  "ALTER",
  "DROP",
  "DELETE",
  "TRUNCATE",
  "UPDATE",
  "MERGE",
  "GRANT",
  "REVOKE",
  "REPLACE",
]);

export interface TdengineNodeRef {
  database: string;
  objectKind: TdengineObjectKind;
  objectName?: string;
  supertable?: string;
}

export interface TdengineSqlInspection {
  statement: string;
  verb: string;
  allowed: boolean;
  reason?: string;
  database?: string | null;
}

function stripSqlComments(sql: string) {
  return sql
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .split("\n")
    .map((line) => line.replace(/--.*$/g, "").replace(/#.*$/g, "").trim())
    .join(" ")
    .trim();
}

function splitStatements(sql: string) {
  return stripSqlComments(sql)
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
}

export function inspectTdengineSql(sql: string): TdengineSqlInspection {
  const statements = splitStatements(sql);

  if (!statements.length) {
    return {
      statement: "",
      verb: "",
      allowed: false,
      reason: "SQL cannot be empty.",
    };
  }

  if (statements.length > 1) {
    return {
      statement: statements[0],
      verb: "",
      allowed: false,
      reason: "Only one SQL statement can be executed at a time.",
    };
  }

  const statement = statements[0];
  const verb = statement.match(/^[A-Za-z]+/)?.[0]?.toUpperCase() ?? "";

  if (!verb) {
    return {
      statement,
      verb,
      allowed: false,
      reason: "The SQL statement could not be parsed.",
    };
  }

  if (blockedVerbs.has(verb)) {
    return {
      statement,
      verb,
      allowed: false,
      reason: `${verb} is blocked in the TDengine query workspace.`,
    };
  }

  if (!allowedVerbs.has(verb)) {
    return {
      statement,
      verb,
      allowed: false,
      reason: `${verb} is not supported in the first TDengine release.`,
    };
  }

  return {
    statement,
    verb,
    allowed: true,
    database: verb === "USE" ? extractUseDatabase(statement) : null,
  };
}

export function extractUseDatabase(sql: string) {
  const match = sql.trim().match(/^use\s+(`([^`]+)`|([^\s;]+))/i);
  return match?.[2] ?? match?.[3] ?? null;
}

export function hasTdengineLimit(sql: string) {
  return /\blimit\b/i.test(stripSqlComments(sql));
}

export function quoteTdengineIdentifier(value: string) {
  return `\`${value.replace(/`/g, "``")}\``;
}

export function buildTdengineNodeId(ref: TdengineNodeRef) {
  const parts = ["td", ref.objectKind, ref.database];

  if (ref.objectKind === "database") {
    return parts.join("|");
  }

  if (ref.objectKind === "child-table" && ref.supertable) {
    parts.push(ref.supertable);
  }

  if (ref.objectName) {
    parts.push(ref.objectName);
  }

  return parts.map(encodeURIComponent).join("|");
}

export function parseTdengineNodeId(nodeId: string | null | undefined): TdengineNodeRef | null {
  if (!nodeId) {
    return null;
  }

  const parts = nodeId.split("|").map((part) => decodeURIComponent(part));
  if (parts[0] !== "td") {
    return null;
  }

  const objectKind = parts[1] as TdengineObjectKind;
  if (objectKind === "database") {
    return {
      database: parts[2] ?? "",
      objectKind,
    };
  }

  if (objectKind === "child-table") {
    return {
      database: parts[2] ?? "",
      objectKind,
      supertable: parts[3] ?? "",
      objectName: parts[4] ?? "",
    };
  }

  return {
    database: parts[2] ?? "",
    objectKind,
    objectName: parts[3] ?? "",
  };
}

export function listTdengineDatabases(nodes: ResourceNode[] | null | undefined, fallbackDatabase?: string | null) {
  const databaseNames = new Set<string>();

  for (const node of nodes ?? []) {
    if (node.kind === "database" && node.label.trim()) {
      databaseNames.add(node.label.trim());
    }
  }

  if (fallbackDatabase?.trim()) {
    databaseNames.add(fallbackDatabase.trim());
  }

  return [...databaseNames];
}

const tdengineHistoryStoragePrefix = "middleware-studio.tdengine.history.";
const tdengineFavoritesStoragePrefix = "middleware-studio.tdengine.favorites.";

function hasLocalStorage() {
  return typeof localStorage !== "undefined";
}

function safeReadJson<T>(storageKey: string, fallback: T): T {
  if (!hasLocalStorage()) {
    return fallback;
  }

  try {
    const raw = localStorage.getItem(storageKey);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
}

function safeWriteJson(storageKey: string, value: unknown) {
  if (!hasLocalStorage()) {
    return;
  }

  localStorage.setItem(storageKey, JSON.stringify(value));
}

export function readTdengineQueryHistory(connectionId: string) {
  const values = safeReadJson<unknown[]>(`${tdengineHistoryStoragePrefix}${connectionId}`, []);
  return values.filter((value): value is string => typeof value === "string" && value.trim().length > 0);
}

export function writeTdengineQueryHistory(connectionId: string, values: string[]) {
  safeWriteJson(`${tdengineHistoryStoragePrefix}${connectionId}`, values);
}

export function readTdengineSavedQueries(connectionId: string) {
  const entries = safeReadJson<unknown[]>(`${tdengineFavoritesStoragePrefix}${connectionId}`, []);

  return entries.flatMap((entry) => {
    if (!entry || typeof entry !== "object") {
      return [];
    }

    const candidate = entry as Partial<TdengineSavedQuery>;
    if (!candidate.id || !candidate.title || typeof candidate.sql !== "string" || typeof candidate.database !== "string") {
      return [];
    }

    return [
      {
        id: candidate.id,
        title: candidate.title,
        sql: candidate.sql,
        database: candidate.database,
        updatedAt: candidate.updatedAt || new Date(0).toISOString(),
      } satisfies TdengineSavedQuery,
    ];
  });
}

export function writeTdengineSavedQueries(connectionId: string, values: TdengineSavedQuery[]) {
  safeWriteJson(`${tdengineFavoritesStoragePrefix}${connectionId}`, values);
}

export function clearTdengineStoredQueries(connectionId: string) {
  if (!hasLocalStorage()) {
    return;
  }

  localStorage.removeItem(`${tdengineHistoryStoragePrefix}${connectionId}`);
  localStorage.removeItem(`${tdengineFavoritesStoragePrefix}${connectionId}`);
}

export function upsertTdengineSavedQuery(
  entries: TdengineSavedQuery[],
  nextEntry: TdengineSavedQuery,
  maxItems = 20,
) {
  const normalizedSql = nextEntry.sql.trim();
  const duplicate = entries.find(
    (entry) =>
      entry.id === nextEntry.id ||
      (entry.database === nextEntry.database && entry.sql.trim().toLowerCase() === normalizedSql.toLowerCase()),
  );

  const mergedEntry: TdengineSavedQuery = duplicate
    ? {
        ...duplicate,
        ...nextEntry,
        sql: normalizedSql,
      }
    : {
        ...nextEntry,
        sql: normalizedSql,
      };

  return [mergedEntry, ...entries.filter((entry) => entry.id !== duplicate?.id && entry.id !== nextEntry.id)].slice(0, maxItems);
}

export function removeTdengineSavedQuery(entries: TdengineSavedQuery[], id: string) {
  return entries.filter((entry) => entry.id !== id);
}

export function findTdengineTimeField(fields: TdengineField[]) {
  return fields.find((field) => field.type.toUpperCase() === "TIMESTAMP")?.name ?? null;
}

export function buildTdenginePreviewSql(database: string, objectName: string, fields: TdengineField[]) {
  const target = `${quoteTdengineIdentifier(database)}.${quoteTdengineIdentifier(objectName)}`;
  const timeField = findTdengineTimeField(fields);

  if (timeField) {
    return `select * from ${target} order by ${quoteTdengineIdentifier(timeField)} desc limit 200`;
  }

  return `select * from ${target} limit 200`;
}

export function tdengineCellToString(value: TdengineCell) {
  if (value === null) {
    return "";
  }

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return JSON.stringify(value);
}

export function tdengineRowsToCsv(columns: string[], rows: Array<Record<string, TdengineCell>>) {
  const escapeCell = (value: TdengineCell) => `"${tdengineCellToString(value).replace(/"/g, "\"\"")}"`;
  const lines = [columns.map((column) => `"${column.replace(/"/g, "\"\"")}"`).join(",")];

  for (const row of rows) {
    lines.push(columns.map((column) => escapeCell(row[column] ?? null)).join(","));
  }

  return lines.join("\n");
}

export function projectTdengineRows(rows: TdengineQueryRow[], columns: string[]) {
  return rows.map((row) => {
    const projected: TdengineQueryRow = {};

    for (const column of columns) {
      projected[column] = row[column] ?? null;
    }

    return projected;
  });
}

export function filterTdengineResultRows(
  rows: TdengineQueryRow[],
  columns: string[],
  query: string,
  columnFilters: Record<string, string> = {},
) {
  const normalizedQuery = query.trim().toLowerCase();
  const allowedColumns = new Set(columns);
  const normalizedColumnFilters = Object.entries(columnFilters)
    .map(([column, value]) => [column, value.trim().toLowerCase()] as const)
    .filter(([column, value]) => allowedColumns.has(column) && value.length > 0);

  if (!normalizedQuery && !normalizedColumnFilters.length) {
    return rows;
  }

  return rows.filter((row) => {
    if (
      normalizedQuery &&
      !columns.some((column) => tdengineCellToString(row[column] ?? null).toLowerCase().includes(normalizedQuery))
    ) {
      return false;
    }

    return normalizedColumnFilters.every(([column, value]) =>
      tdengineCellToString(row[column] ?? null).toLowerCase().includes(value),
    );
  });
}

export type TdengineResultSortDirection = "asc" | "desc";

function compareTdengineCellValues(left: TdengineCell, right: TdengineCell) {
  if (left === null && right === null) {
    return 0;
  }

  if (left === null) {
    return 1;
  }

  if (right === null) {
    return -1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) - Number(right);
  }

  return tdengineCellToString(left).localeCompare(tdengineCellToString(right), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

export function sortTdengineResultRows(
  rows: TdengineQueryRow[],
  columnName: string | null,
  direction: TdengineResultSortDirection,
) {
  if (!columnName) {
    return rows;
  }

  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const leftValue = left.row[columnName] ?? null;
      const rightValue = right.row[columnName] ?? null;

      if (leftValue === null || rightValue === null) {
        if (leftValue === rightValue) {
          return left.index - right.index;
        }

        return leftValue === null ? 1 : -1;
      }

      const compared = compareTdengineCellValues(leftValue, rightValue);
      if (compared !== 0) {
        return direction === "asc" ? compared : -compared;
      }

      return left.index - right.index;
    })
    .map((entry) => entry.row);
}

export function paginateTdengineRows<T>(rows: T[], page: number, pageSize: number) {
  const normalizedPageSize = Math.max(1, pageSize);
  const totalPages = Math.max(1, Math.ceil(rows.length / normalizedPageSize));
  const currentPage = Math.min(Math.max(1, page), totalPages);
  const startIndex = rows.length ? (currentPage - 1) * normalizedPageSize : 0;
  const visibleRows = rows.slice(startIndex, startIndex + normalizedPageSize);

  return {
    rows: visibleRows,
    currentPage,
    totalPages,
    startIndex,
    endIndex: startIndex + visibleRows.length,
    pageSize: normalizedPageSize,
  };
}

interface TdengineSqlSuggestionContext {
  currentDatabase: string;
  databases: string[];
  detail: TdengineObjectDetail | null;
  resources: ResourceNode[] | null | undefined;
  favorites: TdengineSavedQuery[];
}

interface TdengineObjectSuggestionRef {
  database: string;
  objectName: string;
  objectKind: TdengineObjectKind;
}

function buildTdengineBasicPreviewSql(database: string, objectName: string) {
  return `select * from ${quoteTdengineIdentifier(database)}.${quoteTdengineIdentifier(objectName)} limit 200`;
}

function flattenTdengineObjects(nodes: ResourceNode[] | null | undefined) {
  const objects: TdengineObjectSuggestionRef[] = [];

  function visit(nodeList: ResourceNode[]) {
    for (const node of nodeList) {
      const parsed = parseTdengineNodeId(node.id);
      if (parsed?.objectName && parsed.objectKind !== "database") {
        objects.push({
          database: parsed.database,
          objectKind: parsed.objectKind,
          objectName: parsed.objectName,
        });
      }

      if (node.children?.length) {
        visit(node.children);
      }
    }
  }

  visit(nodes ?? []);
  return objects;
}

function uniqueSqlSuggestions(suggestions: TdengineSqlSuggestion[]) {
  const seen = new Set<string>();
  const unique: TdengineSqlSuggestion[] = [];

  for (const suggestion of suggestions) {
    const key = `${suggestion.database}|${suggestion.sql.trim().toLowerCase()}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    unique.push(suggestion);
  }

  return unique;
}

function buildFavoriteSuggestions(favorites: TdengineSavedQuery[]) {
  return favorites.map<TdengineSqlSuggestion>((favorite) => ({
    id: `favorite:${favorite.id}`,
    label: favorite.title,
    detail: `${favorite.database || "No database"} | saved SQL`,
    sql: favorite.sql,
    database: favorite.database,
    kind: "favorite",
  }));
}

export function buildTdengineQuickTemplates({
  currentDatabase,
  databases,
  detail,
  resources,
  favorites,
}: TdengineSqlSuggestionContext) {
  const templates: TdengineSqlSuggestion[] = [
    {
      id: "command:show-databases",
      label: "Show Databases",
      detail: "List all databases",
      sql: "show databases",
      database: "",
      kind: "command",
    },
  ];

  if (currentDatabase) {
    templates.push(
      {
        id: `database:use:${currentDatabase}`,
        label: `Use ${currentDatabase}`,
        detail: "Switch current database context",
        sql: `use ${quoteTdengineIdentifier(currentDatabase)}`,
        database: currentDatabase,
        kind: "database",
      },
      {
        id: `database:stables:${currentDatabase}`,
        label: "Show Stables",
        detail: `${currentDatabase} supertables`,
        sql: "show stables",
        database: currentDatabase,
        kind: "database",
      },
      {
        id: `database:tables:${currentDatabase}`,
        label: "Show Tables",
        detail: `${currentDatabase} tables and child tables`,
        sql: "show tables",
        database: currentDatabase,
        kind: "database",
      },
    );
  }

  for (const database of databases.slice(0, 4)) {
    templates.push({
      id: `database:use-shortcut:${database}`,
      label: `Use ${database}`,
      detail: "Quick database switch",
      sql: `use ${quoteTdengineIdentifier(database)}`,
      database,
      kind: "database",
    });
  }

  if (detail) {
    const target = `${quoteTdengineIdentifier(detail.database)}.${quoteTdengineIdentifier(detail.objectName)}`;
    templates.push(
      {
        id: `detail:preview:${detail.database}:${detail.objectName}`,
        label: `Preview ${detail.objectName}`,
        detail: `${detail.objectKind} preview`,
        sql: detail.previewSql,
        database: detail.database,
        kind: "detail",
      },
      {
        id: `detail:describe:${detail.database}:${detail.objectName}`,
        label: `Describe ${detail.objectName}`,
        detail: "Show columns and tag definitions",
        sql: `describe ${target}`,
        database: detail.database,
        kind: "detail",
      },
      {
        id: `detail:create:${detail.database}:${detail.objectName}`,
        label: `Show Create ${detail.objectName}`,
        detail: detail.objectKind === "supertable" ? "Show stable DDL" : "Show table DDL",
        sql: `${detail.objectKind === "supertable" ? "show create stable" : "show create table"} ${target}`,
        database: detail.database,
        kind: "detail",
      },
      {
        id: `detail:explain:${detail.database}:${detail.objectName}`,
        label: `Explain Preview ${detail.objectName}`,
        detail: "Explain current preview query",
        sql: `explain ${detail.previewSql}`,
        database: detail.database,
        kind: "detail",
      },
    );
  }

  for (const object of flattenTdengineObjects(resources)
    .filter((entry) => !currentDatabase || entry.database === currentDatabase)
    .slice(0, 6)) {
    templates.push({
      id: `object:${object.database}:${object.objectName}`,
      label: object.objectName,
      detail: `${object.database} | ${object.objectKind}`,
      sql: buildTdengineBasicPreviewSql(object.database, object.objectName),
      database: object.database,
      kind: "object",
    });
  }

  templates.push(...buildFavoriteSuggestions(favorites).slice(0, 4));
  return uniqueSqlSuggestions(templates);
}

function suggestionMatchScore(suggestion: TdengineSqlSuggestion, fullQuery: string, lastToken: string) {
  const haystack = `${suggestion.label} ${suggestion.detail} ${suggestion.sql} ${suggestion.database}`.toLowerCase();
  const normalizedSql = suggestion.sql.trim().toLowerCase();

  if (!fullQuery && !lastToken) {
    return 1;
  }

  if (fullQuery && normalizedSql.startsWith(fullQuery)) {
    return 120;
  }

  if (fullQuery && haystack.includes(fullQuery)) {
    return 80;
  }

  if (lastToken && suggestion.label.toLowerCase().startsWith(lastToken)) {
    return 70;
  }

  if (lastToken && normalizedSql.includes(lastToken)) {
    return 60;
  }

  if (lastToken && haystack.includes(lastToken)) {
    return 40;
  }

  return 0;
}

export function buildTdengineAutocompleteSuggestions(
  inputSql: string,
  context: TdengineSqlSuggestionContext,
  limit = 8,
) {
  const normalizedInput = stripSqlComments(inputSql).toLowerCase();
  const compactInput = normalizedInput.trim();
  const tokens = compactInput.split(/\s+/).filter(Boolean);
  const lastToken = (tokens.length ? tokens[tokens.length - 1] : "").replace(/[`;,()]/g, "");
  const source = buildTdengineQuickTemplates(context);

  const ranked = source
    .map((suggestion) => ({
      suggestion,
      score: suggestionMatchScore(suggestion, compactInput, lastToken),
    }))
    .filter((entry) => entry.score > 0)
    .sort((left, right) => right.score - left.score || left.suggestion.label.localeCompare(right.suggestion.label))
    .map((entry) => entry.suggestion);

  return ranked.slice(0, limit);
}
