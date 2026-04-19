import { invoke } from "@tauri-apps/api/core";
import type { AppLanguage } from "../i18n";
import type {
  ConnectionDraft,
  ConnectionHealth,
  ConnectionProtocol,
  ConnectionRecord,
  RedisCommandDisplayMode,
  RedisHelperEntry,
  RedisMonitorSnapshot,
  RedisActionInput,
  RedisBrowseData,
  RedisCreateKeyInput,
  RedisKeyDetail,
  RedisKeyType,
  RedisSearchMode,
  RedisSlowlogEntry,
  RedisStreamState,
  RedisValueFormat,
  ResourceNode,
  TdengineObjectDetail,
  TdengineObjectKind,
  TdengineQueryResult,
  WorkspaceSnapshot,
} from "../types";
import { buildPreviewHealth, buildWorkspaceSnapshot } from "./mockData";
import {
  buildMockRedisBrowse,
  buildMockRedisBulkDelete,
  buildMockRedisCliResult,
  buildMockRedisHelperEntries,
  buildMockRedisKeyDetail,
  buildMockRedisMonitorSnapshot,
  buildMockRedisSlowlog,
  buildMockRedisStreamState,
  buildMockRedisWorkbenchResult,
} from "./mockRedis";
import {
  buildMockTdengineCatalog,
  buildMockTdengineHealth,
  buildMockTdengineObjectDetail,
  buildMockTdengineQueryResult,
} from "./mockTdengine";
import { splitRedisStatements } from "./redisStatements";
import { buildRedisResources } from "./redisView";

const browserStorageKey = "middleware-studio.connections.v1";

interface BrowserWindowWithTauri {
  __TAURI_INTERNALS__?: unknown;
}

function isDesktopRuntime() {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in (window as BrowserWindowWithTauri);
}

function safeRandomId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `connection-${Date.now()}-${Math.round(Math.random() * 10000)}`;
}

function readBrowserConnections() {
  if (typeof localStorage === "undefined") {
    return [] as ConnectionRecord[];
  }

  try {
    const raw = localStorage.getItem(browserStorageKey);
    return raw
      ? (JSON.parse(raw) as Array<Partial<ConnectionRecord>>).map((connection) => ({
          ...connection,
          protocol: connection.kind === "tdengine" ? connection.protocol ?? "ws" : "",
        })) as ConnectionRecord[]
      : [];
  } catch {
    return [] as ConnectionRecord[];
  }
}

function writeBrowserConnections(connections: ConnectionRecord[]) {
  if (typeof localStorage === "undefined") {
    return;
  }

  localStorage.setItem(browserStorageKey, JSON.stringify(connections));
}

function normalizeDraft(draft: ConnectionDraft) {
  return {
    id: draft.id ?? null,
    kind: draft.kind,
    protocol: (draft.kind === "tdengine" ? draft.protocol || "ws" : "") as ConnectionProtocol,
    name: draft.name.trim(),
    host: draft.host.trim(),
    port: Number(draft.port),
    databaseName: draft.databaseName.trim(),
    username: draft.username.trim(),
    authMode: draft.authMode,
    environment: draft.environment,
    tags: draft.tagsInput
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean),
    readonly: draft.readonly,
    useTls: draft.useTls,
    tlsVerify: draft.tlsVerify,
    sshEnabled: draft.sshEnabled,
    sshHost: draft.sshHost.trim(),
    sshPort: Number(draft.sshPort || 22),
    sshUsername: draft.sshUsername.trim(),
    schemaRegistryUrl: draft.schemaRegistryUrl.trim(),
    groupId: draft.groupId.trim(),
    clientId: draft.clientId.trim(),
    notes: draft.notes.trim(),
  };
}

function browserSaveConnection(draft: ConnectionDraft) {
  const normalized = normalizeDraft(draft);
  const now = new Date().toISOString();
  const existingConnections = readBrowserConnections();
  const existing = existingConnections.find((connection) => connection.id === draft.id);

  const saved: ConnectionRecord = {
    id: existing?.id ?? safeRandomId(),
    kind: normalized.kind,
    protocol: normalized.protocol,
    name: normalized.name,
    host: normalized.host,
    port: normalized.port,
    databaseName: normalized.databaseName,
    username: normalized.username,
    authMode: normalized.authMode,
    environment: normalized.environment,
    tags: normalized.tags,
    readonly: normalized.readonly,
    favorite: existing?.favorite ?? false,
    useTls: normalized.useTls,
    tlsVerify: normalized.tlsVerify,
    sshEnabled: normalized.sshEnabled,
    sshHost: normalized.sshHost,
    sshPort: normalized.sshPort,
    sshUsername: normalized.sshUsername,
    schemaRegistryUrl: normalized.schemaRegistryUrl,
    groupId: normalized.groupId,
    clientId: normalized.clientId,
    notes: normalized.notes,
    lastCheckedAt: existing?.lastCheckedAt ?? null,
    lastConnectedAt: existing?.lastConnectedAt ?? null,
    createdAt: existing?.createdAt ?? now,
    updatedAt: now,
  };

  writeBrowserConnections(
    existing ? existingConnections.map((connection) => (connection.id === existing.id ? saved : connection)) : [saved, ...existingConnections],
  );

  return saved;
}

function findPreviewConnection(id: string) {
  const connection = readBrowserConnections().find((entry) => entry.id === id);
  if (!connection) {
    throw new Error("Connection not found.");
  }
  return connection;
}

interface RedisBrowserPayload {
  connectionId: string;
  pattern: string;
  searchMode: RedisSearchMode;
  searchPartial: boolean;
  limit: number;
  loadedCount: number;
  hasMore: boolean;
  metrics: RedisBrowseData["metrics"];
  resources: RedisBrowseData["resources"];
  selectedKeyIds: string[];
  bulkActionState: {
    canDelete: boolean;
    preferredStrategy: string;
    requiresConfirmation: boolean;
  };
  valueViewModes: Array<{ id: string; label: string }>;
  diagnostics: string[];
  infoRows: RedisBrowseData["infoRows"];
  serverRows: RedisBrowseData["serverRows"];
  configRows: RedisBrowseData["configRows"];
  slowlogEntries: RedisSlowlogEntry[];
  selectedKey: {
    key: string;
    keyType: string;
    ttlSeconds: number | null;
    size: number | null;
    encoding: string | null;
    preview: string;
    previewLanguage: "json" | "text";
    rows: RedisBrowseData["infoRows"];
    editable: boolean;
    rawValueBase64: string | null;
    truncated: boolean;
  } | null;
}

interface RedisBulkDeletePayload {
  requestedCount: number;
  deletedCount: number;
  failedKeys: string[];
  strategy: string;
  durationMs: number;
}

interface RedisStreamPayload {
  key: string;
  cursor: string | null;
  pageSize: number;
  filter: string;
  entries: Array<{ id: string; fields: RedisBrowseData["infoRows"]; summary: string }>;
  groups: Array<{
    name: string;
    consumers: number;
    pending: number;
    lastDeliveredId: string;
    lag: number | null;
  }>;
}

interface RedisCliPayload {
  command: string;
  responseMode: "table" | "json" | "raw";
  raw: string;
  json: string | null;
  rows: Array<{ columns: string[] }>;
  executionMs: number;
  isWrite: boolean;
  requiresConfirmation: boolean;
}

function normalizeKeyType(keyType: string): RedisKeyType {
  switch (keyType) {
    case "string":
    case "hash":
    case "list":
    case "set":
    case "zset":
    case "stream":
    case "json":
      return keyType;
    default:
      return "unknown";
  }
}

function decodeBase64Bytes(value: string | null | undefined) {
  if (!value || typeof atob !== "function") {
    return null;
  }

  const binary = atob(value);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

function encodeBase64Bytes(bytes: Uint8Array) {
  if (typeof btoa !== "function") {
    return "";
  }

  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.slice(index, index + chunkSize));
  }
  return btoa(binary);
}

function decodeBytes(bytes: Uint8Array, encoding: string) {
  try {
    return new TextDecoder(encoding).decode(bytes);
  } catch {
    return new TextDecoder().decode(bytes);
  }
}

function asciiPreview(bytes: Uint8Array) {
  return Array.from(bytes, (value) => (value >= 0x20 && value <= 0x7e ? String.fromCharCode(value) : ".")).join("");
}

function hexPreview(bytes: Uint8Array) {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0").toUpperCase()).join(" ");
}

function prettify(raw: string) {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function parseKeyMeta(meta: string | undefined) {
  const result = { ttlSeconds: null as number | null, size: null as number | null };
  for (const segment of (meta ?? "").split("|").map((item) => item.trim())) {
    if (segment.startsWith("TTL ")) {
      const value = Number(segment.slice(4).replace(/s$/i, ""));
      result.ttlSeconds = Number.isFinite(value) ? value : null;
    }
    if (segment.startsWith("size ")) {
      const value = Number(segment.slice(5));
      result.size = Number.isFinite(value) ? value : null;
    }
  }
  return result;
}

function mapValueMode(id: string) {
  switch (id) {
    case "auto":
    case "utf8":
    case "gbk":
    case "json":
    case "hex":
    case "ascii":
    case "base64":
    case "raw":
      return id;
    default:
      return null;
  }
}

function inferSearchMode(pattern: string): RedisSearchMode {
  return /[*?]/.test(pattern) ? "pattern" : "fuzzy";
}

function commandErrorResult(statement: string, error: unknown) {
  const message = error instanceof Error ? error.message : "Command failed.";
  return {
    statement,
    summary: "Command failed.",
    durationMs: 0,
    rawOutput: message,
    jsonOutput: JSON.stringify({ error: message }, null, 2),
    table: null,
    error: message,
  };
}

function buildFormatPreviews(payload: NonNullable<RedisBrowserPayload["selectedKey"]>) {
  const bytes = decodeBase64Bytes(payload.rawValueBase64) ?? new TextEncoder().encode(payload.preview);
  const utf8 = decodeBytes(bytes, "utf-8");
  const gbk = decodeBytes(bytes, "gbk");
  const raw = utf8 || payload.preview;

  return {
    auto: payload.preview,
    utf8,
    gbk,
    json: payload.previewLanguage === "json" ? payload.preview : prettify(utf8),
    hex: hexPreview(bytes),
    ascii: asciiPreview(bytes),
    base64: payload.rawValueBase64 ?? encodeBase64Bytes(bytes),
    raw,
  };
}

function flattenKeySummaries(resources: RedisBrowseData["resources"]) {
  const summaries: RedisBrowseData["keySummaries"] = [];
  for (const databaseNode of resources) {
    for (const prefixNode of databaseNode.children ?? []) {
      for (const keyNode of prefixNode.children ?? []) {
        const parsed = parseKeyMeta(keyNode.meta);
        summaries.push({
          id: keyNode.id,
          key: keyNode.label,
          keyType: normalizeKeyType(keyNode.kind),
          ttlSeconds: parsed.ttlSeconds,
          size: parsed.size,
          namespace: prefixNode.label,
          displayName: keyNode.label,
          meta: keyNode.meta ?? "",
        });
      }
    }
  }
  return summaries;
}

function resolveDbCount(payload: RedisBrowserPayload) {
  const configured = payload.configRows.find((row) => row.label.toLowerCase() === "databases");
  const parsed = Number(configured?.value ?? "");
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 16;
}

function buildCapability(payload: RedisBrowserPayload, readonly: boolean): RedisBrowseData["capability"] {
  const serverMode = payload.serverRows[0]?.secondary?.toLowerCase().includes("cluster") ? "cluster" : "standalone";
  return {
    connectionId: payload.connectionId,
    serverMode,
    dbCount: resolveDbCount(payload),
    moduleNames: [],
    supportsJson: false,
    supportsSlowlog: payload.slowlogEntries.length >= 0,
    readonly,
    browserSupported: true,
    unsupportedReason: null,
    diagnostics: payload.diagnostics,
  };
}

function toKeyDetail(
  browser: RedisBrowserPayload,
  streamState: RedisStreamState | null,
): RedisKeyDetail | null {
  const payload = browser.selectedKey;
  if (!payload) {
    return null;
  }

  const formatPreviews = buildFormatPreviews(payload);
  const availableFormats = browser.valueViewModes
    .map((mode) => mapValueMode(mode.id))
    .filter((mode): mode is RedisValueFormat => mode !== null);
  const defaultFormat =
    payload.previewLanguage === "json" && availableFormats.includes("auto")
      ? "auto"
      : availableFormats.includes("utf8")
        ? "utf8"
        : availableFormats[0] ?? "raw";

  return {
    key: payload.key,
    keyType: normalizeKeyType(payload.keyType),
    ttlSeconds: payload.ttlSeconds,
    size: payload.size,
    encoding: payload.encoding,
    rows: payload.rows,
    editable: payload.editable,
    canRefresh: payload.keyType === "stream",
    previewLanguage: payload.previewLanguage,
    formatPreviews,
    availableFormats,
    defaultFormat,
    streamState,
  };
}

function toBrowseData(
  payload: RedisBrowserPayload,
  options: {
    database: number;
    pattern: string;
    typeFilter: RedisKeyType | "all";
    limit: number;
    cursor?: string | null;
    viewMode: "tree" | "list";
    searchMode?: RedisSearchMode;
  },
  readonly: boolean,
) {
  const offset = Number(options.cursor ?? 0) || 0;
  const summaries = flattenKeySummaries(payload.resources).filter(
    (summary) => options.typeFilter === "all" || summary.keyType === options.typeFilter,
  );
  const pageSummaries = summaries.slice(offset, offset + options.limit);
  const nextCursor = payload.hasMore && pageSummaries.length > 0 ? String(offset + pageSummaries.length) : null;

  return {
    connectionId: payload.connectionId,
    database: options.database,
    pattern: options.pattern,
    searchMode: options.searchMode ?? payload.searchMode ?? "pattern",
    searchPartial: payload.searchPartial,
    limit: options.limit,
    cursor: options.cursor ?? "0",
    nextCursor,
    loadedCount: pageSummaries.length,
    scannedCount: pageSummaries.length,
    hasMore: Boolean(nextCursor),
    viewMode: options.viewMode,
    typeFilter: options.typeFilter,
    metrics: payload.metrics,
    resources: buildRedisResources(options.database, pageSummaries),
    keySummaries: pageSummaries,
    diagnostics: payload.diagnostics,
    infoRows: payload.infoRows,
    serverRows: payload.serverRows,
    configRows: payload.configRows,
    capability: buildCapability(payload, readonly),
  } satisfies RedisBrowseData;
}

export const desktopApi = {
  isDesktopRuntime,

  async listConnections() {
    return isDesktopRuntime() ? invoke<ConnectionRecord[]>("list_connections") : readBrowserConnections();
  },

  async saveConnection(draft: ConnectionDraft) {
    if (isDesktopRuntime()) {
      return invoke<ConnectionRecord>("save_connection", {
        input: normalizeDraft(draft),
        secret: draft.password.trim() ? draft.password : null,
      });
    }

    return browserSaveConnection(draft);
  },

  async deleteConnection(id: string) {
    if (isDesktopRuntime()) {
      await invoke("delete_connection", { id });
      return;
    }

    writeBrowserConnections(readBrowserConnections().filter((connection) => connection.id !== id));
  },

  async toggleFavorite(id: string, favorite: boolean) {
    if (isDesktopRuntime()) {
      return invoke<ConnectionRecord>("toggle_favorite", { id, favorite });
    }

    const current = readBrowserConnections();
    const next = current.map((connection) => (connection.id === id ? { ...connection, favorite } : connection));
    writeBrowserConnections(next);
    return next.find((connection) => connection.id === id)!;
  },

  async touchConnection(id: string) {
    if (isDesktopRuntime()) {
      await invoke("touch_connection", { id });
      return;
    }

    const now = new Date().toISOString();
    writeBrowserConnections(
      readBrowserConnections().map((connection) =>
        connection.id === id ? { ...connection, lastConnectedAt: now, updatedAt: now } : connection,
      ),
    );
  },

  async healthCheck(id: string, language: AppLanguage = "en-US") {
    if (isDesktopRuntime()) {
      return invoke<ConnectionHealth>("health_check", { id });
    }

    const connection = findPreviewConnection(id);
    const preview = connection.kind === "tdengine" ? buildMockTdengineHealth(connection) : buildPreviewHealth(connection, language);
    writeBrowserConnections(
      readBrowserConnections().map((entry) =>
        entry.id === id ? { ...entry, lastCheckedAt: preview.checkedAt, updatedAt: preview.checkedAt } : entry,
      ),
    );
    return preview;
  },

  async getWorkspaceSnapshot(id: string, language: AppLanguage = "en-US") {
    if (isDesktopRuntime()) {
      return invoke<WorkspaceSnapshot>("get_workspace_snapshot", { id });
    }

    return buildWorkspaceSnapshot(findPreviewConnection(id), language);
  },

  async loadTdengineCatalog(
    id: string,
    options?: {
      database?: string | null;
      supertable?: string | null;
    },
  ) {
    if (isDesktopRuntime()) {
      return invoke<ResourceNode[]>("tdengine_load_catalog", {
        id,
        database: options?.database ?? null,
        supertable: options?.supertable ?? null,
      });
    }

    return buildMockTdengineCatalog(options?.database ?? null, options?.supertable ?? null);
  },

  async getTdengineObjectDetail(id: string, database: string, objectName: string, objectKind: TdengineObjectKind) {
    if (isDesktopRuntime()) {
      return invoke<TdengineObjectDetail>("tdengine_load_object_detail", {
        id,
        database,
        objectName,
        objectKind,
      });
    }

    return buildMockTdengineObjectDetail(database, objectName, objectKind);
  },

  async executeTdengineQuery(id: string, database: string, sql: string, maxRows = 1000) {
    if (isDesktopRuntime()) {
      return invoke<TdengineQueryResult>("tdengine_execute_query", {
        id,
        database,
        sql,
        maxRows,
      });
    }

    return buildMockTdengineQueryResult(findPreviewConnection(id), database, sql, maxRows);
  },

  async browseRedisKeys(
    id: string,
    options: {
      database: number;
      pattern: string;
      typeFilter: RedisKeyType | "all";
      limit: number;
      cursor?: string | null;
      viewMode: "tree" | "list";
      searchMode?: RedisSearchMode;
      selectedKeyIds?: string[];
    },
  ) {
    if (isDesktopRuntime()) {
      const offset = Number(options.cursor ?? 0) || 0;
      const payload = await invoke<RedisBrowserPayload>("load_redis_browser", {
        id,
        database: options.database,
        pattern: options.pattern,
        limit: offset + options.limit,
        selectedKey: null,
        searchMode: options.searchMode ?? "pattern",
        typeFilter: options.typeFilter,
        selectedKeyIds: options.selectedKeyIds ?? [],
      });
      return toBrowseData(payload, options, !payload.bulkActionState.canDelete);
    }

    return buildMockRedisBrowse(findPreviewConnection(id), options);
  },

  async getRedisKeyDetail(id: string, database: number, key: string) {
    if (isDesktopRuntime()) {
      const payload = await invoke<RedisBrowserPayload>("load_redis_browser", {
        id,
        database,
        pattern: "",
        limit: 1,
        selectedKey: key,
        searchMode: "pattern",
        typeFilter: "all",
        selectedKeyIds: [],
      });
      const streamState =
        payload.selectedKey?.keyType === "stream" ? await this.getRedisStreamState(id, database, key, 20) : null;
      const detail = toKeyDetail(payload, streamState);
      if (!detail) {
        throw new Error("Key detail not found.");
      }
      return detail;
    }

    return buildMockRedisKeyDetail(key);
  },

  async executeRedisAction(id: string, _database: number, input: RedisActionInput) {
    if (isDesktopRuntime()) {
      switch (input.action) {
        case "create-key":
          await invoke("redis_create_key", {
            id,
            database: _database,
            keyType: input.keyType,
            key: input.key,
            value: input.value,
            ttlSeconds: input.ttlSeconds ?? null,
          });
          return { message: `${input.key} created.` };
        case "save-value":
          await invoke("redis_save_key_value", { id, database: _database, key: input.key, value: input.value });
          return { message: `${input.key} updated.` };
        case "update-ttl":
          await invoke("redis_update_key_ttl", { id, database: _database, key: input.key, ttlSeconds: input.ttlSeconds ?? null });
          return { message: `${input.key} TTL updated.` };
        case "delete-key":
          await invoke("redis_delete_key", { id, database: _database, key: input.key });
          return { message: `${input.key} deleted.` };
      }
    }

    if (input.action === "create-key" || input.action === "save-value" || input.action === "update-ttl" || input.action === "delete-key") {
      throw new Error("Redis write actions require the desktop runtime.");
    }

    return { message: "Preview action executed." };
  },

  async bulkDeleteRedisKeys(id: string, database: number, pattern: string, typeFilter: RedisKeyType | "all", dryRun: boolean) {
    if (isDesktopRuntime()) {
      let cursor: string | null = null;
      const matchedKeys: string[] = [];
      const searchMode = inferSearchMode(pattern);

      do {
        const page = await this.browseRedisKeys(id, {
          database,
          pattern,
          typeFilter,
          limit: 200,
          cursor,
          viewMode: "tree",
          searchMode,
        });
        matchedKeys.push(...page.keySummaries.map((summary) => summary.key));
        cursor = page.nextCursor;
      } while (cursor);

      if (dryRun) {
        return {
          pattern,
          typeFilter,
          dryRun: true,
          matched: matchedKeys.length,
          deleted: 0,
          sampleKeys: matchedKeys.slice(0, 10),
          durationMs: 0,
        };
      }

      const payload = await invoke<RedisBulkDeletePayload>("redis_bulk_delete_keys", {
        id,
        database,
        keys: matchedKeys,
        strategy: "unlink",
      });

      return {
        pattern,
        typeFilter,
        dryRun: false,
        matched: payload.requestedCount,
        deleted: payload.deletedCount,
        sampleKeys: matchedKeys.slice(0, 10),
        durationMs: payload.durationMs,
      };
    }

    return buildMockRedisBulkDelete(pattern, typeFilter, dryRun);
  },

  async bulkDeleteRedisSelectedKeys(id: string, database: number, keys: string[], dryRun: boolean) {
    const normalizedKeys = [...new Set(keys.map((key) => key.trim()).filter(Boolean))];

    if (dryRun || !isDesktopRuntime()) {
      return {
        pattern: "",
        typeFilter: "all" as const,
        dryRun,
        matched: normalizedKeys.length,
        deleted: dryRun ? 0 : normalizedKeys.length,
        sampleKeys: normalizedKeys.slice(0, 10),
        durationMs: 0,
      };
    }

    const payload = await invoke<RedisBulkDeletePayload>("redis_bulk_delete_keys", {
      id,
      database,
      keys: normalizedKeys,
      strategy: "unlink",
    });

    return {
      pattern: "",
      typeFilter: "all" as const,
      dryRun: false,
      matched: payload.requestedCount,
      deleted: payload.deletedCount,
      sampleKeys: normalizedKeys.slice(0, 10),
      durationMs: payload.durationMs,
    };
  },

  async runRedisCliCommand(id: string, _database: number, statement: string, responseMode: RedisCommandDisplayMode = "table") {
    if (isDesktopRuntime()) {
      const payload = await invoke<RedisCliPayload>("redis_execute_cli", {
        id,
        database: _database,
        command: statement,
        responseMode,
      });
      const width = Math.max(...payload.rows.map((row) => row.columns.length), 1);
      return {
        statement,
        summary: payload.raw ? `${payload.responseMode.toUpperCase()} result in ${payload.executionMs} ms.` : "Command finished.",
        durationMs: payload.executionMs,
        rawOutput: payload.raw,
        jsonOutput: payload.json,
        table: payload.rows.length
          ? {
              columns: Array.from({ length: width }, (_, index) => `col${index + 1}`),
              rows: payload.rows.map((row) => row.columns),
            }
          : null,
        error: null,
      };
    }

    return buildMockRedisCliResult(statement);
  },

  async runRedisWorkbenchQuery(id: string, database: number, input: string, responseMode: RedisCommandDisplayMode = "table") {
    if (isDesktopRuntime()) {
      const statements = splitRedisStatements(input);
      return {
        statements: await Promise.all(
          statements.map(async (statement) => {
            try {
              return await this.runRedisCliCommand(id, database, statement, responseMode);
            } catch (error) {
              return commandErrorResult(statement, error);
            }
          }),
        ),
      };
    }

    return buildMockRedisWorkbenchResult(input);
  },

  async getRedisSlowlog(id: string, _database: number, limit: number) {
    if (isDesktopRuntime()) {
      const payload = await invoke<RedisBrowserPayload>("load_redis_browser", {
        id,
        database: _database,
        pattern: "",
        limit: 1,
        selectedKey: null,
        searchMode: "pattern",
        typeFilter: "all",
        selectedKeyIds: [],
      });
      return payload.slowlogEntries.slice(0, limit);
    }

    return buildMockRedisSlowlog(limit);
  },

  async getRedisStreamState(id: string, _database: number, key: string, count: number) {
    if (isDesktopRuntime()) {
      const payload = await invoke<RedisStreamPayload>("load_redis_stream", {
        id,
        database: _database,
        key,
        cursor: null,
        pageSize: count,
        filter: "",
      });
      return {
        key: payload.key,
        length: payload.entries.length,
        radixTreeKeys: null,
        radixTreeNodes: null,
        lastGeneratedId: payload.entries[0]?.id ?? null,
        suggestedRefreshSeconds: 5,
        groups: payload.groups.map((group) => ({
          name: group.name,
          consumers: group.consumers,
          pending: group.pending,
          lastDeliveredId: group.lastDeliveredId,
          lag: group.lag,
        })),
        entries: payload.entries.map((entry) => ({
          label: entry.id,
          value: entry.summary,
          secondary: `${entry.fields.length} fields`,
        })),
      };
    }

    return buildMockRedisStreamState(key, count);
  },

  async addRedisStreamEntry(id: string, database: number, key: string, value: string) {
    if (isDesktopRuntime()) {
      await invoke("redis_stream_add_entry", { id, database, key, value });
      return;
    }

    throw new Error("Redis write actions require the desktop runtime.");
  },

  async deleteRedisStreamEntry(id: string, database: number, key: string, entryId: string) {
    if (isDesktopRuntime()) {
      await invoke("redis_stream_delete_entry", { id, database, key, entryId });
      return;
    }

    throw new Error("Redis write actions require the desktop runtime.");
  },

  async getRedisJson(id: string, database: number, key: string) {
    if (isDesktopRuntime()) {
      const detail = await this.getRedisKeyDetail(id, database, key);
      return detail.formatPreviews.raw;
    }

    return buildMockRedisKeyDetail(key).formatPreviews.raw;
  },

  async setRedisJson(id: string, _database: number, key: string, value: string) {
    if (isDesktopRuntime()) {
      await invoke("redis_save_key_value", { id, database: _database, key, value });
      return;
    }

    throw new Error("Redis write actions require the desktop runtime.");
  },

  async startRedisMonitor(id: string, database: number) {
    if (isDesktopRuntime()) {
      return invoke<{ sessionId: string }>("redis_start_monitor", { id, database });
    }

    return { sessionId: "preview-session" };
  },

  async pollRedisMonitor(sessionId: string) {
    if (isDesktopRuntime()) {
      return invoke<RedisMonitorSnapshot>("redis_poll_monitor", { sessionId });
    }

    return buildMockRedisMonitorSnapshot(sessionId);
  },

  async stopRedisMonitor(sessionId: string) {
    if (isDesktopRuntime()) {
      await invoke("redis_stop_monitor", { sessionId });
      return;
    }
  },

  async listRedisHelperEntries() {
    if (isDesktopRuntime()) {
      return invoke<RedisHelperEntry[]>("list_redis_helper_entries");
    }

    return buildMockRedisHelperEntries();
  },

  toRedisCreateAction(input: RedisCreateKeyInput): RedisActionInput {
    return {
      action: "create-key",
      key: input.key,
      keyType: input.type,
      value: input.value,
      ttlSeconds: input.ttlSeconds,
    };
  },
};
