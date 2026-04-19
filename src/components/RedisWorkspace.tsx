import { useEffect, useMemo, useState, type KeyboardEvent } from "react";
import clsx from "clsx";
import { ChevronDown, PencilLine, Play, RefreshCw, Search, TerminalSquare, Trash2, Waves, X } from "lucide-react";
import { useI18n } from "../i18n";
import { desktopApi } from "../lib/desktopApi";
import { splitRedisStatements } from "../lib/redisStatements";
import type {
  ConnectionHealth,
  ConnectionRecord,
  RedisBrowseData,
  RedisBulkDeleteResult,
  RedisCommandDisplayMode,
  RedisCommandResult,
  RedisCreateKeyInput,
  RedisCreateKeyType,
  RedisHelperEntry,
  RedisKeyDetail,
  RedisKeyType,
  RedisMonitorSnapshot,
  RedisSearchMode,
  RedisSlowlogEntry,
  RedisValueFormat,
  RedisWorkbenchResult,
  WorkspaceTab,
} from "../types";

interface Props {
  connection: ConnectionRecord;
  connections: ConnectionRecord[];
  browse: RedisBrowseData | null;
  detail: RedisKeyDetail | null;
  selectedKeyIds: string[];
  slowlog: RedisSlowlogEntry[];
  health: ConnectionHealth | null;
  selectedDatabase: number;
  selectedResourceId: string | null;
  tab: WorkspaceTab;
  runtimeMode: "desktop" | "preview";
  onTabChange: (tab: WorkspaceTab) => void;
  onSelectConnection: (id: string) => void;
  onSelectDatabase: (database: number) => void;
  onBrowseChange: (options: {
    database: number;
    pattern: string;
    searchMode: RedisSearchMode;
    typeFilter: RedisKeyType | "all";
    viewMode: "tree" | "list";
  }) => void;
  onSelectResource: (id: string) => void;
  onToggleKeySelection: (key: string, selected: boolean) => void;
  onToggleAllVisibleKeys: (selected: boolean) => void;
  onClearKeySelection: () => void;
  onClearSelection: () => Promise<void>;
  onRunHealthCheck: () => void;
  onEditConnection: () => void;
  onDeleteConnection: () => void;
  onRefresh: () => void;
  onLoadMore: () => void;
  onSaveValue: (value: string) => Promise<void>;
  onCreateKey: (input: RedisCreateKeyInput) => Promise<void>;
  onUpdateTtl: (ttlSeconds: number | null) => Promise<void>;
  onDeleteKey: () => Promise<void>;
  onPreviewBulkDelete: (pattern: string, typeFilter: RedisKeyType | "all") => Promise<RedisBulkDeleteResult>;
  onRunBulkDelete: (pattern: string, typeFilter: RedisKeyType | "all") => Promise<RedisBulkDeleteResult>;
  onRunCli: (statement: string, responseMode: RedisCommandDisplayMode) => Promise<RedisCommandResult>;
  onRunWorkbench: (input: string, responseMode: RedisCommandDisplayMode) => Promise<RedisWorkbenchResult>;
  onRefreshSlowlog: () => Promise<void>;
  onRefreshStream: () => Promise<void>;
}

const createExamples: Record<RedisCreateKeyType, string> = {
  string: '{"userId":9001}',
  hash: '{"field":"value"}',
  list: '["job-1","job-2"]',
  set: '["feature.dashboard","feature.checkout"]',
  zset: '[{"member":"us-east","score":91.2}]',
};

const editableFormats = new Set<RedisValueFormat>(["auto", "utf8", "json", "raw"]);
const dangerousCommands = new Set([
  "SET",
  "MSET",
  "DEL",
  "UNLINK",
  "EXPIRE",
  "PERSIST",
  "HSET",
  "HDEL",
  "LPUSH",
  "RPUSH",
  "LPOP",
  "RPOP",
  "SADD",
  "SREM",
  "ZADD",
  "ZREM",
  "XADD",
  "XDEL",
  "FLUSHDB",
  "FLUSHALL",
  "CONFIG",
  "SHUTDOWN",
]);
const cliHistoryStorageKey = "middleware-studio.redis-cli-history.v1";
const writeCommands = new Set([
  "SET",
  "MSET",
  "DEL",
  "UNLINK",
  "EXPIRE",
  "PERSIST",
  "HSET",
  "HDEL",
  "LPUSH",
  "RPUSH",
  "LPOP",
  "RPOP",
  "SADD",
  "SREM",
  "ZADD",
  "ZREM",
  "XADD",
  "XDEL",
  "FLUSHDB",
  "FLUSHALL",
  "CONFIG",
  "SHUTDOWN",
]);

interface CliSuggestion {
  id: string;
  title: string;
  subtitle: string;
  value: string;
  source: "current" | "helper" | "history";
  riskLevel: string;
}

type Translator = (key: string, values?: Record<string, string | number | null | undefined>) => string;

function keyFromResource(resourceId: string | null) {
  return resourceId?.startsWith("key:") ? resourceId.slice(4) : null;
}

function fmtSlowlog(value: number) {
  if (value >= 1000) {
    return `${(value / 1000).toFixed(1)} ms`;
  }
  return `${value} us`;
}

function formatLabel(value: RedisValueFormat, t: Translator) {
  switch (value) {
    case "auto":
      return t("redis.format.auto");
    case "utf8":
      return t("redis.format.utf8");
    case "gbk":
      return t("redis.format.gbk");
    case "json":
      return t("redis.format.json");
    case "hex":
      return t("redis.format.hex");
    case "ascii":
      return t("redis.format.ascii");
    case "base64":
      return t("redis.format.base64");
    case "raw":
      return t("redis.format.raw");
  }
}

function readCliHistory() {
  if (typeof localStorage === "undefined") {
    return [] as string[];
  }

  try {
    const raw = localStorage.getItem(cliHistoryStorageKey);
    return raw ? (JSON.parse(raw) as string[]) : [];
  } catch {
    return [];
  }
}

function commandName(input: string) {
  return input.trim().split(/\s+/)[0]?.toUpperCase() ?? "";
}

function requiresDangerConfirmation(input: string) {
  return dangerousCommands.has(commandName(input));
}

function inferSearchMode(pattern: string, currentMode: RedisSearchMode) {
  return /[*?]/.test(pattern) ? "pattern" : currentMode;
}

function commandRiskLevel(input: string) {
  if (!input.trim()) {
    return "safe";
  }

  return dangerousCommands.has(commandName(input))
    ? "danger"
    : writeCommands.has(commandName(input))
      ? "guarded"
      : "safe";
}

function commandRiskLabel(riskLevel: string, readonly: boolean, t: Translator) {
  if (readonly && riskLevel !== "safe") {
    return t("sidebar.readOnly");
  }

  switch (riskLevel) {
    case "danger":
      return t("redis.dangerousWrite");
    case "guarded":
      return t("redis.writeCommand");
    default:
      return t("redis.readCommand");
  }
}

function commandExampleForKey(detail: RedisKeyDetail) {
  switch (detail.keyType) {
    case "hash":
      return `HGETALL ${detail.key}`;
    case "list":
      return `LRANGE ${detail.key} 0 50`;
    case "set":
      return `SMEMBERS ${detail.key}`;
    case "zset":
      return `ZRANGE ${detail.key} 0 50 WITHSCORES`;
    case "stream":
      return `XRANGE ${detail.key} - + COUNT 20`;
    default:
      return `GET ${detail.key}`;
  }
}

function buildCurrentKeySuggestions(detail: RedisKeyDetail | null, t: Translator): CliSuggestion[] {
  if (!detail) {
    return [];
  }

  const items = [
    {
      title: t("redis.readCurrentKey"),
      subtitle: commandExampleForKey(detail),
      value: commandExampleForKey(detail),
      riskLevel: "safe",
    },
    {
      title: t("redis.inspectKeyType"),
      subtitle: `TYPE ${detail.key}`,
      value: `TYPE ${detail.key}`,
      riskLevel: "safe",
    },
    {
      title: t("redis.checkTtl"),
      subtitle: `TTL ${detail.key}`,
      value: `TTL ${detail.key}`,
      riskLevel: "safe",
    },
    {
      title: t("redis.deleteKey"),
      subtitle: `DEL ${detail.key}`,
      value: `DEL ${detail.key}`,
      riskLevel: "danger",
    },
  ];

  return items.map((item, index) => ({
    id: `current:${detail.key}:${index}`,
    title: item.title,
    subtitle: item.subtitle,
    value: item.value,
    source: "current",
    riskLevel: item.riskLevel,
  }));
}

function insertStatement(current: string, statement: string) {
  return current.trim() ? `${current.trimEnd()}\n${statement}` : statement;
}

function errorCommandResult(statement: string, error: unknown, t: Translator): RedisCommandResult {
  const message = error instanceof Error ? error.message : t("redis.commandFailed");
  return {
    statement,
    summary: t("redis.commandFailed"),
    durationMs: 0,
    rawOutput: message,
    jsonOutput: JSON.stringify({ error: message }, null, 2),
    table: null,
    error: message,
  };
}

function CommandResult({ result, mode, t }: { result: RedisCommandResult | null; mode: RedisCommandDisplayMode; t: Translator }) {
  if (!result) {
    return <p className="redis-muted">{t("redis.runCommandToSeeOutput")}</p>;
  }

  const textOutput = mode === "json" ? result.jsonOutput ?? result.rawOutput : result.rawOutput;

  return (
    <div className="redis-result-card">
      <strong>{result.summary}</strong>
      {mode === "table" && result.table ? (
        <div className="redis-result-table">
          <div className="redis-result-row header">{result.table.columns.map((column) => <span key={column}>{column}</span>)}</div>
          {result.table.rows.map((row, index) => (
            <div key={`${index}-${row.join("|")}`} className="redis-result-row">{row.map((cell, cellIndex) => <span key={`${index}-${cellIndex}`}>{cell}</span>)}</div>
          ))}
        </div>
      ) : null}
      {mode !== "table" || !result.table ? <pre>{textOutput}</pre> : null}
      {result.error ? <p className="redis-create-error">{result.error}</p> : null}
    </div>
  );
}

export function RedisWorkspace(props: Props) {
  const { t, environmentLabel } = useI18n();
  const {
    connection,
    connections,
    browse,
    detail,
    selectedKeyIds,
    slowlog,
    health,
    selectedDatabase,
    selectedResourceId,
    tab,
    runtimeMode,
    onTabChange,
    onSelectConnection,
    onSelectDatabase,
    onBrowseChange,
    onSelectResource,
    onToggleKeySelection,
    onToggleAllVisibleKeys,
    onClearKeySelection,
    onClearSelection,
    onRunHealthCheck,
    onEditConnection,
    onDeleteConnection,
    onRefresh,
    onLoadMore,
    onSaveValue,
    onCreateKey,
    onUpdateTtl,
    onDeleteKey,
    onPreviewBulkDelete,
    onRunBulkDelete,
    onRunCli,
    onRunWorkbench,
    onRefreshSlowlog,
    onRefreshStream,
  } = props;
  const [searchValue, setSearchValue] = useState(browse?.pattern ?? "");
  const [searchMode, setSearchMode] = useState<RedisSearchMode>(browse?.searchMode ?? "pattern");
  const [typeFilter, setTypeFilter] = useState<RedisKeyType | "all">(browse?.typeFilter ?? "all");
  const [format, setFormat] = useState<RedisValueFormat>("raw");
  const [editorValue, setEditorValue] = useState("");
  const [ttlValue, setTtlValue] = useState("");
  const [bulkPattern, setBulkPattern] = useState("");
  const [bulkPreview, setBulkPreview] = useState<RedisBulkDeleteResult | null>(null);
  const [cliInput, setCliInput] = useState("SCAN 0 MATCH session:* COUNT 20");
  const [cliMode, setCliMode] = useState<RedisCommandDisplayMode>("table");
  const [cliHistory, setCliHistory] = useState<string[]>(() => readCliHistory());
  const [cliResults, setCliResults] = useState<RedisCommandResult[]>([]);
  const [workbenchInput, setWorkbenchInput] = useState("GET session:2048\nXRANGE orders.stream - + COUNT 2");
  const [workbenchMode, setWorkbenchMode] = useState<RedisCommandDisplayMode>("table");
  const [workbenchResult, setWorkbenchResult] = useState<RedisWorkbenchResult | null>(null);
  const [helperEntries, setHelperEntries] = useState<RedisHelperEntry[]>([]);
  const [helperQuery, setHelperQuery] = useState("");
  const [streamDraft, setStreamDraft] = useState('{"orderId":"ord_482903","status":"packed"}');
  const [monitorId, setMonitorId] = useState<string | null>(null);
  const [monitor, setMonitor] = useState<RedisMonitorSnapshot | null>(null);
  const [createType, setCreateType] = useState<RedisCreateKeyType>("string");
  const [createKey, setCreateKey] = useState("");
  const [createValue, setCreateValue] = useState(createExamples.string);
  const [createTtl, setCreateTtl] = useState("");

  useEffect(() => {
    setSearchValue(browse?.pattern ?? "");
    setSearchMode(browse?.searchMode ?? "pattern");
    setTypeFilter(browse?.typeFilter ?? "all");
    setBulkPattern(browse?.pattern ?? "");
  }, [browse?.pattern, browse?.searchMode, browse?.typeFilter]);

  useEffect(() => {
    if (!detail) {
      setEditorValue("");
      setTtlValue("");
      return;
    }
    setFormat(detail.defaultFormat);
    setEditorValue(detail.formatPreviews[detail.defaultFormat]);
    setTtlValue(detail.ttlSeconds?.toString() ?? "");
  }, [detail?.key, detail?.defaultFormat, detail?.ttlSeconds]);

  useEffect(() => {
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(cliHistoryStorageKey, JSON.stringify(cliHistory.slice(0, 12)));
    }
  }, [cliHistory]);

  useEffect(() => {
    let active = true;
    void desktopApi.listRedisHelperEntries().then((entries) => active && setHelperEntries(entries));
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!monitorId) {
      return;
    }
    const timer = window.setInterval(() => void desktopApi.pollRedisMonitor(monitorId).then(setMonitor), 2000);
    return () => window.clearInterval(timer);
  }, [monitorId]);

  const key = keyFromResource(selectedResourceId);
  const helperMatches = helperEntries.filter((entry) => {
    const needle = helperQuery.trim().toLowerCase();
    return !needle || entry.command.toLowerCase().includes(needle) || entry.summary.toLowerCase().includes(needle);
  });
  const visibleKeys = browse?.keySummaries.map((summary) => summary.key) ?? [];
  const allVisibleSelected = visibleKeys.length > 0 && visibleKeys.every((value) => selectedKeyIds.includes(value));
  const selectedCount = selectedKeyIds.length;
  const canEditCurrentFormat = Boolean(detail?.editable && runtimeMode === "desktop" && editableFormats.has(format));
  const bulkScope = selectedCount > 0 ? t("redis.selectedKeysTarget", { count: selectedCount }) : t("redis.currentFilter");
  const bulkPreviewKeys = useMemo(() => bulkPreview?.sampleKeys.join(", ") ?? "", [bulkPreview]);
  const currentKeySuggestions = useMemo(() => buildCurrentKeySuggestions(detail, t), [detail, t]);
  const cliRiskLevel = commandRiskLevel(cliInput);
  const cliRiskLabel = commandRiskLabel(cliRiskLevel, connection.readonly, t);
  const databaseOptionCount = Math.max(browse?.capability.dbCount ?? 16, selectedDatabase + 1);
  const canRunBulkDelete = !connection.readonly && runtimeMode === "desktop";
  const helperSuggestions = useMemo(() => {
    const needle = cliInput.trim().toLowerCase();
    const items: CliSuggestion[] = [];

    for (const suggestion of currentKeySuggestions) {
      items.push(suggestion);
    }

    for (const entry of helperEntries) {
      const matches =
        !needle ||
        entry.command.toLowerCase().includes(needle) ||
        entry.syntax.toLowerCase().includes(needle) ||
        entry.summary.toLowerCase().includes(needle);
      if (!matches) {
        continue;
      }
      items.push({
        id: `helper:${entry.command}`,
        title: entry.command,
        subtitle: entry.syntax,
        value: entry.example,
        source: "helper",
        riskLevel: entry.riskLevel,
      });
    }

    for (const statement of cliHistory) {
      const matches = !needle || statement.toLowerCase().includes(needle);
      if (!matches) {
        continue;
      }
      items.push({
        id: `history:${statement}`,
        title: commandName(statement) || statement,
        subtitle: statement,
        value: statement,
        source: "history",
        riskLevel: commandRiskLevel(statement),
      });
    }

    const unique = new Map<string, CliSuggestion>();
    for (const item of items) {
      if (!unique.has(item.value)) {
        unique.set(item.value, item);
      }
    }

    return [...unique.values()].slice(0, 8);
  }, [cliInput, cliHistory, currentKeySuggestions, helperEntries]);

  async function submitBrowse() {
    onBrowseChange({
      database: selectedDatabase,
      pattern: searchValue,
      searchMode: inferSearchMode(searchValue, searchMode),
      typeFilter,
      viewMode: "tree",
    });
  }

  function rememberCliStatement(statement: string) {
    const normalized = statement.trim();
    if (!normalized) {
      return;
    }
    setCliHistory((current) => [normalized, ...current.filter((item) => item !== normalized)].slice(0, 12));
  }

  async function addStreamEntry() {
    if (!detail || detail.keyType !== "stream") {
      return;
    }
    await desktopApi.addRedisStreamEntry(connection.id, selectedDatabase, detail.key, streamDraft);
    await onRefreshStream();
  }

  async function deleteStreamEntry(entryId: string) {
    if (!detail || detail.keyType !== "stream") {
      return;
    }
    if (!window.confirm(t("redis.deleteStreamEntryConfirm", { entryId }))) {
      return;
    }
    await desktopApi.deleteRedisStreamEntry(connection.id, selectedDatabase, detail.key, entryId);
    await onRefreshStream();
  }

  async function previewBulkDelete() {
    const pattern = selectedCount > 0 ? "" : bulkPattern.trim() || browse?.pattern || "";
    const preview = await onPreviewBulkDelete(pattern, typeFilter);
    setBulkPreview(preview);
  }

  async function runBulkDelete() {
    if (selectedCount === 0 && !bulkPattern.trim() && !browse?.pattern.trim()) {
      return;
    }
    const effectivePattern = selectedCount > 0 ? "" : bulkPattern.trim() || browse?.pattern || "";
    const targetLabel = selectedCount > 0 ? t("redis.selectedKeysTarget", { count: selectedCount }) : effectivePattern || t("redis.currentFilter");
    if (!window.confirm(t("redis.deleteTargetConfirm", { target: targetLabel }))) {
      return;
    }
    const result = await onRunBulkDelete(effectivePattern, typeFilter);
    setBulkPreview(result);
  }

  async function runCliCommand() {
    if (!cliInput.trim()) {
      return;
    }
    if (requiresDangerConfirmation(cliInput) && !window.confirm(t("redis.dangerousCommandConfirm", { command: commandName(cliInput) }))) {
      return;
    }
    try {
      const result = await onRunCli(cliInput, cliMode);
      setCliResults((current) => [result, ...current].slice(0, 10));
    } catch (error) {
      setCliResults((current) => [errorCommandResult(cliInput, error, t), ...current].slice(0, 10));
    }
    rememberCliStatement(cliInput);
  }

  async function runWorkbench() {
    const statements = splitRedisStatements(workbenchInput);
    if (statements.length === 0) {
      return;
    }
    const dangerous = statements.find(requiresDangerConfirmation);
    if (dangerous && !window.confirm(t("redis.dangerousWorkbenchConfirm", { command: commandName(dangerous) }))) {
      return;
    }
    try {
      const result = await onRunWorkbench(workbenchInput, workbenchMode);
      setWorkbenchResult(result);
    } catch (error) {
      setWorkbenchResult({
        statements: statements.map((statement) => errorCommandResult(statement, error, t)),
      });
    }
    statements.forEach(rememberCliStatement);
  }

  function useCliSuggestion(value: string) {
    setCliInput(value);
  }

  function insertWorkbenchSuggestion(value: string) {
    setWorkbenchInput((current) => insertStatement(current, value));
  }

  function handleCliKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      void runCliCommand();
      return;
    }

    if (event.key === "Tab" && helperSuggestions.length > 0) {
      event.preventDefault();
      useCliSuggestion(helperSuggestions[0].value);
    }
  }

  function handleWorkbenchKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      void runWorkbench();
    }
  }

  return (
    <section className="redis-v2-workspace">
      <header className="redis-shell-header">
        <div className="redis-shell-breadcrumbs">
          <span className="redis-crumb-link">{t("redis.breadcrumbsDatabases")}</span>
          <span className="redis-crumb-divider">/</span>
          <label className="redis-connection-picker">
            <select value={connection.id} onChange={(event) => onSelectConnection(event.target.value)}>
                  {connections.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
            </select>
            <ChevronDown size={14} />
          </label>
          <label className="redis-db-picker">
            <select value={selectedDatabase} onChange={(event) => onSelectDatabase(Number(event.target.value))}>
              {Array.from({ length: databaseOptionCount }, (_, index) => (
                <option key={index} value={index}>
                  db{index}
                </option>
              ))}
            </select>
            <ChevronDown size={14} />
          </label>
          <span className={clsx("redis-env-pill", connection.environment === "production" && "danger")}>{environmentLabel(connection.environment)}</span>
        </div>
        <div className="redis-shell-actions">
          <button className="redis-icon-action" type="button" onClick={onRunHealthCheck}><Play size={15} /></button>
          <button className="redis-icon-action" type="button" onClick={onEditConnection}><PencilLine size={15} /></button>
          <button className="redis-icon-action danger" type="button" onClick={onDeleteConnection}><Trash2 size={15} /></button>
        </div>
      </header>

      <div className="redis-detail-tabs redis-surface-tabs">
        <button type="button" className={clsx("redis-detail-tab", tab === "overview" && "active")} onClick={() => onTabChange("overview")}>{t("redis.tabs.browser")}</button>
        <button type="button" className={clsx("redis-detail-tab", tab === "explorer" && "active")} onClick={() => onTabChange("explorer")}><Waves size={14} />{t("redis.tabs.stream")}</button>
        <button type="button" className={clsx("redis-detail-tab", tab === "actions" && "active")} onClick={() => onTabChange("actions")}><TerminalSquare size={14} />{t("redis.tabs.cli")}</button>
        <button type="button" className={clsx("redis-detail-tab", tab === "diagnostics" && "active")} onClick={() => onTabChange("diagnostics")}>{t("redis.tabs.monitor")}</button>
      </div>

      {tab === "overview" ? (
        <div className="redis-v2-layout">
          <section className="redis-key-browser-panel">
            <div className="redis-command-bar redis-command-bar-v2">
              <div className="redis-command-group">
                <label className="redis-type-picker">
                  <select value={searchMode} onChange={(event) => setSearchMode(event.target.value as RedisSearchMode)}>
                    <option value="pattern">{t("redis.searchModePattern")}</option>
                    <option value="fuzzy">{t("redis.searchModeFuzzy")}</option>
                  </select>
                  <ChevronDown size={14} />
                </label>
                <label className="redis-type-picker">
                  <select value={typeFilter} onChange={(event) => setTypeFilter(event.target.value as RedisKeyType | "all")}>
                    <option value="all">{t("redis.typeAll")}</option>
                    <option value="string">{t("redis.typeString")}</option>
                    <option value="hash">{t("redis.typeHash")}</option>
                    <option value="list">{t("redis.typeList")}</option>
                    <option value="set">{t("redis.typeSet")}</option>
                    <option value="zset">{t("redis.typeZSet")}</option>
                    <option value="stream">{t("redis.typeStream")}</option>
                    <option value="json">{t("redis.typeJson")}</option>
                  </select>
                  <ChevronDown size={14} />
                </label>
                <form className="redis-key-filter" onSubmit={(event) => { event.preventDefault(); void submitBrowse(); }}>
                  <Search size={16} />
                  <input value={searchValue} onChange={(event) => setSearchValue(event.target.value)} placeholder={t("redis.searchPlaceholder")} />
                </form>
              </div>
              <div className="redis-command-actions">
                <button className="redis-action-button" type="button" onClick={() => void submitBrowse()}>{t("redis.apply")}</button>
                <button className="redis-action-button" type="button" onClick={onRefresh}><RefreshCw size={15} />{t("common.refresh")}</button>
                <button className="redis-action-button" type="button" onClick={onLoadMore} disabled={!browse?.nextCursor}>{t("redis.loadMore")}</button>
              </div>
            </div>
            <div className="redis-search-hint-bar">
              <span>{inferSearchMode(searchValue, searchMode) === "pattern" ? t("redis.patternHint") : t("redis.fuzzyHint")}</span>
              <span>{t("redis.currentDb", { db: selectedDatabase })}</span>
            </div>

            <div className="redis-selection-bar">
              <span>{selectedCount ? t("redis.selectedCount", { count: selectedCount }) : t("redis.visibleKeys", { count: browse?.loadedCount ?? 0 })}</span>
              <div className="redis-inline-actions compact">
                <button className="redis-action-button" type="button" onClick={() => onToggleAllVisibleKeys(!allVisibleSelected)} disabled={visibleKeys.length === 0}>
                  {allVisibleSelected ? t("redis.clearVisible") : t("redis.selectVisible")}
                </button>
                <button className="redis-action-button" type="button" onClick={onClearKeySelection} disabled={selectedCount === 0}>{t("redis.clearPicked")}</button>
                {browse?.searchPartial ? <span className="redis-selection-hint">{t("redis.partialSearch")}</span> : null}
              </div>
            </div>

            <div className="redis-v2-list">
              {browse?.keySummaries.map((summary) => (
                <div key={summary.key} className={clsx("redis-v2-list-item", key === summary.key && "active")}>
                  <div className="redis-v2-list-top">
                    <label className="redis-check-row" onClick={(event) => event.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={selectedKeyIds.includes(summary.key)}
                        onChange={(event) => onToggleKeySelection(summary.key, event.target.checked)}
                      />
                      <span>{summary.key}</span>
                    </label>
                    <button type="button" className="redis-action-button" onClick={() => onSelectResource(summary.id)}>{t("common.open")}</button>
                  </div>
                  <span>{summary.meta}</span>
                </div>
              ))}
            </div>
          </section>

          <section className="redis-detail-panel">
            {detail ? (
              <div className="redis-detail-scroll">
                <div className="redis-detail-card">
                  <div className="redis-card-header">
                    <div><h3>{detail.key}</h3><span>{detail.keyType} | {detail.encoding ?? "--"} | {detail.size ?? "--"} {t("redis.bytes")}</span></div>
                    <button className="redis-icon-action" type="button" onClick={() => void onClearSelection()}><X size={15} /></button>
                  </div>
                  <div className="redis-inline-actions">
                    <label className="redis-type-picker">
                      <select value={format} onChange={(event) => {
                        const next = event.target.value as RedisValueFormat;
                        setFormat(next);
                        setEditorValue(detail.formatPreviews[next]);
                      }}>
                        {detail.availableFormats.map((value) => <option key={value} value={value}>{formatLabel(value, t)}</option>)}
                      </select>
                      <ChevronDown size={14} />
                    </label>
                    <button className="redis-action-button" type="button" onClick={onRefresh}>{t("common.reload")}</button>
                  </div>
                  <textarea className="redis-editor" rows={14} value={editorValue} onChange={(event) => setEditorValue(event.target.value)} disabled={!canEditCurrentFormat} />
                  <div className="redis-inline-actions">
                    <button className="redis-primary-button" type="button" onClick={() => void onSaveValue(editorValue)} disabled={!canEditCurrentFormat}>{t("redis.saveValue")}</button>
                    <input value={ttlValue} onChange={(event) => setTtlValue(event.target.value)} placeholder={t("redis.ttlSecondsPlaceholder")} />
                    <button className="redis-action-button" type="button" onClick={() => void onUpdateTtl(ttlValue.trim() ? Number(ttlValue) : null)}>{t("redis.updateTtl")}</button>
                    <button className="redis-action-button danger" type="button" onClick={() => void onDeleteKey()}>{t("redis.deleteKey")}</button>
                  </div>
                  {!canEditCurrentFormat && detail.editable ? <p className="redis-muted">{t("redis.switchFormatToEdit")}</p> : null}
                </div>

                <div className="redis-detail-card">
                  <div className="redis-card-header"><h3>{t("redis.bulkDelete")}</h3><span>{bulkScope}</span></div>
                  <input value={bulkPattern} onChange={(event) => setBulkPattern(event.target.value)} placeholder={t("redis.bulkDeletePlaceholder")} disabled={selectedCount > 0} />
                  <div className="redis-inline-actions">
                    <button className="redis-action-button" type="button" onClick={() => void previewBulkDelete()}>{t("common.preview")}</button>
                    <button className="redis-action-button danger" type="button" onClick={() => void runBulkDelete()} disabled={!canRunBulkDelete || (selectedCount === 0 && !bulkPattern.trim() && !browse?.pattern.trim())}>{t("common.delete")}</button>
                  </div>
                  {bulkPreview ? (
                    <p>{bulkPreview.dryRun ? t("redis.matchedKeys", { count: bulkPreview.matched }) : t("redis.deletedKeys", { count: bulkPreview.deleted })}{bulkPreviewKeys ? ` | ${bulkPreviewKeys}` : ""}</p>
                  ) : null}
                  {!canRunBulkDelete ? <p className="redis-muted">{t("redis.bulkDeleteLocked")}</p> : null}
                </div>

                <div className="redis-detail-card">
                  <div className="redis-card-header"><h3>{t("redis.createKey")}</h3><span>{runtimeMode === "desktop" ? t("redis.desktopWrite") : t("redis.previewLocked")}</span></div>
                  <div className="redis-inline-actions">
                    <label className="redis-type-picker">
                      <select value={createType} onChange={(event) => {
                        const next = event.target.value as RedisCreateKeyType;
                        setCreateType(next);
                        setCreateValue(createExamples[next]);
                      }}>
                        <option value="string">{t("redis.typeString")}</option>
                        <option value="hash">{t("redis.typeHash")}</option>
                        <option value="list">{t("redis.typeList")}</option>
                        <option value="set">{t("redis.typeSet")}</option>
                        <option value="zset">{t("redis.typeZSet")}</option>
                      </select>
                      <ChevronDown size={14} />
                    </label>
                    <input value={createKey} onChange={(event) => setCreateKey(event.target.value)} placeholder={t("redis.createKeyPlaceholder")} />
                    <input value={createTtl} onChange={(event) => setCreateTtl(event.target.value)} placeholder={t("redis.ttlPlaceholder")} />
                  </div>
                  <textarea className="redis-editor" rows={6} value={createValue} onChange={(event) => setCreateValue(event.target.value)} />
                  <button className="redis-primary-button" type="button" onClick={() => void onCreateKey({ type: createType, key: createKey, value: createValue, ttlSeconds: createTtl.trim() ? Number(createTtl) : null })}>{t("redis.createKeyButton")}</button>
                </div>
              </div>
            ) : <div className="redis-detail-empty"><p>{t("redis.selectKey")}</p></div>}
          </section>
        </div>
      ) : null}

      {tab === "explorer" ? (
        <div className="redis-tab-layout">
          {detail?.keyType === "stream" && detail.streamState ? (
            <>
              <div className="redis-detail-card">
                <div className="redis-card-header"><h3>{t("redis.consumerGroups")}</h3><button className="redis-action-button" type="button" onClick={() => void onRefreshStream()}>{t("common.refresh")}</button></div>
                {detail.streamState.groups.map((group) => <p key={group.name}>{group.name} | {t("redis.consumers", { count: group.consumers })} | {t("redis.pending", { count: group.pending })} | {t("redis.lag", { value: group.lag ?? "--" })}</p>)}
              </div>
              <div className="redis-detail-card">
                <div className="redis-card-header"><h3>{t("redis.xaddTestEntry")}</h3><span>{connection.readonly ? t("redis.locked") : t("redis.writable")}</span></div>
                <textarea className="redis-editor" rows={6} value={streamDraft} onChange={(event) => setStreamDraft(event.target.value)} />
                <button className="redis-primary-button" type="button" onClick={() => void addStreamEntry()} disabled={connection.readonly || runtimeMode !== "desktop"}>{t("redis.addEntry")}</button>
              </div>
              <div className="redis-detail-card">
                <h3>{t("redis.recentEntries")}</h3>
                {detail.streamState.entries.map((entry) => (
                  <div key={entry.label} className="redis-v2-list-item">
                    <strong>{entry.label}</strong>
                    <span>{entry.value}</span>
                    <button className="redis-action-button danger" type="button" onClick={() => void deleteStreamEntry(entry.label)} disabled={connection.readonly}>{t("common.delete")}</button>
                  </div>
                ))}
              </div>
            </>
          ) : <div className="redis-detail-empty"><p>{t("redis.selectStreamKey")}</p></div>}
        </div>
      ) : null}

      {tab === "actions" ? (
        <div className="redis-tab-layout">
          <div className="redis-detail-card redis-cli-card">
            <div className="redis-card-header">
              <div>
                <h3>{t("redis.redisCli")}</h3>
                <span className="redis-muted">{t("redis.redisCliHint")}</span>
              </div>
              <div className="redis-inline-actions compact">
                <label className="redis-type-picker">
                  <select value={cliMode} onChange={(event) => setCliMode(event.target.value as RedisCommandDisplayMode)}>
                    <option value="table">{t("redis.responseTable")}</option>
                    <option value="json">{t("redis.responseJson")}</option>
                    <option value="raw">{t("redis.responseRaw")}</option>
                  </select>
                  <ChevronDown size={14} />
                </label>
                <button className="redis-action-button" type="button" onClick={() => setCliResults([])} disabled={cliResults.length === 0}>{t("redis.clearOutput")}</button>
              </div>
            </div>
            <div className={clsx("redis-command-status", cliRiskLevel)}>
              <strong>{cliRiskLabel}</strong>
              <span>{connection.readonly && cliRiskLevel !== "safe" ? t("redis.readOnlyBlocked") : t("redis.currentConnectionCommand")}</span>
            </div>
            <textarea className="redis-editor redis-cli-input" rows={4} value={cliInput} onChange={(event) => setCliInput(event.target.value)} onKeyDown={handleCliKeyDown} />
            <div className="redis-inline-actions">
              <button className="redis-primary-button" type="button" onClick={() => void runCliCommand()}>{t("redis.runCommand")}</button>
              {detail ? <button className="redis-action-button" type="button" onClick={() => setCliInput(commandExampleForKey(detail))}>{t("redis.useCurrentKey")}</button> : null}
              {cliHistory.slice(0, 4).map((statement) => <button key={statement} className="redis-action-button" type="button" onClick={() => setCliInput(statement)}>{statement}</button>)}
            </div>
            {helperSuggestions.length > 0 ? (
              <div className="redis-suggestion-list">
                {helperSuggestions.map((suggestion) => (
                  <button key={suggestion.id} type="button" className="redis-suggestion-item" onClick={() => useCliSuggestion(suggestion.value)}>
                    <strong>{suggestion.title}</strong>
                    <span>{suggestion.subtitle}</span>
                    <small>{suggestion.source === "current" ? t("redis.suggestionCurrent") : suggestion.source === "history" ? t("redis.suggestionHistory") : t("redis.suggestionHelper")}</small>
                  </button>
                ))}
              </div>
            ) : null}
            <div className="redis-result-stack">
              {cliResults.length > 0 ? cliResults.map((result, index) => <CommandResult key={`${result.statement}-${result.durationMs}-${index}`} result={result} mode={cliMode} t={t} />) : <CommandResult result={null} mode={cliMode} t={t} />}
            </div>
          </div>
          <div className="redis-detail-card redis-workbench-card">
            <div className="redis-card-header">
              <div>
                <h3>{t("redis.workbench")}</h3>
                <span className="redis-muted">{t("redis.workbenchHint")}</span>
              </div>
              <div className="redis-inline-actions compact">
                <label className="redis-type-picker">
                  <select value={workbenchMode} onChange={(event) => setWorkbenchMode(event.target.value as RedisCommandDisplayMode)}>
                    <option value="table">{t("redis.responseTable")}</option>
                    <option value="json">{t("redis.responseJson")}</option>
                    <option value="raw">{t("redis.responseRaw")}</option>
                  </select>
                  <ChevronDown size={14} />
                </label>
                <button className="redis-action-button" type="button" onClick={() => setWorkbenchResult(null)} disabled={!workbenchResult}>{t("redis.clearOutput")}</button>
              </div>
            </div>
            <textarea className="redis-editor" rows={8} value={workbenchInput} onChange={(event) => setWorkbenchInput(event.target.value)} onKeyDown={handleWorkbenchKeyDown} />
            <div className="redis-inline-actions">
              <button className="redis-primary-button" type="button" onClick={() => void runWorkbench()}>{t("redis.runWorkbench")}</button>
              {currentKeySuggestions.map((suggestion) => (
                <button key={suggestion.id} className="redis-action-button" type="button" onClick={() => insertWorkbenchSuggestion(suggestion.value)}>
                  {suggestion.title}
                </button>
              ))}
            </div>
            <div className="redis-result-stack">
              {workbenchResult?.statements.map((statement, index) => <CommandResult key={`${statement.statement}-${statement.durationMs}-${index}`} result={statement} mode={workbenchMode} t={t} />)}
            </div>
          </div>
          <div className="redis-detail-card">
            <div className="redis-card-header">
              <div>
                <h3>{t("redis.helperTitle")}</h3>
                <span className="redis-muted">{t("redis.helperHint")}</span>
              </div>
              {key ? <span className="redis-helper-key">{t("redis.currentKey", { key })}</span> : null}
            </div>
            <input value={helperQuery} onChange={(event) => setHelperQuery(event.target.value)} placeholder={t("redis.helperSearchPlaceholder")} />
            {helperMatches.map((entry) => (
              <div key={entry.command} className="redis-v2-list-item">
                <div className="redis-helper-head">
                  <strong>{entry.command}</strong>
                  <span className={clsx("redis-helper-risk", entry.riskLevel)}>{t(`redis.${entry.riskLevel}`)}</span>
                </div>
                <span>{entry.summary}</span>
                <code>{entry.syntax}</code>
                <div className="redis-inline-actions compact">
                  <button className="redis-action-button" type="button" onClick={() => setCliInput(entry.example)}>{t("redis.useExample")}</button>
                  <button className="redis-action-button" type="button" onClick={() => setCliInput(entry.syntax)}>{t("redis.useSyntax")}</button>
                  <button className="redis-action-button" type="button" onClick={() => setWorkbenchInput((current) => insertStatement(current, entry.example))}>{t("redis.addToWorkbench")}</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {tab === "diagnostics" ? (
        <div className="redis-tab-layout">
          <div className="redis-detail-card">
            <div className="redis-card-header"><h3>{t("redis.health")}</h3><button className="redis-action-button" type="button" onClick={onRunHealthCheck}>{t("redis.runCheck")}</button></div>
            <p>{health?.summary ?? t("redis.noHealthCheckYet")}</p>
            <p>{browse?.diagnostics.join(" ")}</p>
          </div>
          <div className="redis-detail-card">
            <div className="redis-card-header">
              <h3>{t("redis.liveMonitor")}</h3>
              <div className="redis-inline-actions compact">
                <button className="redis-action-button" type="button" onClick={() => void desktopApi.startRedisMonitor(connection.id, selectedDatabase).then((session) => { setMonitorId(session.sessionId); return desktopApi.pollRedisMonitor(session.sessionId); }).then(setMonitor)}>{t("common.start")}</button>
                <button className="redis-action-button" type="button" onClick={() => monitorId ? void desktopApi.stopRedisMonitor(monitorId).then(() => { setMonitorId(null); setMonitor(null); }) : undefined}>{t("common.stop")}</button>
              </div>
            </div>
            {monitor ? (
              <>
                <div className="redis-metric-strip">
                  {monitor.metrics.map((metric) => <div key={metric.label} className="redis-metric-chip"><strong>{metric.value}</strong><span>{metric.label}</span></div>)}
                </div>
                {monitor.commandSamples.map((sample) => <p key={`${sample.at}-${sample.command}`}>{sample.at} | {sample.command}</p>)}
              </>
            ) : <p className="redis-muted">{t("redis.startMonitorHint")}</p>}
          </div>
          <div className="redis-detail-card">
            <div className="redis-card-header"><h3>{t("redis.slowLog")}</h3><button className="redis-action-button" type="button" onClick={() => void onRefreshSlowlog()}><RefreshCw size={14} />{t("common.refresh")}</button></div>
            {slowlog.map((entry) => <div key={entry.id} className="redis-v2-list-item"><strong>#{entry.id}</strong><span>{fmtSlowlog(entry.durationMicros)} | {entry.command}</span></div>)}
          </div>
        </div>
      ) : null}
    </section>
  );
}
