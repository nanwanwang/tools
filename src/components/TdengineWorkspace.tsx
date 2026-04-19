import clsx from "clsx";
import { useEffect, useMemo, useState } from "react";
import {
  Activity,
  BookmarkPlus,
  Download,
  FileText,
  History,
  PanelRightClose,
  PanelRightOpen,
  PencilLine,
  Play,
  Plus,
  ShieldAlert,
  Star,
  Trash2,
  X,
} from "lucide-react";
import type {
  ConnectionHealth,
  ConnectionRecord,
  ResourceNode,
  TdengineObjectDetail,
  TdengineQueryTab,
  TdengineSavedQuery,
  TdengineSqlSuggestion,
} from "../types";
import {
  buildTdengineAutocompleteSuggestions,
  buildTdengineQuickTemplates,
  filterTdengineResultRows,
  paginateTdengineRows,
  projectTdengineRows,
  sortTdengineResultRows,
  tdengineCellToString,
  tdengineRowsToCsv,
  type TdengineResultSortDirection,
} from "../lib/tdengine";
import { useI18n } from "../i18n";

interface TdengineWorkspaceProps {
  connection: ConnectionRecord;
  detail: TdengineObjectDetail | null;
  health: ConnectionHealth | null;
  tabs: TdengineQueryTab[];
  activeTabId: string | null;
  databaseOptions: string[];
  catalog: ResourceNode[] | null;
  favorites: TdengineSavedQuery[];
  history: string[];
  runtimeMode: "desktop" | "preview";
  onCreateTab: () => void;
  onSelectTab: (id: string) => void;
  onCloseTab: (id: string) => void;
  onSelectDatabase: (database: string) => void;
  onUpdateTabSql: (id: string, sql: string) => void;
  onApplyPreviewSql: () => void;
  onSaveFavorite: () => void;
  onUseFavorite: (favorite: TdengineSavedQuery) => void;
  onDeleteFavorite: (id: string) => void;
  onApplySuggestion: (suggestion: TdengineSqlSuggestion) => void;
  onUseHistoryItem: (sql: string) => void;
  onRunQuery: (id: string) => void;
  onExportResult: (format: "csv" | "json") => void;
  onRunHealthCheck: () => void;
  onEdit: () => void;
  onDelete: () => void;
}

type TdengineSidePanelKey = "detail" | "favorites" | "history" | "diagnostics";
type TdengineAssistantItem = {
  id: string;
  badge: "suggestion" | "template";
  hint: string;
  suggestion: TdengineSqlSuggestion;
};

async function copyText(text: string) {
  if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  if (typeof document === "undefined") {
    throw new Error("Clipboard is not available.");
  }

  const input = document.createElement("textarea");
  input.value = text;
  input.setAttribute("readonly", "true");
  input.style.position = "absolute";
  input.style.left = "-9999px";
  document.body.appendChild(input);
  input.select();
  document.execCommand("copy");
  document.body.removeChild(input);
}

export function TdengineWorkspace({
  connection,
  detail,
  health,
  tabs,
  activeTabId,
  databaseOptions,
  catalog,
  favorites,
  history,
  runtimeMode,
  onCreateTab,
  onSelectTab,
  onCloseTab,
  onSelectDatabase,
  onUpdateTabSql,
  onApplyPreviewSql,
  onSaveFavorite,
  onUseFavorite,
  onDeleteFavorite,
  onApplySuggestion,
  onUseHistoryItem,
  onRunQuery,
  onExportResult,
  onRunHealthCheck,
  onEdit,
  onDelete,
}: TdengineWorkspaceProps) {
  const { t, formatDateTime, environmentLabel } = useI18n();
  const activeTab = tabs.find((tab) => tab.id === activeTabId) ?? tabs[0] ?? null;
  const result = activeTab?.result ?? null;
  const effectiveDatabase = activeTab?.database || connection.databaseName || "";
  const databaseSelectValue = activeTab ? activeTab.database : connection.databaseName || "";
  const normalizedSql = activeTab?.sql.trim().toLowerCase() ?? "";
  const isUseStatementResult =
    result !== null && !result.error && !result.columns.length && result.rowCount === 0 && normalizedSql.startsWith("use ");
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<TdengineResultSortDirection>("asc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [copyStatus, setCopyStatus] = useState<string | null>(null);
  const [resultFilterQuery, setResultFilterQuery] = useState("");
  const [columnFilters, setColumnFilters] = useState<Record<string, string>>({});
  const [visibleColumnNames, setVisibleColumnNames] = useState<string[]>([]);
  const [freezeFirstColumn, setFreezeFirstColumn] = useState(true);
  const [isAssistantOpen, setIsAssistantOpen] = useState(false);
  const [isResultToolsOpen, setIsResultToolsOpen] = useState(false);
  const [activeSidePanel, setActiveSidePanel] = useState<TdengineSidePanelKey>("detail");
  const [isSidePanelCollapsed, setIsSidePanelCollapsed] = useState(true);
  const quickTemplates = useMemo(
    () =>
      buildTdengineQuickTemplates({
        currentDatabase: effectiveDatabase,
        databases: databaseOptions,
        detail,
        resources: catalog,
        favorites,
      }).slice(0, 8),
    [catalog, databaseOptions, detail, effectiveDatabase, favorites],
  );
  const autocompleteSuggestions = useMemo(
    () =>
      buildTdengineAutocompleteSuggestions(activeTab?.sql ?? "", {
        currentDatabase: effectiveDatabase,
        databases: databaseOptions,
        detail,
        resources: catalog,
        favorites,
      }),
    [activeTab?.sql, catalog, databaseOptions, detail, effectiveDatabase, favorites],
  );
  const assistantItems = useMemo<TdengineAssistantItem[]>(() => {
    const items: TdengineAssistantItem[] = [
      ...autocompleteSuggestions.slice(0, 6).map((suggestion, index) => ({
        id: `suggestion:${suggestion.id}:${index}`,
        badge: "suggestion" as const,
        hint: suggestion.kind,
        suggestion,
      })),
      ...quickTemplates.slice(0, 6).map((suggestion, index) => ({
        id: `template:${suggestion.id}:${index}`,
        badge: "template" as const,
        hint: suggestion.kind,
        suggestion,
      })),
    ];

    const unique = new Map<string, TdengineAssistantItem>();
    for (const item of items) {
      const dedupeKey = `${item.badge}:${item.suggestion.sql}`;
      if (!unique.has(dedupeKey)) {
        unique.set(dedupeKey, item);
      }
    }

    return [...unique.values()];
  }, [autocompleteSuggestions, quickTemplates]);
  const topSuggestion = assistantItems[0]?.suggestion ?? null;
  const visibleColumns = useMemo(() => {
    if (!result) {
      return [];
    }

    const visibleSet = new Set(visibleColumnNames);
    return result.columns.filter((column) => visibleSet.has(column.name));
  }, [result, visibleColumnNames]);
  const filteredRows = useMemo(
    () =>
      filterTdengineResultRows(
        result?.rows ?? [],
        visibleColumns.map((column) => column.name),
        resultFilterQuery,
        columnFilters,
      ),
    [columnFilters, result?.rows, resultFilterQuery, visibleColumns],
  );
  const sortedRows = useMemo(
    () => sortTdengineResultRows(filteredRows, sortColumn, sortDirection),
    [filteredRows, sortColumn, sortDirection],
  );
  const pagedRows = useMemo(() => paginateTdengineRows(sortedRows, page, pageSize), [page, pageSize, sortedRows]);
  const firstVisibleColumnName = visibleColumns[0]?.name ?? null;
  const activeColumnFilterCount = useMemo(
    () => visibleColumns.filter((column) => columnFilters[column.name]?.trim()).length,
    [columnFilters, visibleColumns],
  );
  const sidePanelItems = [
    {
      key: "detail" as const,
      label: t("tdengine.sidePanelDetail"),
      description: detail ? `${detail.database}.${detail.objectName}` : t("tdengine.sidePanelDetailDescription"),
      icon: <FileText size={18} />,
    },
    {
      key: "favorites" as const,
      label: t("tdengine.sidePanelFavorites"),
      description: favorites.length
        ? t("tdengine.sidePanelFavoritesDescription", { count: favorites.length })
        : t("tdengine.sidePanelFavoritesEmptyDescription"),
      icon: <Star size={18} />,
    },
    {
      key: "history" as const,
      label: t("tdengine.sidePanelHistory"),
      description: history.length
        ? t("tdengine.sidePanelHistoryDescription", { count: history.length })
        : t("tdengine.sidePanelHistoryEmptyDescription"),
      icon: <History size={18} />,
    },
    {
      key: "diagnostics" as const,
      label: t("tdengine.sidePanelDiagnostics"),
      description: health ? health.status : t("tdengine.sidePanelDiagnosticsDescription"),
      icon: <ShieldAlert size={18} />,
    },
  ];
  const activeSidePanelMeta = sidePanelItems.find((item) => item.key === activeSidePanel) ?? sidePanelItems[0];
  const resultRowSummary = result
    ? filteredRows.length !== result.rowCount
      ? t("tdengine.rowsSummary", { visible: filteredRows.length, total: result.rowCount })
      : t("tdengine.rowsSummarySimple", { count: result.rowCount })
    : t("tdengine.noRows");
  const resultColumnSummary = result
    ? t("tdengine.colsSummary", { visible: visibleColumns.length || result.columns.length, total: result.columns.length })
    : `0 ${t("tdengine.noRows")}`;

  useEffect(() => {
    setPage(1);
    setSortColumn((current) => (current && result?.columns.some((column) => column.name === current) ? current : null));
    setResultFilterQuery("");
    setColumnFilters({});
    setVisibleColumnNames(result?.columns.map((column) => column.name) ?? []);
  }, [activeTab?.id, result?.database, result?.rowCount, result?.columns]);

  useEffect(() => {
    if (!copyStatus) {
      return;
    }

    const timer = window.setTimeout(() => setCopyStatus(null), 1800);
    return () => window.clearTimeout(timer);
  }, [copyStatus]);

  useEffect(() => {
    if (sortColumn && !visibleColumns.some((column) => column.name === sortColumn)) {
      setSortColumn(null);
    }
  }, [sortColumn, visibleColumns]);

  useEffect(() => {
    setColumnFilters((current) => {
      const visibleSet = new Set(visibleColumns.map((column) => column.name));
      const nextEntries = Object.entries(current).filter(
        ([columnName, value]) => visibleSet.has(columnName) && value.trim().length > 0,
      );

      if (
        nextEntries.length === Object.keys(current).length &&
        nextEntries.every(([columnName, value]) => current[columnName] === value)
      ) {
        return current;
      }

      return Object.fromEntries(nextEntries);
    });
  }, [visibleColumns]);

  function handleSort(columnName: string) {
    setPage(1);
    setSortColumn((current) => {
      if (current === columnName) {
        setSortDirection((direction) => (direction === "asc" ? "desc" : "asc"));
        return current;
      }

      setSortDirection("asc");
      return columnName;
    });
  }

  async function handleCopyCell(value: string) {
    try {
      await copyText(value);
      setCopyStatus(t("tdengine.cellCopied"));
    } catch (error) {
      setCopyStatus(error instanceof Error ? error.message : t("tdengine.copyFailed"));
    }
  }

  function handleColumnFilterChange(columnName: string, value: string) {
    setPage(1);
    setColumnFilters((current) => {
      const normalizedValue = value.trim();
      if (!normalizedValue) {
        if (!(columnName in current)) {
          return current;
        }

        const next = { ...current };
        delete next[columnName];
        return next;
      }

      return {
        ...current,
        [columnName]: value,
      };
    });
  }

  function handleClearColumnFilters() {
    setPage(1);
    setColumnFilters({});
  }

  async function handleCopyRow(row: Record<string, unknown>) {
    try {
      await copyText(JSON.stringify(row, null, 2));
      setCopyStatus(t("tdengine.rowCopied"));
    } catch (error) {
      setCopyStatus(error instanceof Error ? error.message : t("tdengine.copyFailed"));
    }
  }

  async function handleCopyVisibleRows(format: "json" | "csv") {
    if (!result || !visibleColumns.length) {
      return;
    }

    try {
      const projectedRows = projectTdengineRows(pagedRows.rows, visibleColumns.map((column) => column.name));
      const content =
        format === "json"
          ? JSON.stringify(projectedRows, null, 2)
          : tdengineRowsToCsv(
              visibleColumns.map((column) => column.name),
              projectedRows,
            );
      await copyText(content);
      setCopyStatus(format === "json" ? t("tdengine.visibleRowsCopiedJson") : t("tdengine.visibleRowsCopiedCsv"));
    } catch (error) {
      setCopyStatus(error instanceof Error ? error.message : t("tdengine.copyFailed"));
    }
  }

  function toggleVisibleColumn(columnName: string) {
    setPage(1);
    setVisibleColumnNames((current) => {
      if (current.includes(columnName)) {
        if (current.length === 1) {
          return current;
        }

        return current.filter((entry) => entry !== columnName);
      }

      if (!result) {
        return current;
      }

      const next = [...current, columnName];
      return result.columns
        .map((column) => column.name)
        .filter((name) => next.includes(name));
    });
  }

  function handleSelectSidePanel(panel: TdengineSidePanelKey) {
    setActiveSidePanel(panel);
    setIsSidePanelCollapsed(false);
  }

  function renderSidePanelContent() {
    switch (activeSidePanel) {
      case "detail":
        return (
          <div className="tdengine-card tdengine-side-panel-card">
            <div className="tdengine-card-header">
              <div>
                <p className="eyebrow">{t("tdengine.objectDetail")}</p>
                <h3>{detail ? `${detail.database}.${detail.objectName}` : t("tdengine.selectTableHint")}</h3>
              </div>
            </div>

            {detail ? (
              <div className="tdengine-detail-stack">
                <div className="details-grid">
                  {detail.metaRows.map((row) => (
                    <div key={row.label}>
                      <span>{row.label}</span>
                      <strong>{row.value}</strong>
                      {row.secondary ? <small>{row.secondary}</small> : null}
                    </div>
                  ))}
                </div>

                <div className="tdengine-field-group">
                  <strong>{t("tdengine.columns")}</strong>
                  <div className="tdengine-field-list">
                    {detail.fields.map((field) => (
                      <div key={field.name} className="tdengine-field-row">
                        <span>{field.name}</span>
                        <small>
                          {field.type}
                          {field.length ? ` (${field.length})` : ""}
                          {field.note ? ` | ${field.note}` : ""}
                        </small>
                      </div>
                    ))}
                  </div>
                </div>

                {detail.tagColumns.length ? (
                  <div className="tdengine-field-group">
                    <strong>{t("tdengine.tagColumns")}</strong>
                    <div className="tdengine-field-list">
                      {detail.tagColumns.map((field) => (
                        <div key={field.name} className="tdengine-field-row">
                          <span>{field.name}</span>
                          <small>
                            {field.type}
                            {field.length ? ` (${field.length})` : ""}
                            {field.note ? ` | ${field.note}` : ""}
                          </small>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}

                {detail.tagValueRows.length ? (
                  <div className="tdengine-field-group">
                    <strong>{t("tdengine.tagValues")}</strong>
                    <div className="tdengine-field-list">
                      {detail.tagValueRows.map((row) => (
                        <div key={row.label} className="tdengine-field-row">
                          <span>{row.label}</span>
                          <small>
                            {row.value}
                            {row.secondary ? ` | ${row.secondary}` : ""}
                          </small>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}

                <div className="tdengine-field-group">
                  <div className="tdengine-section-head">
                    <strong>{t("tdengine.previewSql")}</strong>
                    <button className="ghost-button small" type="button" onClick={onApplyPreviewSql}>
                      {t("tdengine.usePreviewSql")}
                    </button>
                  </div>
                  <pre>{detail.previewSql}</pre>
                </div>

                <div className="tdengine-field-group">
                  <strong>{t("tdengine.ddl")}</strong>
                  <pre>{detail.ddl ?? t("tdengine.noDdl")}</pre>
                </div>
              </div>
            ) : (
              <div className="empty-card tdengine-empty-card">
                <p>{t("tdengine.detailEmpty")}</p>
              </div>
            )}
          </div>
        );
      case "favorites":
        return (
          <div className="tdengine-card tdengine-side-panel-card">
            <div className="tdengine-card-header">
              <div>
                <p className="eyebrow">{t("tdengine.savedSql")}</p>
                <h3>{t("tdengine.favoritesTitle")}</h3>
              </div>
              <button className="ghost-button small" type="button" onClick={onSaveFavorite} disabled={!activeTab?.sql.trim()}>
                <BookmarkPlus size={14} />
                {t("tdengine.saveCurrent")}
              </button>
            </div>

            <div className="tdengine-history-list">
              {favorites.length ? (
                favorites.map((entry) => (
                  <div key={entry.id} className="tdengine-saved-item">
                    <div className="tdengine-saved-head">
                      <strong>{entry.title}</strong>
                      <span className="tag">{t("tdengine.dbTag", { database: entry.database || t("common.none") })}</span>
                    </div>
                    <code>{entry.sql}</code>
                    <div className="tdengine-saved-meta">
                      <span>{formatDateTime(entry.updatedAt)}</span>
                      <div className="tdengine-inline-actions">
                        <button className="ghost-button small" type="button" onClick={() => onUseFavorite(entry)}>
                          {t("tdengine.use")}
                        </button>
                        <button className="ghost-button small danger" type="button" onClick={() => onDeleteFavorite(entry.id)}>
                          {t("common.delete")}
                        </button>
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="empty-card tdengine-empty-card">
                  <p>{t("tdengine.noFavorites")}</p>
                </div>
              )}
            </div>
          </div>
        );
      case "history":
        return (
          <div className="tdengine-card tdengine-side-panel-card">
            <div className="tdengine-card-header">
              <div>
                <p className="eyebrow">{t("tdengine.queryHistory")}</p>
                <h3>{t("tdengine.recentSql")}</h3>
              </div>
            </div>

            <div className="tdengine-history-list">
              {history.length ? (
                history.map((entry, index) => (
                  <button
                    key={`${entry}-${index}`}
                    className="tdengine-history-item"
                    type="button"
                    onClick={() => onUseHistoryItem(entry)}
                    title={entry}
                  >
                    <span>{entry}</span>
                  </button>
                ))
              ) : (
                <div className="empty-card tdengine-empty-card">
                  <p>{t("tdengine.noHistory")}</p>
                </div>
              )}
            </div>
          </div>
        );
      case "diagnostics":
        return (
          <div className="tdengine-card tdengine-side-panel-card tdengine-health-card">
            <div className="tdengine-card-header">
              <div>
                <p className="eyebrow">{t("tdengine.diagnosticsTitle")}</p>
                <h3>{health ? `${health.status} | ${health.summary}` : t("tdengine.noHealthCheckYet")}</h3>
              </div>
            </div>
            <div className="tdengine-health-panel">
              <ShieldAlert size={18} />
              <div>
                <strong>{health?.status ?? t("workspace.notAvailable")}</strong>
                <p>
                  {health
                    ? t("tdengine.checkedAt", { value: formatDateTime(health.checkedAt) })
                    : t("tdengine.tdengineHealthHint")}
                </p>
              </div>
            </div>
            <ul className="diagnostic-list">
              {(health?.details ?? [
                t("tdengine.diagnosticReadonly"),
                t("tdengine.diagnosticNative"),
                t("tdengine.diagnosticWs"),
              ]).map((detailLine) => (
                <li key={detailLine}>{detailLine}</li>
              ))}
            </ul>
          </div>
        );
      default:
        return null;
    }
  }

  return (
    <section className="tdengine-shell">
      <div className="tdengine-header">
        <div>
          <p className="eyebrow">TDengine | {environmentLabel(connection.environment)}</p>
          <h2>{connection.name}</h2>
          <p className="tdengine-subtitle">
            {t("tdengine.headerSubtitle", {
              protocol: connection.protocol === "native" ? t("tdengine.native") : t("tdengine.websocket"),
              host: connection.host,
              port: connection.port,
              database: effectiveDatabase || t("tdengine.noDefaultDatabase"),
            })}
          </p>
        </div>

        <div className="hero-actions">
          <button className="ghost-button small" type="button" onClick={onEdit}>
            <PencilLine size={15} />
            {t("common.edit")}
          </button>
          <button className="ghost-button small" type="button" onClick={onRunHealthCheck}>
            <Activity size={15} />
            {t("common.healthCheck")}
          </button>
          <button className="ghost-button small danger" type="button" onClick={onDelete}>
            <Trash2 size={15} />
            {t("common.delete")}
          </button>
        </div>
      </div>

      <div className="workspace-chip-row">
        <span className="tag">{connection.protocol === "native" ? t("connection.protocolNative") : t("connection.protocolWs")}</span>
        <span className="tag">{runtimeMode === "desktop" ? t("tdengine.desktopRuntime") : t("tdengine.previewMode")}</span>
        <span className={clsx("tag", connection.readonly && "critical")}>{t("tdengine.readOnlySqlOnly")}</span>
        {connection.useTls ? <span className="tag">{t("workspace.tls")}</span> : null}
      </div>

      <div className="tdengine-layout">
        <div className="tdengine-main">
          <div className="tdengine-card tdengine-workbench-card">
            <div className="tdengine-workbench-tabs">
              <div className="tdengine-tabs">
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    className={clsx("tdengine-tab", activeTab?.id === tab.id && "active")}
                    onClick={() => onSelectTab(tab.id)}
                  >
                    <span>{tab.title}</span>
                    {tabs.length > 1 ? (
                      <span
                        className="tdengine-tab-close"
                        onClick={(event) => {
                          event.stopPropagation();
                          onCloseTab(tab.id);
                        }}
                      >
                        <X size={12} />
                      </span>
                    ) : null}
                  </button>
                ))}
              </div>
              <button className="ghost-button small" type="button" onClick={onCreateTab}>
                <Plus size={14} />
                {t("tdengine.newQuery")}
              </button>
            </div>

            <div className="tdengine-workbench-body">
              <section className="tdengine-editor-pane">
                <div className="tdengine-pane-toolbar">
                  <div>
                    <p className="eyebrow">{t("tdengine.sqlEditor")}</p>
                    <h3>{activeTab?.title ?? t("tdengine.noTabSelected")}</h3>
                  </div>
                  <div className="tdengine-inline-actions tdengine-context-actions">
                    <label className="tdengine-select-field">
                      <span>{t("connection.database")}</span>
                      <select
                        value={databaseSelectValue}
                        onChange={(event) => onSelectDatabase(event.target.value)}
                        disabled={!activeTab}
                      >
                        <option value="">{connection.databaseName ? t("tdengine.noOverride") : t("tdengine.noDatabase")}</option>
                        {databaseOptions.map((database) => (
                          <option key={database} value={database}>
                            {database}
                          </option>
                        ))}
                      </select>
                    </label>
                    <button className="ghost-button small" type="button" onClick={onApplyPreviewSql} disabled={!detail?.previewSql}>
                      {t("tdengine.usePreviewSql")}
                    </button>
                    <button
                      className={clsx("ghost-button small", isAssistantOpen && "active")}
                      type="button"
                      onClick={() => setIsAssistantOpen((current) => !current)}
                    >
                      {isAssistantOpen ? t("tdengine.hideAssistant") : t("tdengine.showAssistant")}
                    </button>
                    <button className="ghost-button small" type="button" onClick={onSaveFavorite} disabled={!activeTab?.sql.trim()}>
                      <BookmarkPlus size={14} />
                      {t("tdengine.saveSql")}
                    </button>
                    <button
                      className="primary-button small"
                      type="button"
                      onClick={() => (activeTab ? onRunQuery(activeTab.id) : null)}
                      disabled={!activeTab || activeTab.isRunning}
                    >
                      <Play size={14} />
                      {activeTab?.isRunning ? t("tdengine.running") : t("common.run")}
                    </button>
                  </div>
                </div>

                <div className={clsx("tdengine-editor-layout", isAssistantOpen && "assistant-open")}>
                  <div className="tdengine-editor-main">
                    <textarea
                      className="tdengine-editor"
                      value={activeTab?.sql ?? ""}
                      onChange={(event) => (activeTab ? onUpdateTabSql(activeTab.id, event.target.value) : null)}
                      onKeyDown={(event) => {
                        if (topSuggestion && event.key === "Tab" && !event.shiftKey && !event.metaKey && !event.ctrlKey && !event.altKey) {
                          event.preventDefault();
                          onApplySuggestion(topSuggestion);
                          return;
                        }

                        if (activeTab && event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
                          event.preventDefault();
                          onRunQuery(activeTab.id);
                        }
                      }}
                      placeholder={t("tdengine.sqlPlaceholder")}
                    />

                    <div className="tdengine-editor-footer">
                      <span>{t("tdengine.allowedStatements")}</span>
                      <div className="tdengine-inline-actions">
                        <span>{t("tdengine.keyboardHint")}</span>
                        {topSuggestion ? (
                          <button className="ghost-button small" type="button" onClick={() => onApplySuggestion(topSuggestion)}>
                            {t("tdengine.useSuggestion", { label: topSuggestion.label })}
                          </button>
                        ) : null}
                      </div>
                    </div>
                  </div>

                  {isAssistantOpen ? (
                    <aside className="tdengine-assistant-panel">
                      <div className="tdengine-assistant-panel-header">
                        <div>
                          <strong>{t("tdengine.assistantTitle")}</strong>
                          <p className="tdengine-muted">{t("tdengine.assistantHint")}</p>
                        </div>
                        {topSuggestion ? <span className="tag">{t("tdengine.assistantPrimary")}</span> : null}
                      </div>

                      {assistantItems.length ? (
                        <div className="tdengine-assistant-list">
                          {assistantItems.map((item, index) => (
                            <button
                              key={item.id}
                              type="button"
                              className={clsx("tdengine-assistant-item", index === 0 && "active")}
                              onClick={() => onApplySuggestion(item.suggestion)}
                            >
                              <div className="tdengine-assistant-item-head">
                                <strong>{item.suggestion.label}</strong>
                                <span className="tag">
                                  {item.badge === "suggestion" ? t("tdengine.assistantSuggestion") : t("tdengine.assistantTemplate")}
                                </span>
                              </div>
                              <span className="tdengine-assistant-item-detail">{item.suggestion.detail}</span>
                              <code className="tdengine-assistant-item-snippet">{item.suggestion.sql}</code>
                              <div className="tdengine-assistant-item-foot">
                                <span className="tdengine-muted">{item.hint}</span>
                                <span>{t("tdengine.assistantInsert")}</span>
                              </div>
                            </button>
                          ))}
                        </div>
                      ) : (
                        <div className="empty-card tdengine-empty-card tdengine-assistant-empty">
                          <p>{t("tdengine.assistantEmpty")}</p>
                        </div>
                      )}
                    </aside>
                  ) : null}
                </div>
              </section>

              <section className="tdengine-result-pane">
                <div className="tdengine-pane-toolbar">
                  <div>
                    <p className="eyebrow">{t("tdengine.resultTitle")}</p>
                    <h3>{result?.error ? t("tdengine.resultError") : activeTab?.isRunning ? t("tdengine.resultRunning") : result ? t("tdengine.resultReady") : t("tdengine.resultEmpty")}</h3>
                  </div>
                  <div className="tdengine-inline-actions tdengine-result-header-actions">
                    {result ? <span className="tag">{t("tdengine.dbTag", { database: result?.database || effectiveDatabase || t("common.none") })}</span> : null}
                    {result ? <span className="tag">{resultRowSummary}</span> : null}
                    {result?.columns.length ? <span className="tag">{resultColumnSummary}</span> : null}
                    {result ? <span className="tag">{`${result.durationMs} ms`}</span> : null}
                    {result?.truncated ? <span className="tag critical">{t("tdengine.truncated")}</span> : null}
                    <button
                      className={clsx("ghost-button small", isResultToolsOpen && "active")}
                      type="button"
                      onClick={() => setIsResultToolsOpen((current) => !current)}
                      disabled={!result?.columns.length}
                    >
                      {isResultToolsOpen ? t("tdengine.hideTools") : t("tdengine.viewTools")}
                    </button>
                    <button className="ghost-button small" type="button" onClick={() => onExportResult("csv")} disabled={!result}>
                      <Download size={14} />
                      CSV
                    </button>
                    <button className="ghost-button small" type="button" onClick={() => onExportResult("json")} disabled={!result}>
                      <Download size={14} />
                      JSON
                    </button>
                  </div>
                </div>

                {result?.error ? <div className="tdengine-error-box">{result.error}</div> : null}
                {result?.truncated ? <div className="tdengine-info-box">{t("tdengine.resultTruncated")}</div> : null}
                {copyStatus ? <div className="tdengine-copy-box">{copyStatus}</div> : null}

                {result && result.columns.length ? (
                  <>
                    <div className="tdengine-result-primary-bar">
                      <label className="tdengine-result-search tdengine-result-search-inline">
                        <span>{t("tdengine.find")}</span>
                        <input
                          value={resultFilterQuery}
                          onChange={(event) => {
                            setPage(1);
                            setResultFilterQuery(event.target.value);
                          }}
                          placeholder={t("tdengine.findPlaceholder")}
                        />
                      </label>

                      <div className="tdengine-inline-actions tdengine-result-pagination">
                        <label className="tdengine-select-field tdengine-page-size">
                          <span>{t("tdengine.pageSize")}</span>
                          <select
                            value={pageSize}
                            onChange={(event) => {
                              setPage(1);
                              setPageSize(Number(event.target.value));
                            }}
                          >
                            {[25, 50, 100, 200].map((size) => (
                              <option key={size} value={size}>
                                {size}
                              </option>
                            ))}
                          </select>
                        </label>
                        <button className="ghost-button small" type="button" onClick={() => setPage((current) => current - 1)} disabled={pagedRows.currentPage <= 1}>
                          {t("common.prev")}
                        </button>
                        <span className="tag">
                          {t("tdengine.pageStatus", { current: pagedRows.currentPage, total: pagedRows.totalPages })}
                        </span>
                        <button
                          className="ghost-button small"
                          type="button"
                          onClick={() => setPage((current) => current + 1)}
                          disabled={pagedRows.currentPage >= pagedRows.totalPages}
                        >
                          {t("common.next")}
                        </button>
                      </div>
                    </div>

                    {isResultToolsOpen ? (
                      <div className="tdengine-result-tools-panel">
                        <div className="tdengine-inline-actions tdengine-result-secondary-actions">
                          {sortColumn ? <span className="tag">{t("tdengine.sortStatus", { column: sortColumn, direction: sortDirection.toUpperCase() })}</span> : null}
                          {resultFilterQuery ? <span className="tag">{t("tdengine.filterStatus", { count: filteredRows.length })}</span> : null}
                          {activeColumnFilterCount ? <span className="tag">{t("tdengine.columnFilterCount", { count: activeColumnFilterCount })}</span> : null}
                          <button className={clsx("ghost-button small", freezeFirstColumn && "active")} type="button" onClick={() => setFreezeFirstColumn((current) => !current)}>
                            {freezeFirstColumn ? t("tdengine.unfreezeFirstColumn") : t("tdengine.freezeFirstColumn")}
                          </button>
                          <button className="ghost-button small" type="button" onClick={handleClearColumnFilters} disabled={!activeColumnFilterCount}>
                            {t("tdengine.clearFilters")}
                          </button>
                          <button className="ghost-button small" type="button" onClick={() => void handleCopyVisibleRows("json")}>
                            {t("tdengine.copyPageJson")}
                          </button>
                          <button className="ghost-button small" type="button" onClick={() => void handleCopyVisibleRows("csv")}>
                            {t("tdengine.copyPageCsv")}
                          </button>
                        </div>

                        <div className="tdengine-column-picker">
                          {result.columns.map((column) => {
                            const checked = visibleColumns.some((entry) => entry.name === column.name);
                            return (
                              <label key={column.name} className={clsx("tdengine-column-chip", checked && "active")}>
                                <input type="checkbox" checked={checked} onChange={() => toggleVisibleColumn(column.name)} />
                                <span>{column.name}</span>
                              </label>
                            );
                          })}
                        </div>

                        <div className="tdengine-column-filters">
                          {visibleColumns.map((column) => (
                            <label key={column.name} className="tdengine-column-filter-field">
                              <span>{column.name}</span>
                              <input
                                value={columnFilters[column.name] ?? ""}
                                onChange={(event) => handleColumnFilterChange(column.name, event.target.value)}
                                placeholder={t("tdengine.filterPlaceholder", { column: column.name })}
                              />
                            </label>
                          ))}
                        </div>
                      </div>
                    ) : null}

                    <div className="tdengine-result-table-wrap tdengine-result-table-wrap-fill">
                      <table className="tdengine-result-table">
                        <thead>
                          <tr>
                            {visibleColumns.map((column) => (
                              <th
                                key={column.name}
                                scope="col"
                                className={clsx(
                                  freezeFirstColumn && firstVisibleColumnName === column.name && "tdengine-frozen-column",
                                )}
                              >
                                <button
                                  className={clsx("tdengine-sort-button", sortColumn === column.name && "active")}
                                  type="button"
                                  onClick={() => handleSort(column.name)}
                                >
                                  <span>{column.name}</span>
                                  <small>{column.type}</small>
                                  <em>{sortColumn === column.name ? sortDirection.toUpperCase() : t("tdengine.sortLabel")}</em>
                                </button>
                              </th>
                            ))}
                            <th className="tdengine-actions-col" scope="col">
                              {t("tdengine.actions")}
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {pagedRows.rows.length ? (
                            pagedRows.rows.map((row, rowIndex) => (
                              <tr key={`${activeTab?.id ?? "tab"}-${pagedRows.startIndex + rowIndex}`}>
                                {visibleColumns.map((column) => {
                                  const displayValue = tdengineCellToString(row[column.name] ?? null);
                                  return (
                                    <td
                                      key={`${pagedRows.startIndex + rowIndex}-${column.name}`}
                                      title={`${displayValue}${displayValue ? " | Click to copy" : ""}`}
                                      className={clsx(
                                        "tdengine-clickable-cell",
                                        freezeFirstColumn && firstVisibleColumnName === column.name && "tdengine-frozen-column",
                                      )}
                                      onClick={() => void handleCopyCell(displayValue)}
                                    >
                                      {displayValue || " "}
                                    </td>
                                  );
                                })}
                                <td className="tdengine-row-actions">
                                  <button className="ghost-button small" type="button" onClick={() => void handleCopyRow(row)}>
                                    {t("tdengine.copyRow")}
                                  </button>
                                </td>
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td className="tdengine-empty-row" colSpan={visibleColumns.length + 1}>
                                {t("tdengine.noRowsAfterFilter")}
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <div className="empty-card tdengine-empty-card tdengine-empty-panel">
                    <p>
                      {result
                        ? isUseStatementResult
                          ? t("tdengine.switchedDatabase", { database: result.database || t("common.none") })
                          : t("tdengine.statementWithoutRows")
                        : t("tdengine.runReadOnlyHint")}
                    </p>
                  </div>
                )}
              </section>
            </div>
          </div>
        </div>

        <aside className={clsx("tdengine-side-shell", isSidePanelCollapsed && "collapsed")}>
          <div className="tdengine-side-rail">
            <button
              className="tdengine-side-toggle"
              type="button"
              onClick={() => setIsSidePanelCollapsed((current) => !current)}
              title={isSidePanelCollapsed ? t("tdengine.sidePanelExpand") : t("tdengine.sidePanelCollapse")}
              aria-label={isSidePanelCollapsed ? t("tdengine.sidePanelExpand") : t("tdengine.sidePanelCollapse")}
            >
              {isSidePanelCollapsed ? <PanelRightOpen size={18} /> : <PanelRightClose size={18} />}
            </button>

            <div className="tdengine-side-nav">
              {sidePanelItems.map((item) => (
                <button
                  key={item.key}
                  className={clsx("tdengine-side-nav-item", activeSidePanel === item.key && "active")}
                  type="button"
                  onClick={() => handleSelectSidePanel(item.key)}
                  title={item.label}
                  aria-label={item.label}
                >
                  {item.icon}
                </button>
              ))}
            </div>
          </div>

          {!isSidePanelCollapsed ? (
            <div className="tdengine-side-panel">
              <div className="tdengine-side-panel-header">
                <div>
                  <p className="eyebrow">{t("tdengine.sidePanel")}</p>
                  <h3>{activeSidePanelMeta.label}</h3>
                </div>
                <span className="tag">{activeSidePanelMeta.description}</span>
              </div>
              {renderSidePanelContent()}
            </div>
          ) : null}
        </aside>
      </div>
    </section>
  );
}
