import clsx from "clsx";
import { useEffect, useRef, useState, type ChangeEvent } from "react";
import "./App.css";
import { ConnectionModal } from "./components/ConnectionModal";
import { RedisNavRail } from "./components/RedisNavRail";
import { RedisWorkspace } from "./components/RedisWorkspace";
import { ResourceTree } from "./components/ResourceTree";
import { Sidebar } from "./components/Sidebar";
import { TdengineWorkspace } from "./components/TdengineWorkspace";
import { Workspace } from "./components/Workspace";
import { useI18n } from "./i18n";
import { desktopApi } from "./lib/desktopApi";
import { countByKind, filterConnections } from "./lib/filters";
import { createEmptyDraft, draftFromConnection, findNode, redisResourceIdToKey } from "./lib/mockData";
import { mergeRedisBrowsePages } from "./lib/redisView";
import {
  buildTdengineNodeId,
  clearTdengineStoredQueries,
  listTdengineDatabases,
  parseTdengineNodeId,
  readTdengineQueryHistory,
  readTdengineSavedQueries,
  removeTdengineSavedQuery,
  tdengineRowsToCsv,
  upsertTdengineSavedQuery,
  writeTdengineQueryHistory,
  writeTdengineSavedQueries,
} from "./lib/tdengine";
import type {
  ConnectionDraft,
  ConnectionHealth,
  ConnectionRecord,
  MiddlewareKind,
  RedisActionInput,
  RedisBrowseData,
  RedisCommandDisplayMode,
  RedisCreateKeyInput,
  RedisKeyDetail,
  RedisSearchMode,
  RedisKeyType,
  RedisSlowlogEntry,
  ResourceNode,
  TdengineObjectDetail,
  TdengineQueryResult,
  TdengineQueryTab,
  TdengineSavedQuery,
  TdengineSqlSuggestion,
  WorkspaceSnapshot,
  WorkspaceTab,
} from "./types";

function makeLocalId(prefix: string) {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return `${prefix}-${crypto.randomUUID()}`;
  }

  return `${prefix}-${Date.now()}-${Math.round(Math.random() * 10000)}`;
}

function createTdengineTab(title: string, database: string, sql = ""): TdengineQueryTab {
  return {
    id: makeLocalId("td-tab"),
    title,
    database,
    sql,
    isRunning: false,
    result: null,
  };
}

function replaceTreeChildren(nodes: ResourceNode[], parentId: string, children: ResourceNode[]): ResourceNode[] {
  return nodes.map((node) => {
    if (node.id === parentId) {
      return {
        ...node,
        children,
        expandable: children.length > 0,
      };
    }

    if (!node.children?.length) {
      return node;
    }

    return {
      ...node,
      children: replaceTreeChildren(node.children, parentId, children),
    };
  });
}

function buildTdengineErrorResult(database: string, message: string): TdengineQueryResult {
  return {
    columns: [],
    rows: [],
    rowCount: 0,
    durationMs: 0,
    truncated: false,
    database,
    error: message,
  };
}

function App() {
  const { language, setLanguage, t } = useI18n();
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(() => {
    if (typeof window === "undefined") {
      return false;
    }

    return window.localStorage.getItem("middleware-studio.sidebar-collapsed") === "true";
  });
  const [isResourceTreeCollapsed, setIsResourceTreeCollapsed] = useState(() => {
    if (typeof window === "undefined") {
      return false;
    }

    return window.localStorage.getItem("middleware-studio.resource-tree-collapsed") === "true";
  });
  const [connections, setConnections] = useState<ConnectionRecord[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [snapshots, setSnapshots] = useState<Record<string, WorkspaceSnapshot>>({});
  const [healthById, setHealthById] = useState<Record<string, ConnectionHealth>>({});
  const [query, setQuery] = useState("");
  const [workspaceTab, setWorkspaceTab] = useState<WorkspaceTab>("overview");
  const [selectedResourceId, setSelectedResourceId] = useState<string | null>(null);
  const [redisBrowseById, setRedisBrowseById] = useState<Record<string, RedisBrowseData>>({});
  const [redisDetailById, setRedisDetailById] = useState<Record<string, RedisKeyDetail | null>>({});
  const [redisSelectedKeyIdsById, setRedisSelectedKeyIdsById] = useState<Record<string, string[]>>({});
  const [redisSlowlogById, setRedisSlowlogById] = useState<Record<string, RedisSlowlogEntry[]>>({});
  const [redisDbById, setRedisDbById] = useState<Record<string, number>>({});
  const [tdengineCatalogById, setTdengineCatalogById] = useState<Record<string, ResourceNode[]>>({});
  const [tdengineObjectDetailById, setTdengineObjectDetailById] = useState<Record<string, TdengineObjectDetail | null>>({});
  const [tdengineTabsById, setTdengineTabsById] = useState<Record<string, TdengineQueryTab[]>>({});
  const [tdengineActiveTabIdById, setTdengineActiveTabIdById] = useState<Record<string, string | null>>({});
  const [tdengineHistoryById, setTdengineHistoryById] = useState<Record<string, string[]>>({});
  const [tdengineFavoritesById, setTdengineFavoritesById] = useState<Record<string, TdengineSavedQuery[]>>({});
  const [modalState, setModalState] = useState<{
    open: boolean;
    mode: "create" | "edit";
    draft: ConnectionDraft;
  }>({
    open: false,
    mode: "create",
    draft: createEmptyDraft("redis"),
  });
  const [notice, setNotice] = useState<{ tone: "success" | "danger" | "info"; message: string } | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const importInputRef = useRef<HTMLInputElement | null>(null);

  const selectedConnection = connections.find((connection) => connection.id === selectedId) ?? null;
  const selectedSnapshot = selectedConnection ? snapshots[selectedConnection.id] ?? null : null;
  const selectedRedisBrowse = selectedConnection ? redisBrowseById[selectedConnection.id] ?? null : null;
  const selectedRedisDetail = selectedConnection ? redisDetailById[selectedConnection.id] ?? null : null;
  const selectedRedisKeyIds = selectedConnection ? redisSelectedKeyIdsById[selectedConnection.id] ?? [] : [];
  const selectedRedisSlowlog = selectedConnection ? redisSlowlogById[selectedConnection.id] ?? [] : [];
  const selectedTdengineCatalog = selectedConnection ? tdengineCatalogById[selectedConnection.id] ?? null : null;
  const selectedTdengineDetail = selectedConnection ? tdengineObjectDetailById[selectedConnection.id] ?? null : null;
  const selectedTdengineTabs = selectedConnection ? tdengineTabsById[selectedConnection.id] ?? [] : [];
  const selectedTdengineActiveTabId =
    selectedConnection ? tdengineActiveTabIdById[selectedConnection.id] ?? selectedTdengineTabs[0]?.id ?? null : null;
  const selectedTdengineHistory = selectedConnection ? tdengineHistoryById[selectedConnection.id] ?? [] : [];
  const selectedTdengineFavorites = selectedConnection ? tdengineFavoritesById[selectedConnection.id] ?? [] : [];
  const selectedTdengineDatabases =
    selectedConnection?.kind === "tdengine" ? listTdengineDatabases(selectedTdengineCatalog, selectedConnection.databaseName) : [];
  const selectedHealth = selectedConnection ? healthById[selectedConnection.id] ?? null : null;
  const filteredConnections = filterConnections(connections, query);
  const redisConnections = connections.filter((connection) => connection.kind === "redis");
  const counts = countByKind(connections);
  const selectedRedisDb =
    selectedConnection?.kind === "redis"
      ? redisDbById[selectedConnection.id] ?? Number(selectedConnection.databaseName || 0)
      : 0;
  const activeResources =
    selectedConnection?.kind === "redis"
      ? selectedRedisBrowse?.resources ?? null
      : selectedConnection?.kind === "tdengine"
        ? selectedTdengineCatalog
        : selectedSnapshot?.resources ?? null;
  const activeResourceTitle =
    selectedConnection?.kind === "redis"
      ? (language === "zh-CN" ? "Redis 浏览器" : "Redis browser")
      : selectedConnection?.kind === "tdengine"
        ? (language === "zh-CN" ? "TDengine 目录" : "TDengine catalog")
        : selectedSnapshot?.title ?? null;

  useEffect(() => {
    let active = true;

    async function boot() {
      try {
        const nextConnections = await desktopApi.listConnections();
        if (!active) {
          return;
        }

        setConnections(nextConnections);
        setSelectedId(nextConnections[0]?.id ?? null);
      } catch (error) {
        if (active) {
          setNotice({
            tone: "danger",
            message: error instanceof Error ? error.message : t("notices.loadSavedConnectionsFailed"),
          });
        }
      } finally {
        if (active) {
          setIsLoading(false);
        }
      }
    }

    void boot();

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!notice) {
      return;
    }

    const timer = window.setTimeout(() => setNotice(null), 2800);
    return () => window.clearTimeout(timer);
  }, [notice]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem("middleware-studio.sidebar-collapsed", String(isSidebarCollapsed));
  }, [isSidebarCollapsed]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem("middleware-studio.resource-tree-collapsed", String(isResourceTreeCollapsed));
  }, [isResourceTreeCollapsed]);

  useEffect(() => {
    if (!selectedConnection) {
      return;
    }

    if (selectedConnection.kind === "redis") {
      if (!selectedRedisBrowse) {
        void loadRedisBrowse(selectedConnection.id, {
          database: selectedRedisDb,
          pattern: "",
          searchMode: "pattern",
          typeFilter: "all",
          limit: 80,
          cursor: null,
          viewMode: "tree",
        });
      }
      return;
    }

    if (selectedConnection.kind === "tdengine") {
      if (!selectedTdengineCatalog) {
        void loadTdengineCatalog(selectedConnection.id);
      }

      if (!selectedTdengineTabs.length) {
        ensureTdengineDefaultTab(selectedConnection);
      }
      return;
    }

    if (!snapshots[selectedConnection.id]) {
      void loadSnapshot(selectedConnection.id);
    }
  }, [selectedConnection, selectedRedisBrowse, selectedRedisDb, selectedTdengineCatalog, selectedTdengineTabs.length, snapshots]);

  useEffect(() => {
    if (!selectedConnection || desktopApi.isDesktopRuntime()) {
      return;
    }

    if (selectedConnection.kind === "mysql" || selectedConnection.kind === "postgres" || selectedConnection.kind === "kafka") {
      void loadSnapshot(selectedConnection.id);
    }
  }, [language, selectedConnection]);

  useEffect(() => {
    if (selectedConnection?.kind !== "tdengine") {
      return;
    }

    if (!Object.prototype.hasOwnProperty.call(tdengineHistoryById, selectedConnection.id)) {
      setTdengineHistoryById((current) => ({
        ...current,
        [selectedConnection.id]: readTdengineQueryHistory(selectedConnection.id),
      }));
    }

    if (!Object.prototype.hasOwnProperty.call(tdengineFavoritesById, selectedConnection.id)) {
      setTdengineFavoritesById((current) => ({
        ...current,
        [selectedConnection.id]: readTdengineSavedQueries(selectedConnection.id),
      }));
    }
  }, [selectedConnection, tdengineFavoritesById, tdengineHistoryById]);

  useEffect(() => {
    if (!selectedConnection) {
      setSelectedResourceId(null);
      return;
    }

    if (selectedConnection.kind === "redis") {
      const currentKey = redisResourceIdToKey(selectedResourceId);
      if (currentKey && selectedRedisBrowse?.keySummaries.some((summary) => summary.key === currentKey)) {
        return;
      }

      setSelectedResourceId(selectedRedisBrowse?.keySummaries[0] ? `key:${selectedRedisBrowse.keySummaries[0].key}` : null);
      return;
    }

    if (selectedConnection.kind === "tdengine") {
      const resources = selectedTdengineCatalog;
      if (!resources?.length) {
        setSelectedResourceId(null);
        return;
      }

      const existing = findNode(resources, selectedResourceId);
      if (!existing) {
        setSelectedResourceId(resources[0]?.id ?? null);
      }
      return;
    }

    const resources = selectedSnapshot?.resources;
    if (!resources) {
      setSelectedResourceId(null);
      return;
    }

    const existing = findNode(resources, selectedResourceId);
    if (!existing) {
      setSelectedResourceId(resources[0]?.id ?? null);
    }
  }, [selectedConnection, selectedRedisBrowse, selectedSnapshot, selectedTdengineCatalog, selectedResourceId]);

  useEffect(() => {
    if (selectedConnection?.kind !== "redis") {
      return;
    }

    const key = redisResourceIdToKey(selectedResourceId);
    if (!key) {
      setRedisDetailById((current) => ({
        ...current,
        [selectedConnection.id]: null,
      }));
      return;
    }

    const currentDetail = redisDetailById[selectedConnection.id];
    if (currentDetail?.key === key) {
      return;
    }

    void loadRedisKeyDetail(selectedConnection.id, selectedRedisDb, key);
  }, [selectedConnection, selectedRedisDb, selectedResourceId, redisDetailById]);

  useEffect(() => {
    if (selectedConnection?.kind !== "tdengine") {
      return;
    }

    const parsedNode = parseTdengineNodeId(selectedResourceId);
    if (!parsedNode || parsedNode.objectKind === "database" || !parsedNode.objectName) {
      setTdengineObjectDetailById((current) => ({
        ...current,
        [selectedConnection.id]: null,
      }));
      return;
    }

    const currentDetail = tdengineObjectDetailById[selectedConnection.id];
    if (
      currentDetail?.database === parsedNode.database &&
      currentDetail.objectName === parsedNode.objectName &&
      currentDetail.objectKind === parsedNode.objectKind
    ) {
      return;
    }

    void loadTdengineObjectDetail(selectedConnection.id, parsedNode.database, parsedNode.objectName, parsedNode.objectKind);
  }, [selectedConnection, selectedResourceId, tdengineObjectDetailById]);

  useEffect(() => {
    if (selectedConnection?.kind !== "redis" || workspaceTab !== "diagnostics" || redisSlowlogById[selectedConnection.id]) {
      return;
    }

    void loadRedisSlowlog(selectedConnection.id, selectedRedisDb);
  }, [selectedConnection, selectedRedisDb, workspaceTab, redisSlowlogById]);

  function openCreateModal(kind: MiddlewareKind = selectedConnection?.kind ?? "redis") {
    setModalState({
      open: true,
      mode: "create",
      draft: createEmptyDraft(kind),
    });
  }

  function ensureTdengineDefaultTab(connection: ConnectionRecord) {
    const database = connection.databaseName || "";
    const nextTab = createTdengineTab("Query 1", database);

    setTdengineTabsById((current) => {
      if ((current[connection.id] ?? []).length) {
        return current;
      }

      return {
        ...current,
        [connection.id]: [nextTab],
      };
    });

    setTdengineActiveTabIdById((current) => ({
      ...current,
      [connection.id]: current[connection.id] ?? nextTab.id,
    }));
  }

  async function refreshConnections(preferredId?: string | null) {
    const nextConnections = await desktopApi.listConnections();
    setConnections(nextConnections);
    setSelectedId((current) => {
      const candidate = preferredId === undefined ? current : preferredId;
      if (candidate && nextConnections.some((connection) => connection.id === candidate)) {
        return candidate;
      }
      return nextConnections[0]?.id ?? null;
    });
  }

  async function loadSnapshot(connectionId: string) {
    try {
      const snapshot = await desktopApi.getWorkspaceSnapshot(connectionId, language);
      setSnapshots((current) => ({
        ...current,
        [connectionId]: snapshot,
      }));
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.loadWorkspaceSnapshotFailed"),
      });
    }
  }

  async function loadTdengineCatalog(connectionId: string, options?: { database?: string | null; supertable?: string | null }) {
    try {
      const nodes = await desktopApi.loadTdengineCatalog(connectionId, options);
      setTdengineCatalogById((current) => {
        if (!options?.database) {
          return {
            ...current,
            [connectionId]: nodes,
          };
        }

        const parentId = options.supertable
          ? buildTdengineNodeId({ database: options.database, objectKind: "supertable", objectName: options.supertable })
          : buildTdengineNodeId({ database: options.database, objectKind: "database" });

        return {
          ...current,
          [connectionId]: replaceTreeChildren(current[connectionId] ?? [], parentId, nodes),
        };
      });
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.loadTdengineCatalogFailed"),
      });
    }
  }

  async function loadTdengineObjectDetail(
    connectionId: string,
    database: string,
    objectName: string,
    objectKind: TdengineObjectDetail["objectKind"],
  ) {
    try {
      const detail = await desktopApi.getTdengineObjectDetail(connectionId, database, objectName, objectKind);
      setTdengineObjectDetailById((current) => ({
        ...current,
        [connectionId]: detail,
      }));
      return detail;
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.loadTdengineObjectDetailFailed"),
      });
      return null;
    }
  }

  async function loadRedisBrowse(
    connectionId: string,
    options: {
      database: number;
      pattern: string;
      searchMode: RedisSearchMode;
      typeFilter: RedisKeyType | "all";
      limit: number;
      cursor?: string | null;
      viewMode: "tree" | "list";
    },
    append = false,
  ) {
    try {
      const previous = redisBrowseById[connectionId] ?? null;
      const page = await desktopApi.browseRedisKeys(connectionId, options);
      const merged = append ? mergeRedisBrowsePages(previous, page) : mergeRedisBrowsePages(null, page);
      const visibleKeys = new Set(merged.keySummaries.map((summary) => summary.key));

      setRedisDbById((current) => ({
        ...current,
        [connectionId]: options.database,
      }));
      setRedisBrowseById((current) => ({
        ...current,
        [connectionId]: merged,
      }));
      setRedisSelectedKeyIdsById((current) => ({
        ...current,
        [connectionId]: (current[connectionId] ?? []).filter((key) => visibleKeys.has(key)),
      }));

      const currentKey = redisResourceIdToKey(selectedResourceId);
      const nextKey =
        currentKey && merged.keySummaries.some((summary) => summary.key === currentKey)
          ? currentKey
          : merged.keySummaries[0]?.key ?? null;
      setSelectedResourceId(nextKey ? `key:${nextKey}` : null);
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.loadRedisBrowserFailed"),
      });
    }
  }

  async function loadRedisKeyDetail(connectionId: string, database: number, key: string) {
    try {
      const detail = await desktopApi.getRedisKeyDetail(connectionId, database, key);
      setRedisDetailById((current) => ({
        ...current,
        [connectionId]: detail,
      }));
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.loadRedisKeyDetailFailed"),
      });
    }
  }

  async function loadRedisSlowlog(connectionId: string, database: number) {
    try {
      const slowlog = await desktopApi.getRedisSlowlog(connectionId, database, 20);
      setRedisSlowlogById((current) => ({
        ...current,
        [connectionId]: slowlog,
      }));
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.loadRedisSlowlogFailed"),
      });
    }
  }

  function updateTdengineTab(connectionId: string, tabId: string, updater: (tab: TdengineQueryTab) => TdengineQueryTab) {
    setTdengineTabsById((current) => ({
      ...current,
      [connectionId]: (current[connectionId] ?? []).map((tab) => (tab.id === tabId ? updater(tab) : tab)),
    }));
  }

  async function runTdengineQuery(connectionId: string, tabId: string, sqlOverride?: string, databaseOverride?: string) {
    const tabs = tdengineTabsById[connectionId] ?? [];
    const targetTab = tabs.find((tab) => tab.id === tabId);
    const sql = sqlOverride ?? targetTab?.sql ?? "";
    const database = databaseOverride ?? targetTab?.database ?? selectedConnection?.databaseName ?? "";

    updateTdengineTab(connectionId, tabId, (tab) => ({
      ...tab,
      sql,
      database,
      isRunning: true,
    }));

    try {
      const result = await desktopApi.executeTdengineQuery(connectionId, database, sql, 1000);

      setTdengineTabsById((current) => ({
        ...current,
        [connectionId]: (current[connectionId] ?? []).map((tab) =>
          tab.id === tabId
            ? {
                ...tab,
                sql,
                database: result.database || database,
                isRunning: false,
                result,
              }
            : tab,
        ),
      }));

      const statement = sql.trim();
      if (statement) {
        setTdengineHistoryById((current) => {
          const currentHistory = current[connectionId] ?? [];
          const nextHistory = [statement, ...currentHistory.filter((item) => item !== statement)].slice(0, 12);
          writeTdengineQueryHistory(connectionId, nextHistory);

          return {
            ...current,
            [connectionId]: nextHistory,
          };
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "TDengine query failed.";
      updateTdengineTab(connectionId, tabId, (tab) => ({
        ...tab,
        sql,
        database,
        isRunning: false,
        result: buildTdengineErrorResult(database, message),
      }));
      setNotice({
        tone: "danger",
        message,
      });
    }
  }

  async function openTdengineObject(detail: TdengineObjectDetail) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    const existing = selectedTdengineTabs.find((tab) => tab.objectName === detail.objectName && tab.database === detail.database);
    const nextTabId = existing?.id ?? makeLocalId("td-tab");
    const nextTitle = detail.objectKind === "supertable" ? `${detail.objectName} template` : detail.objectName;

    setTdengineTabsById((current) => {
      const currentTabs = current[selectedConnection.id] ?? [];
      if (existing) {
        return {
          ...current,
          [selectedConnection.id]: currentTabs.map((tab) =>
            tab.id === existing.id
              ? {
                  ...tab,
                  title: nextTitle,
                  sql: detail.previewSql,
                  database: detail.database,
                  objectName: detail.objectName,
                  objectKind: detail.objectKind,
                }
              : tab,
          ),
        };
      }

      return {
        ...current,
        [selectedConnection.id]: [
          ...currentTabs,
          {
            id: nextTabId,
            title: nextTitle,
            database: detail.database,
            sql: detail.previewSql,
            isRunning: false,
            result: null,
            objectName: detail.objectName,
            objectKind: detail.objectKind,
          },
        ],
      };
    });

    setTdengineActiveTabIdById((current) => ({
      ...current,
      [selectedConnection.id]: nextTabId,
    }));

    if (detail.objectKind === "table" || detail.objectKind === "child-table") {
      await runTdengineQuery(selectedConnection.id, nextTabId, detail.previewSql, detail.database);
    }
  }

  async function handleSaveConnection(draft: ConnectionDraft) {
    const saved = await desktopApi.saveConnection(draft);
    setModalState((current) => ({ ...current, open: false }));
    await refreshConnections(saved.id);

    if (saved.kind === "redis") {
      await loadRedisBrowse(saved.id, {
        database: Number(saved.databaseName || 0),
        pattern: "",
        searchMode: "pattern",
        typeFilter: "all",
        limit: 80,
        cursor: null,
        viewMode: "tree",
      });
    } else if (saved.kind === "tdengine") {
      await loadTdengineCatalog(saved.id);
      ensureTdengineDefaultTab(saved);
    } else {
      await loadSnapshot(saved.id);
    }

    setNotice({
      tone: "success",
      message: t("notices.savedNamed", { name: saved.name }),
    });
  }

  async function handleSelectConnection(connectionId: string) {
    setSelectedId(connectionId);
    setWorkspaceTab("overview");
    setSelectedResourceId(null);

    try {
      await desktopApi.touchConnection(connectionId);
      await refreshConnections(connectionId);
    } catch {
      // Selection should still succeed even if the touch metadata call fails.
    }
  }

  async function handleRedisSelectDatabase(database: number) {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    setSelectedResourceId(null);
    setRedisDetailById((current) => ({
      ...current,
      [selectedConnection.id]: null,
    }));
    setRedisSlowlogById((current) => ({
      ...current,
      [selectedConnection.id]: [],
    }));
    setRedisSelectedKeyIdsById((current) => ({
      ...current,
      [selectedConnection.id]: [],
    }));

    await loadRedisBrowse(selectedConnection.id, {
      database,
      pattern: selectedRedisBrowse?.pattern ?? "",
      searchMode: selectedRedisBrowse?.searchMode ?? "pattern",
      typeFilter: selectedRedisBrowse?.typeFilter ?? "all",
      viewMode: selectedRedisBrowse?.viewMode ?? "tree",
      limit: 80,
      cursor: null,
    });
  }

  async function handleToggleFavorite(connection: ConnectionRecord) {
    try {
      await desktopApi.toggleFavorite(connection.id, !connection.favorite);
      await refreshConnections(connection.id);
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : "Failed to update favorite state.",
      });
    }
  }

  async function handleDeleteConnection() {
    if (!selectedConnection) {
      return;
    }

    const confirmed = window.confirm(
      language === "zh-CN"
        ? `确定删除 ${selectedConnection.name} 吗？这会从桌面应用里移除这个已保存连接。`
        : `Delete ${selectedConnection.name}? This removes the saved profile from the desktop app.`,
    );
    if (!confirmed) {
      return;
    }

    try {
      await desktopApi.deleteConnection(selectedConnection.id);
      setSnapshots((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setHealthById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setRedisBrowseById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setRedisDetailById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setRedisSlowlogById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setRedisSelectedKeyIdsById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setTdengineCatalogById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setTdengineObjectDetailById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setTdengineTabsById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setTdengineActiveTabIdById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setTdengineHistoryById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      setTdengineFavoritesById((current) => {
        const next = { ...current };
        delete next[selectedConnection.id];
        return next;
      });
      clearTdengineStoredQueries(selectedConnection.id);
      await refreshConnections(null);
      setNotice({
        tone: "success",
        message: t("notices.deletedNamed", { name: selectedConnection.name }),
      });
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : "Delete failed.",
      });
    }
  }

  async function handleRunHealthCheck() {
    if (!selectedConnection) {
      return;
    }

    try {
      const result = await desktopApi.healthCheck(selectedConnection.id, language);
      setHealthById((current) => ({
        ...current,
        [selectedConnection.id]: result,
      }));
      await refreshConnections(selectedConnection.id);
      setNotice({
        tone: "info",
        message: result.summary,
      });
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.healthCheckFailed"),
      });
    }
  }

  async function handleResourceSelect(resourceId: string) {
    setSelectedResourceId(resourceId);

    if (selectedConnection?.kind === "redis") {
      const key = redisResourceIdToKey(resourceId);
      if (key) {
        await loadRedisKeyDetail(selectedConnection.id, selectedRedisDb, key);
      }
      return;
    }

    if (selectedConnection?.kind === "tdengine") {
      const parsedNode = parseTdengineNodeId(resourceId);
      if (!parsedNode) {
        return;
      }

      if (parsedNode.objectKind === "database") {
        if (selectedTdengineActiveTabId) {
          updateTdengineTab(selectedConnection.id, selectedTdengineActiveTabId, (tab) => ({
            ...tab,
            database: parsedNode.database,
          }));
        }
        setTdengineObjectDetailById((current) => ({
          ...current,
          [selectedConnection.id]: null,
        }));
        return;
      }

      if (!parsedNode.objectName) {
        return;
      }

      const detail = await loadTdengineObjectDetail(selectedConnection.id, parsedNode.database, parsedNode.objectName, parsedNode.objectKind);
      if (detail) {
        await openTdengineObject(detail);
      }
    }
  }

  async function handleTdengineExpandNode(node: ResourceNode) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    const parsedNode = parseTdengineNodeId(node.id);
    if (!parsedNode) {
      return;
    }

    if (parsedNode.objectKind === "database") {
      await loadTdengineCatalog(selectedConnection.id, { database: parsedNode.database });
    }

    if (parsedNode.objectKind === "supertable" && parsedNode.objectName) {
      await loadTdengineCatalog(selectedConnection.id, {
        database: parsedNode.database,
        supertable: parsedNode.objectName,
      });
    }
  }

  async function handleRedisBrowseChange(options: {
    database: number;
    pattern: string;
    searchMode: RedisSearchMode;
    typeFilter: RedisKeyType | "all";
    viewMode: "tree" | "list";
  }) {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    await loadRedisBrowse(selectedConnection.id, {
      database: options.database,
      pattern: options.pattern,
      searchMode: options.searchMode,
      typeFilter: options.typeFilter,
      viewMode: options.viewMode,
      limit: 80,
      cursor: null,
    });
  }

  async function handleRedisRefresh() {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    await loadRedisBrowse(selectedConnection.id, {
      database: selectedRedisDb,
      pattern: selectedRedisBrowse?.pattern ?? "",
      searchMode: selectedRedisBrowse?.searchMode ?? "pattern",
      typeFilter: selectedRedisBrowse?.typeFilter ?? "all",
      viewMode: selectedRedisBrowse?.viewMode ?? "tree",
      limit: 80,
      cursor: null,
    });

    const selectedKey = redisResourceIdToKey(selectedResourceId);
    if (selectedKey) {
      await loadRedisKeyDetail(selectedConnection.id, selectedRedisDb, selectedKey);
    }
  }

  async function handleRedisLoadMore() {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    const current = redisBrowseById[selectedConnection.id];
    if (!current?.nextCursor) {
      return;
    }

    await loadRedisBrowse(
      selectedConnection.id,
      {
        database: selectedRedisDb,
        pattern: current.pattern,
        searchMode: current.searchMode,
        typeFilter: current.typeFilter,
        viewMode: current.viewMode,
        limit: current.limit,
        cursor: current.nextCursor,
      },
      true,
    );
  }

  async function handleRedisExecuteAction(input: RedisActionInput, successMessage?: string) {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    await desktopApi.executeRedisAction(selectedConnection.id, selectedRedisDb, input);
    await handleRedisRefresh();
    const key = input.key ?? redisResourceIdToKey(selectedResourceId);
    if (key) {
      await loadRedisKeyDetail(selectedConnection.id, selectedRedisDb, key);
    }
    setNotice({
      tone: "success",
      message: successMessage ?? t("notices.actionCompleted"),
    });
  }

  async function handleRedisCreateKey(input: RedisCreateKeyInput) {
    await handleRedisExecuteAction(desktopApi.toRedisCreateAction(input), `${input.key} created.`);
    setSelectedResourceId(`key:${input.key}`);
    await loadRedisKeyDetail(selectedConnection!.id, selectedRedisDb, input.key);
  }

  async function handleRedisSaveValue(value: string) {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    const key = redisResourceIdToKey(selectedResourceId);
    if (!key) {
      return;
    }

    if (selectedRedisDetail?.keyType === "json") {
      await desktopApi.setRedisJson(selectedConnection.id, selectedRedisDb, key, value);
      await handleRedisRefresh();
      await loadRedisKeyDetail(selectedConnection.id, selectedRedisDb, key);
      setNotice({
        tone: "success",
        message: `${key} updated.`,
      });
      return;
    }

    await handleRedisExecuteAction({ action: "save-value", key, value }, `${key} updated.`);
  }

  async function handleRedisUpdateTtl(ttlSeconds: number | null) {
    const key = redisResourceIdToKey(selectedResourceId);
    if (!key) {
      return;
    }

    await handleRedisExecuteAction(
      { action: "update-ttl", key, ttlSeconds },
      ttlSeconds === null ? `${key} TTL removed.` : `${key} TTL updated.`,
    );
  }

  async function handleRedisDeleteKey() {
    const key = redisResourceIdToKey(selectedResourceId);
    if (!key) {
      return;
    }

    await handleRedisExecuteAction({ action: "delete-key", key }, `${key} deleted.`);
    if (selectedConnection?.kind === "redis") {
      setRedisSelectedKeyIdsById((current) => ({
        ...current,
        [selectedConnection.id]: (current[selectedConnection.id] ?? []).filter((item) => item !== key),
      }));
    }
    setSelectedResourceId(null);
  }

  async function handleRedisPreviewBulkDelete(pattern: string, typeFilter: RedisKeyType | "all") {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      throw new Error(t("notices.redisConnectionNotSelected"));
    }

    if (selectedRedisKeyIds.length > 0) {
      return desktopApi.bulkDeleteRedisSelectedKeys(selectedConnection.id, selectedRedisDb, selectedRedisKeyIds, true);
    }

    return desktopApi.bulkDeleteRedisKeys(selectedConnection.id, selectedRedisDb, pattern, typeFilter, true);
  }

  async function handleRedisRunBulkDelete(pattern: string, typeFilter: RedisKeyType | "all") {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      throw new Error(t("notices.redisConnectionNotSelected"));
    }

    const result =
      selectedRedisKeyIds.length > 0
        ? await desktopApi.bulkDeleteRedisSelectedKeys(selectedConnection.id, selectedRedisDb, selectedRedisKeyIds, false)
        : await desktopApi.bulkDeleteRedisKeys(selectedConnection.id, selectedRedisDb, pattern, typeFilter, false);
    await handleRedisRefresh();
    setRedisSelectedKeyIdsById((current) => ({
      ...current,
      [selectedConnection.id]: [],
    }));
    setNotice({
      tone: "success",
      message: result.deleted ? t("notices.keysDeleted", { count: result.deleted }) : t("notices.noKeysDeleted"),
    });
    return result;
  }

  async function handleRedisRunCli(statement: string, responseMode: RedisCommandDisplayMode) {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      throw new Error(t("notices.redisConnectionNotSelected"));
    }

    return desktopApi.runRedisCliCommand(selectedConnection.id, selectedRedisDb, statement, responseMode);
  }

  async function handleRedisRunWorkbench(input: string, responseMode: RedisCommandDisplayMode) {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      throw new Error(t("notices.redisConnectionNotSelected"));
    }

    return desktopApi.runRedisWorkbenchQuery(selectedConnection.id, selectedRedisDb, input, responseMode);
  }

  async function handleRedisClearSelection() {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    setSelectedResourceId(null);
    setRedisDetailById((current) => ({
      ...current,
      [selectedConnection.id]: null,
    }));
  }

  function handleRedisToggleKeySelection(key: string, selected: boolean) {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    setRedisSelectedKeyIdsById((current) => {
      const next = new Set(current[selectedConnection.id] ?? []);
      if (selected) {
        next.add(key);
      } else {
        next.delete(key);
      }
      return {
        ...current,
        [selectedConnection.id]: [...next],
      };
    });
  }

  function handleRedisToggleAllVisibleKeys(selected: boolean) {
    if (!selectedConnection || selectedConnection.kind !== "redis" || !selectedRedisBrowse) {
      return;
    }

    const visibleKeys = selectedRedisBrowse.keySummaries.map((summary) => summary.key);
    setRedisSelectedKeyIdsById((current) => ({
      ...current,
      [selectedConnection.id]: selected ? visibleKeys : [],
    }));
  }

  function handleRedisClearKeySelection() {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    setRedisSelectedKeyIdsById((current) => ({
      ...current,
      [selectedConnection.id]: [],
    }));
  }

  async function handleRedisRefreshSlowlog() {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    await loadRedisSlowlog(selectedConnection.id, selectedRedisDb);
  }

  async function handleRedisRefreshStream() {
    if (!selectedConnection || selectedConnection.kind !== "redis") {
      return;
    }

    const key = redisResourceIdToKey(selectedResourceId);
    if (!key || selectedRedisDetail?.keyType !== "stream") {
      return;
    }

    const streamState = await desktopApi.getRedisStreamState(selectedConnection.id, selectedRedisDb, key, 20);
    setRedisDetailById((current) => {
      const detail = current[selectedConnection.id];
      if (!detail) {
        return current;
      }

      return {
        ...current,
        [selectedConnection.id]: {
          ...detail,
          streamState,
          rows: streamState.entries,
        },
      };
    });
  }

  function handleTdengineCreateTab() {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    const title = `Query ${selectedTdengineTabs.length + 1}`;
    const database = selectedTdengineTabs.find((tab) => tab.id === selectedTdengineActiveTabId)?.database ?? selectedConnection.databaseName ?? "";
    const nextTab = createTdengineTab(title, database);

    setTdengineTabsById((current) => ({
      ...current,
      [selectedConnection.id]: [...(current[selectedConnection.id] ?? []), nextTab],
    }));
    setTdengineActiveTabIdById((current) => ({
      ...current,
      [selectedConnection.id]: nextTab.id,
    }));
  }

  function handleTdengineCloseTab(tabId: string) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    const remainingTabs = selectedTdengineTabs.filter((tab) => tab.id !== tabId);

    setTdengineTabsById((current) => {
      const remaining = (current[selectedConnection.id] ?? []).filter((tab) => tab.id !== tabId);
      if (remaining.length) {
        return {
          ...current,
          [selectedConnection.id]: remaining,
        };
      }

      const fallback = createTdengineTab("Query 1", selectedConnection.databaseName ?? "");
      setTdengineActiveTabIdById((activeState) => ({
        ...activeState,
        [selectedConnection.id]: fallback.id,
      }));

      return {
        ...current,
        [selectedConnection.id]: [fallback],
      };
    });

    setTdengineActiveTabIdById((current) => {
      if (current[selectedConnection.id] !== tabId) {
        return current;
      }

      if (!remainingTabs.length) {
        return current;
      }

      return {
        ...current,
        [selectedConnection.id]: remainingTabs[0].id,
      };
    });
  }

  function handleTdengineUpdateTabSql(tabId: string, sql: string) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    updateTdengineTab(selectedConnection.id, tabId, (tab) => ({
      ...tab,
      sql,
    }));
  }

  function handleTdengineApplyPreviewSql() {
    if (!selectedConnection || selectedConnection.kind !== "tdengine" || !selectedTdengineDetail || !selectedTdengineActiveTabId) {
      return;
    }

    updateTdengineTab(selectedConnection.id, selectedTdengineActiveTabId, (tab) => ({
      ...tab,
      sql: selectedTdengineDetail.previewSql,
      database: selectedTdengineDetail.database,
    }));
  }

  function handleTdengineUseHistoryItem(sql: string) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine" || !selectedTdengineActiveTabId) {
      return;
    }

    updateTdengineTab(selectedConnection.id, selectedTdengineActiveTabId, (tab) => ({
      ...tab,
      sql,
    }));
  }

  function handleTdengineSaveFavorite() {
    if (!selectedConnection || selectedConnection.kind !== "tdengine" || !selectedTdengineActiveTabId) {
      return;
    }

    const activeTab = selectedTdengineTabs.find((tab) => tab.id === selectedTdengineActiveTabId);
    const sql = activeTab?.sql.trim() ?? "";
    if (!sql) {
      setNotice({
        tone: "info",
        message: t("notices.sqlEmpty"),
      });
      return;
    }

    const nextEntry: TdengineSavedQuery = {
      id: makeLocalId("td-favorite"),
      title: activeTab?.title || "Saved SQL",
      database: activeTab?.database ?? "",
      sql,
      updatedAt: new Date().toISOString(),
    };

    setTdengineFavoritesById((current) => {
      const nextFavorites = upsertTdengineSavedQuery(current[selectedConnection.id] ?? [], nextEntry, 20);
      writeTdengineSavedQueries(selectedConnection.id, nextFavorites);
      return {
        ...current,
        [selectedConnection.id]: nextFavorites,
      };
    });

    setNotice({
      tone: "success",
      message: t("notices.savedToFavorites"),
    });
  }

  function handleTdengineUseFavorite(favorite: TdengineSavedQuery) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine" || !selectedTdengineActiveTabId) {
      return;
    }

    updateTdengineTab(selectedConnection.id, selectedTdengineActiveTabId, (tab) => ({
      ...tab,
      title: favorite.title,
      database: favorite.database,
      sql: favorite.sql,
    }));
  }

  function handleTdengineDeleteFavorite(favoriteId: string) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    setTdengineFavoritesById((current) => {
      const nextFavorites = removeTdengineSavedQuery(current[selectedConnection.id] ?? [], favoriteId);
      writeTdengineSavedQueries(selectedConnection.id, nextFavorites);
      return {
        ...current,
        [selectedConnection.id]: nextFavorites,
      };
    });

    setNotice({
      tone: "info",
      message: t("notices.favoriteRemoved"),
    });
  }

  function handleTdengineApplySuggestion(suggestion: TdengineSqlSuggestion) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine" || !selectedTdengineActiveTabId) {
      return;
    }

    updateTdengineTab(selectedConnection.id, selectedTdengineActiveTabId, (tab) => ({
      ...tab,
      database: suggestion.database || tab.database,
      sql: suggestion.sql,
      title:
        tab.title.startsWith("Query ") || !tab.objectName
          ? suggestion.label
          : tab.title,
    }));

    if (suggestion.database && suggestion.database !== selectedTdengineDetail?.database) {
      setSelectedResourceId(buildTdengineNodeId({ database: suggestion.database, objectKind: "database" }));
    }
  }

  function handleTdengineSelectDatabase(database: string) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine" || !selectedTdengineActiveTabId) {
      return;
    }

    updateTdengineTab(selectedConnection.id, selectedTdengineActiveTabId, (tab) => ({
      ...tab,
      database,
    }));
    setSelectedResourceId(database ? buildTdengineNodeId({ database, objectKind: "database" }) : null);
    setTdengineObjectDetailById((current) => ({
      ...current,
      [selectedConnection.id]: null,
    }));
  }

  async function handleTdengineRunQuery(tabId: string) {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    const activeTab = selectedTdengineTabs.find((tab) => tab.id === tabId);
    if (!activeTab) {
      return;
    }

    await runTdengineQuery(selectedConnection.id, activeTab.id, activeTab.sql, activeTab.database);
  }

  function handleTdengineExport(format: "csv" | "json") {
    if (!selectedConnection || selectedConnection.kind !== "tdengine") {
      return;
    }

    const activeTab = selectedTdengineTabs.find((tab) => tab.id === selectedTdengineActiveTabId);
    const result = activeTab?.result;
    if (!result) {
      return;
    }

    const filenameBase = `${selectedConnection.name}-${activeTab?.title ?? "query"}`.replace(/[^\w.-]+/g, "-");
    const content =
      format === "json"
        ? JSON.stringify(result.rows, null, 2)
        : tdengineRowsToCsv(
            result.columns.map((column) => column.name),
            result.rows,
          );
    const blob = new Blob([content], { type: format === "json" ? "application/json" : "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${filenameBase}.${format}`;
    anchor.click();
    URL.revokeObjectURL(url);
    setNotice({
      tone: "success",
      message: `${format.toUpperCase()} exported.`,
    });
  }

  function handleExport() {
    const payload = connections.map((connection) => ({
      ...connection,
      exportedAt: new Date().toISOString(),
    }));
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `middleware-studio-export-${new Date().toISOString().slice(0, 10)}.json`;
    anchor.click();
    URL.revokeObjectURL(url);
    setNotice({
      tone: "success",
      message: t("notices.connectionsExported"),
    });
  }

  function handleImportTrigger() {
    importInputRef.current?.click();
  }

  async function handleImport(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    try {
      const raw = JSON.parse(await file.text()) as Array<Partial<ConnectionRecord>>;
      let imported = 0;

      for (const entry of raw) {
        const kind = entry.kind ?? "redis";
        const draft = createEmptyDraft(kind);
        draft.name = entry.name ?? "";
        draft.host = entry.host ?? "127.0.0.1";
        draft.port = entry.port ?? draft.port;
        draft.protocol = kind === "tdengine" ? entry.protocol ?? "ws" : "";
        draft.databaseName = entry.databaseName ?? draft.databaseName;
        draft.username = entry.username ?? draft.username;
        draft.authMode = entry.authMode ?? draft.authMode;
        draft.environment = entry.environment ?? draft.environment;
        draft.tagsInput = entry.tags?.join(", ") ?? "";
        draft.readonly = entry.readonly ?? false;
        draft.useTls = entry.useTls ?? false;
        draft.tlsVerify = entry.tlsVerify ?? true;
        draft.sshEnabled = entry.sshEnabled ?? false;
        draft.sshHost = entry.sshHost ?? "";
        draft.sshPort = entry.sshPort ?? 22;
        draft.sshUsername = entry.sshUsername ?? "";
        draft.schemaRegistryUrl = entry.schemaRegistryUrl ?? "";
        draft.groupId = entry.groupId ?? "";
        draft.clientId = entry.clientId ?? "";
        draft.notes = entry.notes ?? "";

        if (!draft.name.trim() || !draft.host.trim()) {
          continue;
        }

        await desktopApi.saveConnection(draft);
        imported += 1;
      }

      await refreshConnections();
      setNotice({
        tone: imported ? "success" : "info",
        message: imported ? t("notices.importedCount", { count: imported }) : t("notices.noValidConnections"),
      });
    } catch (error) {
      setNotice({
        tone: "danger",
        message: error instanceof Error ? error.message : t("notices.importFailed"),
      });
    } finally {
      event.target.value = "";
    }
  }

  const tdengineContent =
    selectedConnection?.kind === "tdengine" ? (
      <main className={clsx("app-shell", isSidebarCollapsed && "sidebar-collapsed", isResourceTreeCollapsed && "resource-tree-collapsed")}>
        <Sidebar
          connections={filteredConnections}
          selectedId={selectedId}
          query={query}
          collapsed={isSidebarCollapsed}
          healthById={healthById}
          counts={counts}
          onQueryChange={setQuery}
          onSelect={handleSelectConnection}
          onCreate={() => openCreateModal(selectedConnection.kind)}
          onEdit={(connection) =>
            setModalState({
              open: true,
              mode: "edit",
              draft: draftFromConnection(connection),
            })
          }
          onToggleFavorite={handleToggleFavorite}
          onToggleCollapse={() => setIsSidebarCollapsed((current) => !current)}
          onExport={handleExport}
          onImport={handleImportTrigger}
        />

        <ResourceTree
          title={activeResourceTitle}
          resources={activeResources}
          selectedResourceId={selectedResourceId}
          collapsed={isResourceTreeCollapsed}
          onSelect={handleResourceSelect}
          onToggleCollapse={() => setIsResourceTreeCollapsed((current) => !current)}
          onExpandNode={handleTdengineExpandNode}
        />

        <TdengineWorkspace
          connection={selectedConnection}
          detail={selectedTdengineDetail}
          health={selectedHealth}
          tabs={selectedTdengineTabs}
          activeTabId={selectedTdengineActiveTabId}
          databaseOptions={selectedTdengineDatabases}
          catalog={selectedTdengineCatalog}
          favorites={selectedTdengineFavorites}
          history={selectedTdengineHistory}
          runtimeMode={desktopApi.isDesktopRuntime() ? "desktop" : "preview"}
          onCreateTab={handleTdengineCreateTab}
          onSelectTab={(tabId) =>
            setTdengineActiveTabIdById((current) => ({
              ...current,
              [selectedConnection.id]: tabId,
            }))
          }
          onCloseTab={handleTdengineCloseTab}
          onSelectDatabase={handleTdengineSelectDatabase}
          onUpdateTabSql={handleTdengineUpdateTabSql}
          onApplyPreviewSql={handleTdengineApplyPreviewSql}
          onSaveFavorite={handleTdengineSaveFavorite}
          onUseFavorite={handleTdengineUseFavorite}
          onDeleteFavorite={handleTdengineDeleteFavorite}
          onApplySuggestion={handleTdengineApplySuggestion}
          onUseHistoryItem={handleTdengineUseHistoryItem}
          onRunQuery={(tabId) => void handleTdengineRunQuery(tabId)}
          onExportResult={handleTdengineExport}
          onRunHealthCheck={handleRunHealthCheck}
          onEdit={() =>
            setModalState({
              open: true,
              mode: "edit",
              draft: draftFromConnection(selectedConnection),
            })
          }
          onDelete={handleDeleteConnection}
        />
      </main>
    ) : null;

  const appContent =
    selectedConnection?.kind === "redis" ? (
      <main className="redis-studio-shell">
        <RedisNavRail
          connectionCount={redisConnections.length}
          onCreate={() => openCreateModal("redis")}
          onImport={handleImportTrigger}
          onExport={handleExport}
        />

        <RedisWorkspace
          connection={selectedConnection}
          connections={redisConnections}
          browse={selectedRedisBrowse}
          detail={selectedRedisDetail}
          selectedKeyIds={selectedRedisKeyIds}
          slowlog={selectedRedisSlowlog}
          health={selectedHealth}
          selectedDatabase={selectedRedisDb}
          selectedResourceId={selectedResourceId}
          tab={workspaceTab}
          runtimeMode={desktopApi.isDesktopRuntime() ? "desktop" : "preview"}
          onTabChange={setWorkspaceTab}
          onSelectConnection={handleSelectConnection}
          onSelectDatabase={handleRedisSelectDatabase}
          onBrowseChange={handleRedisBrowseChange}
          onSelectResource={handleResourceSelect}
          onToggleKeySelection={handleRedisToggleKeySelection}
          onToggleAllVisibleKeys={handleRedisToggleAllVisibleKeys}
          onClearKeySelection={handleRedisClearKeySelection}
          onClearSelection={handleRedisClearSelection}
          onRunHealthCheck={handleRunHealthCheck}
          onEditConnection={() =>
            setModalState({
              open: true,
              mode: "edit",
              draft: draftFromConnection(selectedConnection),
            })
          }
          onDeleteConnection={handleDeleteConnection}
          onRefresh={handleRedisRefresh}
          onLoadMore={handleRedisLoadMore}
          onSaveValue={handleRedisSaveValue}
          onCreateKey={handleRedisCreateKey}
          onUpdateTtl={handleRedisUpdateTtl}
          onDeleteKey={handleRedisDeleteKey}
          onPreviewBulkDelete={handleRedisPreviewBulkDelete}
          onRunBulkDelete={handleRedisRunBulkDelete}
          onRunCli={handleRedisRunCli}
          onRunWorkbench={handleRedisRunWorkbench}
          onRefreshSlowlog={handleRedisRefreshSlowlog}
          onRefreshStream={handleRedisRefreshStream}
        />
      </main>
    ) : tdengineContent ? (
      tdengineContent
    ) : (
      <main className={clsx("app-shell", isSidebarCollapsed && "sidebar-collapsed", isResourceTreeCollapsed && "resource-tree-collapsed")}>
        <Sidebar
          connections={filteredConnections}
          selectedId={selectedId}
          query={query}
          collapsed={isSidebarCollapsed}
          healthById={healthById}
          counts={counts}
          onQueryChange={setQuery}
          onSelect={handleSelectConnection}
          onCreate={() => openCreateModal()}
          onEdit={(connection) =>
            setModalState({
              open: true,
              mode: "edit",
              draft: draftFromConnection(connection),
            })
          }
          onToggleFavorite={handleToggleFavorite}
          onToggleCollapse={() => setIsSidebarCollapsed((current) => !current)}
          onExport={handleExport}
          onImport={handleImportTrigger}
        />

        <ResourceTree
          title={activeResourceTitle}
          resources={activeResources}
          selectedResourceId={selectedResourceId}
          collapsed={isResourceTreeCollapsed}
          onSelect={handleResourceSelect}
          onToggleCollapse={() => setIsResourceTreeCollapsed((current) => !current)}
        />

        <Workspace
          connection={selectedConnection}
          snapshot={selectedSnapshot}
          health={selectedHealth}
          selectedResourceId={selectedResourceId}
          tab={workspaceTab}
          runtimeMode={desktopApi.isDesktopRuntime() ? "desktop" : "preview"}
          onTabChange={setWorkspaceTab}
          onRunHealthCheck={handleRunHealthCheck}
          onEdit={() =>
            selectedConnection
              ? setModalState({
                  open: true,
                  mode: "edit",
                  draft: draftFromConnection(selectedConnection),
                })
              : null
          }
          onDelete={handleDeleteConnection}
        />
      </main>
    );

  return (
    <>
      {appContent}

      {notice ? <div className={`notice-banner ${notice.tone}`}>{notice.message}</div> : null}

      <div className="app-language-switcher" aria-label={t("language.switcherLabel")}>
        <button
          type="button"
          className={clsx("app-language-chip", language === "zh-CN" && "active")}
          onClick={() => setLanguage("zh-CN")}
        >
          {t("language.chinese")}
        </button>
        <button
          type="button"
          className={clsx("app-language-chip", language === "en-US" && "active")}
          onClick={() => setLanguage("en-US")}
        >
          {t("language.english")}
        </button>
      </div>

      {isLoading ? <div className="loading-scrim">{t("common.loading")}</div> : null}

      <input ref={importInputRef} type="file" accept="application/json" hidden onChange={handleImport} />

      <ConnectionModal
        open={modalState.open}
        mode={modalState.mode}
        initialDraft={modalState.draft}
        onClose={() => setModalState((current) => ({ ...current, open: false }))}
        onSave={handleSaveConnection}
      />
    </>
  );
}

export default App;
