import clsx from "clsx";
import { ChevronsLeft, ChevronsRight, Download, Edit3, Plus, Search, Star, Upload } from "lucide-react";
import { useI18n } from "../i18n";
import type { ConnectionHealth, ConnectionRecord } from "../types";

interface SidebarProps {
  connections: ConnectionRecord[];
  selectedId: string | null;
  query: string;
  collapsed: boolean;
  healthById: Record<string, ConnectionHealth>;
  counts: Record<string, number>;
  onQueryChange: (value: string) => void;
  onSelect: (id: string) => void;
  onCreate: () => void;
  onEdit: (connection: ConnectionRecord) => void;
  onToggleFavorite: (connection: ConnectionRecord) => void;
  onToggleCollapse: () => void;
  onExport: () => void;
  onImport: () => void;
}

function MiddlewareKindIcon({ kind, compact = false }: { kind: ConnectionRecord["kind"]; compact?: boolean }) {
  return (
    <span className={clsx("middleware-kind-icon", `kind-${kind}`, compact && "compact")} aria-hidden="true">
      {kind === "redis" ? (
        <svg viewBox="0 0 24 24" fill="none">
          <path
            d="M5 7.2 12 4l7 3.2-7 3.1L5 7.2Z"
            fill="currentColor"
            opacity="0.95"
            stroke="rgba(255,255,255,0.22)"
            strokeWidth="0.7"
          />
          <path
            d="M5 12 12 8.9l7 3.1-7 3.1L5 12Z"
            fill="currentColor"
            opacity="0.78"
            stroke="rgba(255,255,255,0.18)"
            strokeWidth="0.7"
          />
          <path
            d="M5 16.8 12 13.7l7 3.1-7 3.2-7-3.2Z"
            fill="currentColor"
            opacity="0.62"
            stroke="rgba(255,255,255,0.16)"
            strokeWidth="0.7"
          />
          <circle cx="9" cy="7.2" r="1.05" fill="#fff7f4" />
          <circle cx="15.2" cy="12" r="1.05" fill="#fff7f4" />
          <circle cx="9.8" cy="16.8" r="1.05" fill="#fff7f4" />
        </svg>
      ) : kind === "tdengine" ? (
        <svg viewBox="0 0 24 24" fill="none">
          <circle cx="12" cy="12" r="9" fill="currentColor" opacity="0.92" />
          <path
            d="M6 12h2.2l1.4-3 2.2 6 1.9-4h4.3"
            stroke="#eefcff"
            strokeWidth="1.7"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      ) : kind === "kafka" ? (
        <svg viewBox="0 0 24 24" fill="none">
          <circle cx="12" cy="6" r="3" fill="currentColor" />
          <circle cx="7" cy="17" r="3" fill="currentColor" opacity="0.78" />
          <circle cx="17" cy="17" r="3" fill="currentColor" opacity="0.78" />
          <path d="M11 8.4 8.3 14M13 8.4 15.7 14M10 17h4" stroke="#f5fbff" strokeWidth="1.5" strokeLinecap="round" />
        </svg>
      ) : kind === "mysql" ? (
        <svg viewBox="0 0 24 24" fill="none">
          <ellipse cx="12" cy="6.4" rx="6.2" ry="2.8" fill="currentColor" />
          <path d="M5.8 6.4v7.2c0 1.6 2.8 2.8 6.2 2.8s6.2-1.2 6.2-2.8V6.4" fill="currentColor" opacity="0.78" />
          <path d="M5.8 10c0 1.6 2.8 2.8 6.2 2.8s6.2-1.2 6.2-2.8" stroke="#eef7ff" strokeWidth="1.2" opacity="0.9" />
        </svg>
      ) : (
        <svg viewBox="0 0 24 24" fill="none">
          <path d="M7.5 18.5V8.7c0-1.6 2-2.9 4.5-2.9s4.5 1.3 4.5 2.9v9.8" fill="currentColor" opacity="0.82" />
          <path d="M9.2 12.2h5.6M9.2 15.3h4.2" stroke="#f6f5ff" strokeWidth="1.6" strokeLinecap="round" />
          <path d="M8.2 7.6c.5-1.4 2-2.4 3.8-2.4 1.9 0 3.4 1 3.9 2.4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        </svg>
      )}
    </span>
  );
}

export function Sidebar({
  connections,
  selectedId,
  query,
  collapsed,
  healthById,
  counts,
  onQueryChange,
  onSelect,
  onCreate,
  onEdit,
  onToggleFavorite,
  onToggleCollapse,
  onExport,
  onImport,
}: SidebarProps) {
  const { t, kindLabel, environmentLabel } = useI18n();
  const orderedEnvironments = ["local", "dev", "staging", "production"] as const;

  function buildCollapsedConnectionTooltip(connection: ConnectionRecord, health: ConnectionHealth | undefined) {
    const lines = [
      connection.name,
      `${kindLabel(connection.kind)} | ${connection.host}:${connection.port}`,
      connection.databaseName ? `${t("sidebar.databaseLabel")}: ${connection.databaseName}` : "",
      connection.protocol ? `${t("sidebar.protocolLabel")}: ${connection.protocol}` : "",
      connection.username ? `${t("sidebar.userLabel")}: ${connection.username}` : "",
      `${t("sidebar.environmentLabel")}: ${environmentLabel(connection.environment)}`,
      connection.readonly ? t("sidebar.readOnly") : t("sidebar.readWrite"),
      `${t("sidebar.healthLabel")}: ${health?.summary ?? t("sidebar.noHealthCheckYet")}`,
    ].filter(Boolean);

    return lines.join("\n");
  }

  return (
    <aside className={clsx("sidebar", collapsed && "collapsed")}>
      <div className="brand-block">
        {!collapsed ? (
          <div className="sidebar-brand-copy">
            <p className="eyebrow">{t("sidebar.eyebrow")}</p>
            <h1>{t("sidebar.title")}</h1>
          </div>
        ) : (
          <div className="sidebar-brand-mark">
            <MiddlewareKindIcon kind="tdengine" />
          </div>
        )}

        <div className="sidebar-brand-actions">
          <button
            className="icon-button"
            type="button"
            onClick={onToggleCollapse}
            title={collapsed ? t("sidebar.expand") : t("sidebar.collapse")}
            aria-label={collapsed ? t("sidebar.expand") : t("sidebar.collapse")}
          >
            {collapsed ? <ChevronsRight size={16} /> : <ChevronsLeft size={16} />}
          </button>
          <button
            className={clsx(collapsed ? "icon-button" : "primary-button small")}
            type="button"
            onClick={onCreate}
            title={t("sidebar.addConnection")}
            aria-label={t("sidebar.addConnection")}
          >
            <Plus size={16} />
            {!collapsed ? t("common.add") : null}
          </button>
        </div>
      </div>

      {!collapsed ? (
        <>
          <div className="sidebar-card sidebar-toolbar">
            <label className="search-box">
              <Search size={16} />
              <input value={query} onChange={(event) => onQueryChange(event.target.value)} placeholder={t("sidebar.searchPlaceholder")} />
            </label>

            <div className="toolbar-actions">
              <button className="ghost-button small" type="button" onClick={onImport}>
                <Upload size={15} />
                {t("common.import")}
              </button>
              <button className="ghost-button small" type="button" onClick={onExport}>
                <Download size={15} />
                {t("common.export")}
              </button>
            </div>
          </div>

          <div className="sidebar-card stats-grid">
            {(["redis", "kafka", "mysql", "postgres", "tdengine"] as const).map((kind) => (
              <div key={kind} className="mini-stat">
                <span>{kindLabel(kind)}</span>
                <strong>{counts[kind] ?? 0}</strong>
              </div>
            ))}
          </div>
        </>
      ) : (
        <div className="sidebar-collapsed-actions">
          <button className="icon-button" type="button" onClick={onImport} title={t("sidebar.importConnections")} aria-label={t("sidebar.importConnections")}>
            <Upload size={15} />
          </button>
          <button className="icon-button" type="button" onClick={onExport} title={t("sidebar.exportConnections")} aria-label={t("sidebar.exportConnections")}>
            <Download size={15} />
          </button>
        </div>
      )}

      <div className="connection-scroll">
        {orderedEnvironments.map((environment) => {
          const environmentConnections = connections.filter((connection) => connection.environment === environment);
          if (!environmentConnections.length) {
            return null;
          }

          return (
            <section key={environment} className={clsx("connection-group", collapsed && "compact")}>
              <div className={clsx("connection-group-header", collapsed && "compact")}>
                <span>{collapsed ? environment.toUpperCase().slice(0, 3) : environmentLabel(environment)}</span>
                {!collapsed ? <span>{environmentConnections.length}</span> : null}
              </div>

              <div className={clsx("connection-list", collapsed && "compact")}>
                {environmentConnections.map((connection) => {
                  const health = healthById[connection.id];
                  return (
                    <button
                      key={connection.id}
                      type="button"
                      className={clsx("connection-item", selectedId === connection.id && "active", collapsed && "icon-only")}
                      onClick={() => onSelect(connection.id)}
                      title={collapsed ? buildCollapsedConnectionTooltip(connection, health) : undefined}
                      aria-label={collapsed ? connection.name : undefined}
                    >
                      {collapsed ? (
                        <div className="connection-icon-stack">
                          <MiddlewareKindIcon kind={connection.kind} />
                          <span className={clsx("status-dot connection-health-dot", health?.status ?? "unreachable")} />
                          {connection.favorite ? (
                            <span className="connection-favorite-dot">
                              <Star size={10} fill="currentColor" />
                            </span>
                          ) : null}
                        </div>
                      ) : (
                        <>
                          <div className="connection-item-topline">
                            <div className="connection-name-row">
                              <MiddlewareKindIcon kind={connection.kind} compact />
                              <span>{connection.name}</span>
                            </div>
                            <div className="connection-item-actions">
                              <button
                                className={clsx("icon-button tiny", connection.favorite && "active")}
                                type="button"
                                aria-label={t("sidebar.toggleFavorite")}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  onToggleFavorite(connection);
                                }}
                              >
                                <Star size={14} fill={connection.favorite ? "currentColor" : "none"} />
                              </button>
                              <button
                                className="icon-button tiny"
                                type="button"
                                aria-label={t("sidebar.editConnection")}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  onEdit(connection);
                                }}
                              >
                                <Edit3 size={14} />
                              </button>
                            </div>
                          </div>
                          <div className="connection-meta-row">
                            <span>{kindLabel(connection.kind)}</span>
                            <span>
                              {connection.host}:{connection.port}
                            </span>
                          </div>
                          <div className="connection-meta-row">
                            <span className={clsx("status-dot", health?.status ?? "unreachable")} />
                            <span>{health?.summary ?? t("sidebar.noHealthCheckYet")}</span>
                          </div>
                          <div className="connection-tag-row">
                            {connection.readonly ? <span className="tag critical">{t("sidebar.readOnly")}</span> : null}
                            {connection.tags.slice(0, 2).map((tag) => (
                              <span key={tag} className="tag">
                                {tag}
                              </span>
                            ))}
                          </div>
                        </>
                      )}
                    </button>
                  );
                })}
              </div>
            </section>
          );
        })}

        {!connections.length ? (
          <div className="sidebar-card empty-card">
            <p className="eyebrow">{t("sidebar.noSavedTargets")}</p>
            <h3>{t("sidebar.startWithConnection")}</h3>
            <p>{t("sidebar.emptyDescription")}</p>
          </div>
        ) : null}
      </div>
    </aside>
  );
}
