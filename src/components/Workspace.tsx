import clsx from "clsx";
import { AlertTriangle, Cable, PencilLine, Play, ShieldAlert, Trash2 } from "lucide-react";
import { useI18n } from "../i18n";
import type { ConnectionHealth, ConnectionRecord, WorkspaceSnapshot, WorkspaceTab } from "../types";
import { findNode } from "../lib/mockData";

interface WorkspaceProps {
  connection: ConnectionRecord | null;
  snapshot: WorkspaceSnapshot | null;
  health: ConnectionHealth | null;
  selectedResourceId: string | null;
  tab: WorkspaceTab;
  runtimeMode: "desktop" | "preview";
  onTabChange: (tab: WorkspaceTab) => void;
  onRunHealthCheck: () => void;
  onEdit: () => void;
  onDelete: () => void;
}

const tabs: WorkspaceTab[] = ["overview", "explorer", "actions", "diagnostics"];

export function Workspace({
  connection,
  snapshot,
  health,
  selectedResourceId,
  tab,
  runtimeMode,
  onTabChange,
  onRunHealthCheck,
  onEdit,
  onDelete,
}: WorkspaceProps) {
  const { t, formatDateTime, environmentLabel, kindLabel } = useI18n();
  if (!connection || !snapshot) {
    return (
      <section className="workspace">
        <div className="empty-workspace">
          <p className="eyebrow">{t("workspace.emptyEyebrow")}</p>
          <h2>{t("workspace.emptyTitle")}</h2>
          <p>{t("workspace.emptyDescription")}</p>
          <div className="starter-grid">
            <article className="starter-card">
              <h3>{t("workspace.redisCardTitle")}</h3>
              <p>{t("workspace.redisCardDescription")}</p>
            </article>
            <article className="starter-card">
              <h3>{t("workspace.kafkaCardTitle")}</h3>
              <p>{t("workspace.kafkaCardDescription")}</p>
            </article>
            <article className="starter-card">
              <h3>{t("workspace.databaseCardTitle")}</h3>
              <p>{t("workspace.databaseCardDescription")}</p>
            </article>
          </div>
        </div>
      </section>
    );
  }

  const selectedNode = findNode(snapshot.resources, selectedResourceId) ?? snapshot.resources[0] ?? null;

  return (
    <section className="workspace">
      <div className="workspace-hero">
        <div>
          <p className="eyebrow">
            {kindLabel(connection.kind)} | {environmentLabel(connection.environment)}
          </p>
          <h2>{connection.name}</h2>
          <p>{snapshot.subtitle}</p>
        </div>

        <div className="hero-actions">
          <button className="ghost-button small" type="button" onClick={onEdit}>
            <PencilLine size={15} />
            {t("common.edit")}
          </button>
          <button className="ghost-button small" type="button" onClick={onRunHealthCheck}>
            <Play size={15} />
            {t("common.healthCheck")}
          </button>
          <button className="ghost-button small danger" type="button" onClick={onDelete}>
            <Trash2 size={15} />
            {t("common.delete")}
          </button>
        </div>
      </div>

      <div className="workspace-chip-row">
        <span className={clsx("tag", connection.readonly && "critical")}>{connection.readonly ? t("workspace.readOnly") : t("workspace.writeGuarded")}</span>
        <span className="tag">{connection.host}:{connection.port}</span>
        {connection.useTls ? <span className="tag">{t("workspace.tls")}</span> : null}
        {connection.sshEnabled ? <span className="tag">{t("workspace.sshTunnel")}</span> : null}
        {snapshot.capabilityTags.map((tag) => (
          <span key={tag} className="tag">
            {tag}
          </span>
        ))}
      </div>

      <div className="tab-row">
        {tabs.map((candidate) => (
          <button
            key={candidate}
            type="button"
            className={clsx("tab-button", tab === candidate && "active")}
            onClick={() => onTabChange(candidate)}
          >
            {t(`workspace.tabs.${candidate}`)}
          </button>
        ))}
      </div>

      {tab === "overview" ? (
        <div className="workspace-grid">
          <div className="panel card-grid">
            {snapshot.metrics.map((metric) => (
              <article key={metric.label} className={clsx("metric-card", metric.tone)}>
                <span>{metric.label}</span>
                <strong>{metric.value}</strong>
                <p>{metric.detail}</p>
              </article>
            ))}
          </div>

          <div className="panel panel-stack">
            <div className="panel-header compact">
              <div>
                <p className="eyebrow">{t("workspace.connectionProfile")}</p>
                <h3>{t("workspace.guardrails")}</h3>
              </div>
            </div>
            <div className="details-grid">
              <div>
                <span>{t("workspace.authMode")}</span>
                <strong>{connection.authMode}</strong>
              </div>
              <div>
                <span>{t("workspace.database")}</span>
                <strong>{connection.databaseName || t("workspace.notAvailable")}</strong>
              </div>
              <div>
                <span>{t("workspace.tags")}</span>
                <strong>{connection.tags.join(", ") || t("common.none")}</strong>
              </div>
              <div>
                <span>{t("workspace.schemaRegistry")}</span>
                <strong>{connection.schemaRegistryUrl || t("common.none")}</strong>
              </div>
            </div>
          </div>

          <div className="panel panel-stack">
            {snapshot.panels.map((panel) => (
              <article key={panel.title} className="preview-card">
                <p className="eyebrow">{panel.eyebrow}</p>
                <h3>{panel.title}</h3>
                <p>{panel.description}</p>
                <pre>{panel.content}</pre>
              </article>
            ))}
          </div>
        </div>
      ) : null}

      {tab === "explorer" ? (
        <div className="workspace-grid two-column">
          <div className="panel panel-stack">
            <div className="panel-header compact">
              <div>
                <p className="eyebrow">{t("workspace.selectedResource")}</p>
                <h3>{selectedNode?.label ?? t("workspace.chooseNode")}</h3>
              </div>
            </div>
            <div className="details-grid">
              <div>
                <span>{t("workspace.kind")}</span>
                <strong>{selectedNode?.kind ?? t("workspace.notAvailable")}</strong>
              </div>
              <div>
                <span>{t("workspace.summary")}</span>
                <strong>{selectedNode?.meta ?? t("workspace.noSummary")}</strong>
              </div>
              <div>
                <span>{t("workspace.connection")}</span>
                <strong>{connection.name}</strong>
              </div>
              <div>
                <span>{t("workspace.runtime")}</span>
                <strong>{runtimeMode === "desktop" ? t("workspace.desktop") : t("workspace.previewFallback")}</strong>
              </div>
            </div>
          </div>

          <div className="panel panel-stack">
            {snapshot.panels.map((panel) => (
              <article key={panel.title} className="preview-card compact">
                <p className="eyebrow">{panel.eyebrow}</p>
                <h3>{panel.title}</h3>
                <p>{panel.description}</p>
                <pre>{panel.content}</pre>
              </article>
            ))}
          </div>
        </div>
      ) : null}

      {tab === "actions" ? (
        <div className="workspace-grid two-column">
          <div className="panel panel-stack">
            <div className="panel-header compact">
              <div>
                <p className="eyebrow">{t("workspace.operationalActions")}</p>
                <h3>{t("workspace.safeDefaults")}</h3>
              </div>
            </div>
            <div className="action-grid">
              {snapshot.actions.map((action) => (
                <article key={action.title} className={clsx("action-card", action.tone)}>
                  <h3>{action.title}</h3>
                  <p>{action.description}</p>
                </article>
              ))}
            </div>
          </div>

          <div className="panel danger-panel">
            <div className="danger-heading">
              <ShieldAlert size={18} />
              <h3>{t("workspace.highRiskTitle")}</h3>
            </div>
            <p>{t("workspace.highRiskDescription")}</p>
            <div className="danger-list">
              <div>
                <AlertTriangle size={15} />
                <span>{t("workspace.askAgain")}</span>
              </div>
              <div>
                <Cable size={15} />
                <span>{t("workspace.keepDiagnosticsClose")}</span>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {tab === "diagnostics" ? (
        <div className="workspace-grid two-column">
          <div className="panel panel-stack">
            <div className="panel-header compact">
              <div>
                <p className="eyebrow">{t("workspace.healthStatus")}</p>
                <h3>{health?.summary ?? t("workspace.noLiveCheck")}</h3>
              </div>
            </div>
            <div className="health-card">
              <span className={clsx("status-dot", health?.status ?? "unreachable")} />
              <div>
                <strong>{health?.status ?? t("workspace.notAvailable")}</strong>
                <p>{health ? t("workspace.checkedAt", { value: formatDateTime(health.checkedAt) }) : t("workspace.runHealthCheckHint")}</p>
                {health?.latencyMs ? <p>{t("workspace.latency", { value: health.latencyMs })}</p> : null}
              </div>
            </div>
            <ul className="diagnostic-list">
              {(health?.details ?? snapshot.diagnostics).map((detail) => (
                <li key={detail}>{detail}</li>
              ))}
            </ul>
          </div>

          <div className="panel panel-stack">
            <div className="panel-header compact">
              <div>
                <p className="eyebrow">{t("workspace.runtimeNotes")}</p>
                <h3>{runtimeMode === "desktop" ? t("workspace.desktopCommandsActive") : t("workspace.previewFallbackActive")}</h3>
              </div>
            </div>
            <ul className="diagnostic-list">
              <li>{t("workspace.runtimeNote1")}</li>
              <li>{t("workspace.runtimeNote2")}</li>
              <li>{t("workspace.runtimeNote3")}</li>
            </ul>
          </div>
        </div>
      ) : null}
    </section>
  );
}
