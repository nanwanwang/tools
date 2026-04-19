import { Bell, Cloud, Database, Download, Plus, Settings, Upload } from "lucide-react";
import { useI18n } from "../i18n";

interface RedisNavRailProps {
  connectionCount: number;
  onCreate: () => void;
  onImport: () => void;
  onExport: () => void;
}

export function RedisNavRail({ connectionCount, onCreate, onImport, onExport }: RedisNavRailProps) {
  const { t } = useI18n();
  const topItems = [
    { id: "databases", label: t("redis.breadcrumbsDatabases"), icon: Database, active: true },
    { id: "insights", label: "Insights", icon: Bell },
  ];
  const bottomItems = [{ id: "settings", label: "Settings", icon: Settings }];

  return (
    <aside className="redis-nav-rail">
      <div className="redis-nav-brand">
        <div className="redis-nav-logo">R</div>
      </div>

      <div className="redis-nav-stack">
        {topItems.map((item) => {
          const Icon = item.icon;
          return (
            <button key={item.id} type="button" className={`redis-nav-button ${item.active ? "active" : ""}`} aria-label={item.label}>
              <Icon size={18} />
              {item.id === "databases" ? <span className="redis-nav-badge">{connectionCount}</span> : null}
            </button>
          );
        })}
      </div>

      <div className="redis-nav-divider" />

      <div className="redis-nav-stack">
        <button type="button" className="redis-nav-button" aria-label={t("sidebar.addConnection")} onClick={onCreate}>
          <Plus size={18} />
        </button>
        <button type="button" className="redis-nav-button" aria-label={t("sidebar.importConnections")} onClick={onImport}>
          <Upload size={18} />
        </button>
        <button type="button" className="redis-nav-button" aria-label={t("sidebar.exportConnections")} onClick={onExport}>
          <Download size={18} />
        </button>
      </div>

      <div className="redis-nav-spacer" />

      <div className="redis-nav-stack">
        <button type="button" className="redis-nav-button" aria-label={t("sidebar.cloudSync")}>
          <Cloud size={18} />
        </button>
        {bottomItems.map((item) => {
          const Icon = item.icon;
          return (
            <button key={item.id} type="button" className="redis-nav-button" aria-label={item.label}>
              <Icon size={18} />
            </button>
          );
        })}
      </div>
    </aside>
  );
}
