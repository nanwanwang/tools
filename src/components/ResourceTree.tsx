import { useEffect, useState, type ReactNode } from "react";
import clsx from "clsx";
import { ChevronRight, ChevronsLeft, ChevronsRight, Database, FolderTree, Hash, Layers3, TableProperties } from "lucide-react";
import { useI18n } from "../i18n";
import type { ResourceNode } from "../types";

interface ResourceTreeProps {
  title: string | null;
  resources: ResourceNode[] | null;
  selectedResourceId: string | null;
  collapsed: boolean;
  onSelect: (id: string) => void;
  onToggleCollapse: () => void;
  onExpandNode?: (node: ResourceNode) => Promise<void> | void;
}

function iconForNode(kind: string) {
  if (kind === "database" || kind === "schema") {
    return Database;
  }

  if (kind === "supertable") {
    return Layers3;
  }

  if (kind === "topic" || kind === "consumer-group" || kind === "partition") {
    return Layers3;
  }

  if (kind === "table" || kind === "view") {
    return TableProperties;
  }

  if (kind === "prefix" || kind === "stream") {
    return FolderTree;
  }

  return Hash;
}

function mergeExpandedState(current: Record<string, boolean>, nodes: ResourceNode[]) {
  const next = { ...current };
  for (const node of nodes) {
    if (node.children?.length || node.expandable) {
      next[node.id] = current[node.id] ?? Boolean(node.children?.length);
    }

    if (node.children?.length) {
      Object.assign(next, mergeExpandedState(next, node.children));
    }
  }

  return next;
}

function buildNodeTooltip(node: ResourceNode) {
  return [node.label, node.meta ?? "", node.kind].filter(Boolean).join("\n");
}

export function ResourceTree({
  title,
  resources,
  selectedResourceId,
  collapsed,
  onSelect,
  onToggleCollapse,
  onExpandNode,
}: ResourceTreeProps) {
  const { t } = useI18n();
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [loadingNodeIds, setLoadingNodeIds] = useState<Record<string, boolean>>({});

  useEffect(() => {
    setExpanded((current) => (resources ? mergeExpandedState(current, resources) : {}));
  }, [resources]);

  async function toggle(node: ResourceNode) {
    const willExpand = !expanded[node.id];
    if (willExpand && onExpandNode && node.expandable && !node.children?.length) {
      setLoadingNodeIds((current) => ({
        ...current,
        [node.id]: true,
      }));

      try {
        await onExpandNode(node);
      } finally {
        setLoadingNodeIds((current) => {
          const next = { ...current };
          delete next[node.id];
          return next;
        });
      }
    }

    setExpanded((current) => ({
      ...current,
      [node.id]: !current[node.id],
    }));
  }

  function renderNode(node: ResourceNode, level = 0): ReactNode {
    const Icon = iconForNode(node.kind);
    const isExpanded = expanded[node.id] ?? false;
    const hasChildren = Boolean(node.children?.length || node.expandable);
    const isLoading = Boolean(loadingNodeIds[node.id]);

    return (
      <div key={node.id} className="tree-node">
        <button
          type="button"
          className={clsx("tree-row", collapsed && "compact", selectedResourceId === node.id && "active")}
          style={{ paddingLeft: `${collapsed ? Math.min(level, 2) * 8 + 10 : level * 18 + 14}px` }}
          onClick={() => onSelect(node.id)}
          title={collapsed ? buildNodeTooltip(node) : undefined}
          aria-label={collapsed ? node.label : undefined}
        >
          {hasChildren ? (
            <span
              className={clsx("tree-caret", collapsed && "compact", isExpanded && "expanded")}
              onClick={(event) => {
                event.stopPropagation();
                void toggle(node);
              }}
            >
              <ChevronRight size={14} />
            </span>
          ) : (
            <span className={clsx("tree-caret placeholder", collapsed && "compact")} />
          )}
          <span className={clsx("tree-kind-icon", `kind-${node.kind}`)}>
            <Icon size={15} />
          </span>
          {!collapsed ? <span className="tree-label">{node.label}</span> : null}
          {!collapsed ? (
            isLoading ? <span className="tree-meta">{t("resourceTree.loading")}</span> : node.meta ? <span className="tree-meta">{node.meta}</span> : null
          ) : null}
        </button>

        {hasChildren && isExpanded ? (node.children ?? []).map((child) => renderNode(child, level + 1)) : null}
      </div>
    );
  }

  return (
    <section className={clsx("tree-panel", collapsed && "collapsed")}>
      <div className={clsx("panel-header", collapsed && "compact")}>
        {collapsed ? (
          <div className="tree-panel-collapsed-head">
            <span className="tree-panel-badge" title={title ?? t("resourceTree.selectConnection")}>
              <FolderTree size={18} />
            </span>
          </div>
        ) : (
          <div>
            <p className="eyebrow">{t("resourceTree.title")}</p>
            <h2>{title ?? t("resourceTree.selectConnection")}</h2>
          </div>
        )}
        <button
          className="icon-button"
          type="button"
          onClick={onToggleCollapse}
          title={collapsed ? t("resourceTree.expand") : t("resourceTree.collapse")}
          aria-label={collapsed ? t("resourceTree.expand") : t("resourceTree.collapse")}
        >
          {collapsed ? <ChevronsRight size={16} /> : <ChevronsLeft size={16} />}
        </button>
      </div>

      {resources ? (
        <div className={clsx("tree-scroll", collapsed && "compact")}>{resources.map((node) => renderNode(node))}</div>
      ) : (
        collapsed ? (
          <div className="tree-empty-compact" title={t("resourceTree.emptyCollapsed")}>
            <FolderTree size={18} />
          </div>
        ) : (
          <div className="empty-panel">
            <p>{t("resourceTree.emptyDescription")}</p>
          </div>
        )
      )}
    </section>
  );
}
