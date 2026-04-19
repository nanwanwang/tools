import type { RedisBrowseData, RedisKeySummary, ResourceNode, WorkspaceMetric } from "../types";

function namespaceLabel(key: string) {
  if (key.includes(":")) {
    return `${key.split(":")[0]}:*`;
  }

  if (key.includes(".")) {
    return `${key.split(".")[0]}.*`;
  }

  return "misc";
}

export function buildRedisResources(database: number, summaries: RedisKeySummary[]): ResourceNode[] {
  const groups = new Map<string, RedisKeySummary[]>();

  for (const summary of summaries) {
    const namespace = summary.namespace || namespaceLabel(summary.key);
    const items = groups.get(namespace) ?? [];
    items.push(summary);
    groups.set(namespace, items);
  }

  const children = [...groups.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([namespace, items]) => ({
      id: `prefix:${namespace}`,
      label: namespace,
      kind: "prefix",
      meta: `${items.length} keys`,
      children: items
        .slice()
        .sort((left, right) => left.key.localeCompare(right.key))
        .map((item) => ({
          id: item.id,
          label: item.displayName,
          kind: item.keyType,
          meta: item.meta,
        })),
    }));

  return [
    {
      id: `db:${database}`,
      label: `db${database}`,
      kind: "database",
      meta: `${summaries.length} loaded keys`,
      children,
    },
  ];
}

function buildMetrics(summaries: RedisKeySummary[], readonly: boolean): WorkspaceMetric[] {
  const namespaces = new Set(summaries.map((summary) => summary.namespace || namespaceLabel(summary.key)));
  const ttlCount = summaries.filter((summary) => summary.ttlSeconds !== null).length;

  return [
    { label: "Loaded keys", value: String(summaries.length), detail: "Accumulated browser results", tone: "accent" },
    { label: "Namespaces", value: String(namespaces.size), detail: "Grouped by prefix", tone: "neutral" },
    {
      label: "Write safety",
      value: readonly ? "Locked" : "Guarded",
      detail: ttlCount ? `${ttlCount} keys with TTL` : "No expiring keys in view",
      tone: readonly ? "success" : "danger",
    },
  ];
}

export function mergeRedisBrowsePages(previous: RedisBrowseData | null, next: RedisBrowseData): RedisBrowseData {
  if (
    !previous ||
    previous.database !== next.database ||
    previous.pattern !== next.pattern ||
    previous.searchMode !== next.searchMode ||
    previous.typeFilter !== next.typeFilter ||
    previous.viewMode !== next.viewMode ||
    previous.connectionId !== next.connectionId
  ) {
    return {
      ...next,
      resources: buildRedisResources(next.database, next.keySummaries),
      metrics: buildMetrics(next.keySummaries, next.capability.readonly),
      loadedCount: next.keySummaries.length,
    };
  }

  const mergedMap = new Map<string, RedisKeySummary>();
  for (const summary of previous.keySummaries) {
    mergedMap.set(summary.key, summary);
  }
  for (const summary of next.keySummaries) {
    mergedMap.set(summary.key, summary);
  }

  const mergedSummaries = [...mergedMap.values()].sort((left, right) => left.key.localeCompare(right.key));

  return {
    ...next,
    keySummaries: mergedSummaries,
    resources: buildRedisResources(next.database, mergedSummaries),
    metrics: buildMetrics(mergedSummaries, next.capability.readonly),
    loadedCount: mergedSummaries.length,
    scannedCount: previous.scannedCount + next.scannedCount,
  };
}
