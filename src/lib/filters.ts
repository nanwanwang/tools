import type { ConnectionRecord } from "../types";

function compareDates(left: string | null, right: string | null) {
  if (left === right) {
    return 0;
  }

  if (!left) {
    return 1;
  }

  if (!right) {
    return -1;
  }

  return right.localeCompare(left);
}

export function sortConnections(connections: ConnectionRecord[]) {
  return [...connections].sort((left, right) => {
    if (left.favorite !== right.favorite) {
      return left.favorite ? -1 : 1;
    }

    const recentCompare = compareDates(left.lastConnectedAt, right.lastConnectedAt);
    if (recentCompare !== 0) {
      return recentCompare;
    }

    const checkedCompare = compareDates(left.lastCheckedAt, right.lastCheckedAt);
    if (checkedCompare !== 0) {
      return checkedCompare;
    }

    return right.updatedAt.localeCompare(left.updatedAt);
  });
}

export function filterConnections(connections: ConnectionRecord[], query: string) {
  const normalized = query.trim().toLowerCase();
  const sorted = sortConnections(connections);

  if (!normalized) {
    return sorted;
  }

  return sorted.filter((connection) => {
    const haystack = [
      connection.name,
      connection.host,
      connection.kind,
      connection.environment,
      connection.databaseName,
      connection.username,
      connection.tags.join(" "),
      connection.notes,
    ]
      .join(" ")
      .toLowerCase();

    return haystack.includes(normalized);
  });
}

export function countByKind(connections: ConnectionRecord[]) {
  return connections.reduce<Record<string, number>>((accumulator, connection) => {
    accumulator[connection.kind] = (accumulator[connection.kind] ?? 0) + 1;
    return accumulator;
  }, {});
}
