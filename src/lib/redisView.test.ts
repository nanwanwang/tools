import { describe, expect, it } from "vitest";
import type { RedisBrowseData, RedisKeySummary } from "../types";
import { buildRedisResources, mergeRedisBrowsePages } from "./redisView";

const baseSummary = (key: string): RedisKeySummary => ({
  id: `key:${key}`,
  key,
  keyType: "string",
  ttlSeconds: null,
  size: 12,
  namespace: key.includes(":") ? `${key.split(":")[0]}:*` : "misc",
  displayName: key,
  meta: "string | No TTL | size 12",
});

const baseBrowse = (keys: string[]): RedisBrowseData => ({
  connectionId: "redis-1",
  database: 0,
  pattern: "",
  searchMode: "pattern",
  searchPartial: false,
  limit: 2,
  cursor: "0",
  nextCursor: keys.length > 1 ? "2" : null,
  loadedCount: keys.length,
  scannedCount: keys.length,
  hasMore: keys.length > 1,
  viewMode: "tree",
  typeFilter: "all",
  metrics: [],
  resources: [],
  keySummaries: keys.map(baseSummary),
  diagnostics: [],
  infoRows: [],
  serverRows: [],
  configRows: [],
  capability: {
    connectionId: "redis-1",
    serverMode: "standalone",
    dbCount: 16,
    moduleNames: ["ReJSON"],
    supportsJson: true,
    supportsSlowlog: true,
    readonly: false,
    browserSupported: true,
    unsupportedReason: null,
    diagnostics: [],
  },
});

describe("buildRedisResources", () => {
  it("groups keys by namespace", () => {
    const resources = buildRedisResources(0, [baseSummary("session:1"), baseSummary("cache:1")]);

    expect(resources[0]?.children?.[0]?.label).toBe("cache:*");
    expect(resources[0]?.children?.[1]?.label).toBe("session:*");
  });
});

describe("mergeRedisBrowsePages", () => {
  it("accumulates paged results without duplicates", () => {
    const merged = mergeRedisBrowsePages(baseBrowse(["session:1", "session:2"]), {
      ...baseBrowse(["session:2", "session:3"]),
      cursor: "2",
      nextCursor: null,
      hasMore: false,
    });

    expect(merged.keySummaries.map((item) => item.key)).toEqual(["session:1", "session:2", "session:3"]);
    expect(merged.loadedCount).toBe(3);
    expect(merged.resources[0]?.children?.[0]?.children?.length).toBe(3);
  });
});
