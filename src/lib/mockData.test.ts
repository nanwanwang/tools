import { describe, expect, it } from "vitest";
import { buildWorkspaceSnapshot, createEmptyDraft, redisResourceIdToKey } from "./mockData";
import { buildMockRedisBrowse, buildMockRedisBulkDelete, buildMockRedisKeyDetail } from "./mockRedis";
import type { ConnectionRecord } from "../types";

function record(kind: ConnectionRecord["kind"]): ConnectionRecord {
  const draft = createEmptyDraft(kind);

  return {
    id: `id-${kind}`,
    kind,
    protocol: kind === "tdengine" ? "ws" : "",
    name: `${kind}-target`,
    host: "127.0.0.1",
    port: draft.port,
    databaseName: draft.databaseName,
    username: draft.username,
    authMode: draft.authMode,
    environment: "dev",
    tags: [],
    readonly: false,
    favorite: false,
    useTls: false,
    tlsVerify: true,
    sshEnabled: false,
    sshHost: "",
    sshPort: 22,
    sshUsername: "",
    schemaRegistryUrl: "",
    groupId: "",
    clientId: "",
    notes: "",
    lastCheckedAt: null,
    lastConnectedAt: null,
    createdAt: "2026-04-13T10:00:00.000Z",
    updatedAt: "2026-04-13T10:00:00.000Z",
  };
}

describe("buildWorkspaceSnapshot", () => {
  it("creates redis-specific capability tags", () => {
    const snapshot = buildWorkspaceSnapshot(record("redis"));

    expect(snapshot.title).toBe("Redis browser");
    expect(snapshot.capabilityTags).toContain("SCAN paging");
    expect(snapshot.resources[0]?.children?.[0]?.label).toBe("sessions:*");
  });

  it("creates kafka and sql variants", () => {
    const kafkaSnapshot = buildWorkspaceSnapshot(record("kafka"));
    const postgresSnapshot = buildWorkspaceSnapshot(record("postgres"));
    const tdengineSnapshot = buildWorkspaceSnapshot(record("tdengine"));

    expect(kafkaSnapshot.title).toBe("Kafka workspace");
    expect(postgresSnapshot.title).toContain("PostgreSQL");
    expect(postgresSnapshot.panels[0]?.language).toBe("sql");
    expect(tdengineSnapshot.title).toBe("TDengine workspace");
  });
});

describe("buildMockRedisBrowse", () => {
  it("filters keys and keeps the selected key detail", () => {
    const redisRecord = record("redis");
    const browser = buildMockRedisBrowse(redisRecord, {
      database: 0,
      pattern: "cache",
      searchMode: "fuzzy",
      typeFilter: "all",
      limit: 10,
      cursor: null,
      viewMode: "tree",
    });
    const detail = buildMockRedisKeyDetail("cache:feed");

    expect(browser.loadedCount).toBe(1);
    expect(detail.key).toBe("cache:feed");
    expect(browser.resources[0]?.children?.[0]?.label).toBe("cache:*");
    expect(browser.configRows.length).toBeGreaterThan(0);
    expect(browser.capability.supportsSlowlog).toBe(true);
  });

  it("maps resource ids back to redis keys", () => {
    expect(redisResourceIdToKey("key:session:2048")).toBe("session:2048");
    expect(redisResourceIdToKey("prefix:session:*")).toBeNull();
  });

  it("returns structured stream previews in preview mode", () => {
    const redisRecord = record("redis");
    const browser = buildMockRedisBrowse(redisRecord, {
      database: 0,
      pattern: "orders.stream",
      searchMode: "pattern",
      typeFilter: "all",
      limit: 10,
      cursor: null,
      viewMode: "tree",
    });
    const detail = buildMockRedisKeyDetail("orders.stream");

    expect(detail.keyType).toBe("stream");
    expect(detail.previewLanguage).toBe("json");
    expect(detail.formatPreviews.auto).toContain("\"orderId\": \"ord_482901\"");
    expect(browser.serverRows[0]?.label).toBe("Role");
  });

  it("supports redis pattern search with wildcard characters", () => {
    const redisRecord = record("redis");
    const browser = buildMockRedisBrowse(redisRecord, {
      database: 0,
      pattern: "session:*",
      searchMode: "pattern",
      typeFilter: "all",
      limit: 10,
      cursor: null,
      viewMode: "tree",
    });
    const bulkPreview = buildMockRedisBulkDelete("session:*", "all", true);

    expect(browser.keySummaries.map((item) => item.key)).toContain("session:2048");
    expect(browser.keySummaries.map((item) => item.key)).not.toContain("cache:feed");
    expect(bulkPreview.matched).toBe(1);
  });

  it("applies type filters before returning browser results", () => {
    const redisRecord = record("redis");
    const browser = buildMockRedisBrowse(redisRecord, {
      database: 0,
      pattern: "",
      searchMode: "pattern",
      typeFilter: "stream",
      limit: 20,
      cursor: null,
      viewMode: "tree",
    });

    expect(browser.keySummaries.length).toBeGreaterThan(0);
    expect(browser.keySummaries.every((item) => item.keyType === "stream")).toBe(true);
  });
});
