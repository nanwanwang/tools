import { describe, expect, it } from "vitest";
import { buildMockTdengineCatalog, buildMockTdengineObjectDetail, buildMockTdengineQueryResult } from "./mockTdengine";
import { createEmptyDraft, getDefaultPort } from "./mockData";
import {
  buildTdengineNodeId,
  buildTdengineAutocompleteSuggestions,
  buildTdengineQuickTemplates,
  filterTdengineResultRows,
  inspectTdengineSql,
  listTdengineDatabases,
  paginateTdengineRows,
  parseTdengineNodeId,
  projectTdengineRows,
  removeTdengineSavedQuery,
  sortTdengineResultRows,
  upsertTdengineSavedQuery,
} from "./tdengine";
import type { ConnectionRecord, TdengineSavedQuery } from "../types";

const tdengineConnection: ConnectionRecord = {
  id: "td-1",
  kind: "tdengine",
  protocol: "ws",
  name: "td-preview",
  host: "127.0.0.1",
  port: 6041,
  databaseName: "power",
  username: "root",
  authMode: "password",
  environment: "dev",
  tags: [],
  readonly: true,
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
  createdAt: "2026-04-18T10:00:00.000Z",
  updatedAt: "2026-04-18T10:00:00.000Z",
};

describe("tdengine helpers", () => {
  it("switches default ports by protocol", () => {
    const draft = createEmptyDraft("tdengine");

    expect(draft.protocol).toBe("ws");
    expect(draft.port).toBe(6041);
    expect(getDefaultPort("tdengine", "native")).toBe(6030);
  });

  it("classifies read-only and blocked SQL", () => {
    expect(inspectTdengineSql("select * from power.meter_events").allowed).toBe(true);
    expect(inspectTdengineSql("use `power`").database).toBe("power");
    expect(inspectTdengineSql("insert into power.meter_events values(now, 'x', 'y', 1)").allowed).toBe(false);
    expect(inspectTdengineSql("select 1; show databases").allowed).toBe(false);
  });

  it("round-trips TDengine node ids", () => {
    const nodeId = buildTdengineNodeId({
      database: "power",
      objectKind: "child-table",
      supertable: "meters",
      objectName: "d1001",
    });

    expect(parseTdengineNodeId(nodeId)).toEqual({
      database: "power",
      objectKind: "child-table",
      supertable: "meters",
      objectName: "d1001",
    });
  });

  it("lists available databases without duplicating the fallback database", () => {
    const roots = buildMockTdengineCatalog();

    expect(listTdengineDatabases(roots, "power")).toEqual(["power", "ops"]);
    expect(listTdengineDatabases(null, "power")).toEqual(["power"]);
  });

  it("upserts and removes saved sql entries", () => {
    const current: TdengineSavedQuery[] = [
      {
        id: "fav-1",
        title: "Recent events",
        database: "power",
        sql: "select * from power.meter_events",
        updatedAt: "2026-04-18T10:00:00.000Z",
      },
    ];

    const upserted = upsertTdengineSavedQuery(current, {
      id: "fav-2",
      title: "Recent events",
      database: "power",
      sql: " SELECT * FROM power.meter_events ",
      updatedAt: "2026-04-18T10:05:00.000Z",
    });

    expect(upserted).toHaveLength(1);
    expect(upserted[0]?.id).toBe("fav-2");
    expect(upserted[0]?.sql).toBe("SELECT * FROM power.meter_events");
    expect(removeTdengineSavedQuery(upserted, "fav-2")).toEqual([]);
  });

  it("builds quick templates and autocomplete suggestions", () => {
    const roots = buildMockTdengineCatalog();
    const detail = buildMockTdengineObjectDetail("power", "meter_events", "table");
    const templates = buildTdengineQuickTemplates({
      currentDatabase: "power",
      databases: listTdengineDatabases(roots, "power"),
      detail,
      resources: [...roots, ...buildMockTdengineCatalog("power")],
      favorites: [
        {
          id: "fav-1",
          title: "Saved meter events",
          database: "power",
          sql: "select * from `power`.`meter_events` limit 20",
          updatedAt: "2026-04-18T10:00:00.000Z",
        },
      ],
    });

    const suggestions = buildTdengineAutocompleteSuggestions("meter", {
      currentDatabase: "power",
      databases: ["power", "ops"],
      detail,
      resources: [...roots, ...buildMockTdengineCatalog("power")],
      favorites: [],
    });

    expect(templates.some((entry) => entry.label === "Show Databases")).toBe(true);
    expect(templates.some((entry) => entry.kind === "favorite")).toBe(true);
    expect(suggestions.some((entry) => entry.sql.includes("meter_events"))).toBe(true);
  });

  it("sorts and paginates tdengine result rows", () => {
    const rows = [
      { device: "d1002", value: 2, online: false },
      { device: "d1001", value: 10, online: true },
      { device: "d1003", value: null, online: null },
    ];

    const sorted = sortTdengineResultRows(rows, "value", "asc");
    const descSorted = sortTdengineResultRows(rows, "value", "desc");
    const paged = paginateTdengineRows(sorted, 2, 2);

    expect(sorted[0]?.device).toBe("d1002");
    expect(sorted[1]?.device).toBe("d1001");
    expect(sorted[2]?.device).toBe("d1003");
    expect(descSorted[0]?.device).toBe("d1001");
    expect(descSorted[2]?.device).toBe("d1003");
    expect(paged.currentPage).toBe(2);
    expect(paged.totalPages).toBe(2);
    expect(paged.rows[0]?.device).toBe("d1003");
  });

  it("filters and projects tdengine result rows by visible columns", () => {
    const rows = [
      { device: "d1002", status: "offline", value: 2 },
      { device: "d1001", status: "online", value: 10 },
    ];

    const filtered = filterTdengineResultRows(rows, ["device", "status"], "offline");
    const projected = projectTdengineRows(filtered, ["device"]);

    expect(filtered).toHaveLength(1);
    expect(filtered[0]?.device).toBe("d1002");
    expect(projected).toEqual([{ device: "d1002" }]);
  });

  it("combines global search with per-column filters", () => {
    const rows = [
      { device: "d1002", status: "offline", group_name: "north", value: 2 },
      { device: "d1001", status: "online", group_name: "north", value: 10 },
      { device: "d2001", status: "online", group_name: "south", value: 10 },
    ];

    const filtered = filterTdengineResultRows(rows, ["device", "status", "group_name", "value"], "online", {
      device: "d100",
      group_name: "north",
      hidden_column: "ignored",
    });

    expect(filtered).toEqual([{ device: "d1001", status: "online", group_name: "north", value: 10 }]);
  });
});

describe("mock tdengine catalog", () => {
  it("loads lazy catalog branches", () => {
    const roots = buildMockTdengineCatalog();
    const powerChildren = buildMockTdengineCatalog("power");
    const childTables = buildMockTdengineCatalog("power", "meters");

    expect(roots[0]?.kind).toBe("database");
    expect(roots[0]?.expandable).toBe(true);
    expect(powerChildren.some((node) => node.kind === "supertable")).toBe(true);
    expect(childTables.every((node) => node.kind === "child-table")).toBe(true);
  });

  it("builds object detail and read-only query results", () => {
    const detail = buildMockTdengineObjectDetail("power", "meters", "supertable");
    const childDetail = buildMockTdengineObjectDetail("power", "d1001", "child-table");
    const result = buildMockTdengineQueryResult(tdengineConnection, "power", "select * from power.meter_events", 1000);
    const switched = buildMockTdengineQueryResult(tdengineConnection, "power", "use ops", 1000);

    expect(detail.tagColumns.length).toBeGreaterThan(0);
    expect(detail.tagValueRows).toEqual([]);
    expect(detail.previewSql).toContain("limit 200");
    expect(childDetail.tagValueRows[0]?.label).toBe("group_id");
    expect(result.columns.length).toBeGreaterThan(0);
    expect(result.database).toBe("power");
    expect(switched.database).toBe("ops");
  });
});
