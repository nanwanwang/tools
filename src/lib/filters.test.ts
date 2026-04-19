import { describe, expect, it } from "vitest";
import { filterConnections, sortConnections } from "./filters";
import type { ConnectionRecord } from "../types";

const baseConnection: ConnectionRecord = {
  id: "1",
  kind: "redis",
  protocol: "",
  name: "orders-cache",
  host: "127.0.0.1",
  port: 6379,
  databaseName: "0",
  username: "default",
  authMode: "password",
  environment: "dev",
  tags: ["orders"],
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

describe("sortConnections", () => {
  it("puts favorites and recent targets first", () => {
    const items: ConnectionRecord[] = [
      { ...baseConnection, id: "alpha", name: "alpha", favorite: false, updatedAt: "2026-04-10T10:00:00.000Z" },
      {
        ...baseConnection,
        id: "bravo",
        name: "bravo",
        favorite: true,
        updatedAt: "2026-04-11T10:00:00.000Z",
      },
      {
        ...baseConnection,
        id: "charlie",
        name: "charlie",
        favorite: false,
        lastConnectedAt: "2026-04-12T10:00:00.000Z",
        updatedAt: "2026-04-12T10:00:00.000Z",
      },
    ];

    expect(sortConnections(items).map((item) => item.id)).toEqual(["bravo", "charlie", "alpha"]);
  });
});

describe("filterConnections", () => {
  it("matches host, name and tags", () => {
    const items: ConnectionRecord[] = [
      { ...baseConnection, id: "redis", name: "orders-cache", tags: ["cache", "orders"] },
      { ...baseConnection, id: "kafka", kind: "kafka", name: "payments-bus", host: "mq.internal", tags: ["events"] },
    ];

    expect(filterConnections(items, "mq.internal").map((item) => item.id)).toEqual(["kafka"]);
    expect(filterConnections(items, "orders").map((item) => item.id)).toEqual(["redis"]);
  });
});
