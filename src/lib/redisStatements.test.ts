import { describe, expect, it } from "vitest";
import { splitRedisStatements } from "./redisStatements";

describe("splitRedisStatements", () => {
  it("splits new lines and semicolons outside quotes", () => {
    expect(splitRedisStatements("GET session:1;\nTTL session:1")).toEqual(["GET session:1", "TTL session:1"]);
  });

  it("keeps semicolons inside quoted values", () => {
    expect(splitRedisStatements('SET greeting "hello;world"; GET greeting')).toEqual([
      'SET greeting "hello;world"',
      "GET greeting",
    ]);
  });

  it("keeps escaped quotes intact", () => {
    expect(splitRedisStatements(String.raw`SET note "hello \"redis\""; TYPE note`)).toEqual([
      String.raw`SET note "hello \"redis\""`,
      "TYPE note",
    ]);
  });
});
