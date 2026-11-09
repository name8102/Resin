import assert from "node:assert/strict";
import { test } from "node:test";
import { parseDomainRegionRules } from "../src/features/platforms/formParsers.ts";

test("domain region lines normalize domains and region filters", () => {
  assert.deepEqual(parseDomainRegionRules(" CHATGPT.COM. = US,JP\n\nexample.com = !cn "), [
    { domain: "chatgpt.com", regions: ["us", "jp"] },
    { domain: "example.com", regions: ["!cn"] },
  ]);
  assert.deepEqual(parseDomainRegionRules(""), []);
});

test("domain region lines reject malformed and duplicate rules", () => {
  for (const text of ["example.com", "example.com =", "https://example.com = us", "api?.example.com = us", "bad..example.com = us", "192.0.2.1 = us", "example.com = USA", "EXAMPLE.com = us\nexample.com. = jp"]) {
    assert.throws(() => parseDomainRegionRules(text), text);
  }
});

test("domain region lines accept wildcards, IDNA and exclusions", () => {
  assert.deepEqual(parseDomainRegionRules(" DLSITE.COM = JP\n*.EXAMPLE.COM. = !JP,!KR\napi-*.例子.测试 = us\n* = !cn"), [
    { domain: "dlsite.com", regions: ["jp"] },
    { domain: "*.example.com", regions: ["!jp", "!kr"] },
    { domain: "api-*.xn--fsqu00a.xn--0zwm56d", regions: ["us"] },
    { domain: "*", regions: ["!cn"] },
  ]);
});
