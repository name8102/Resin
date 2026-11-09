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
  for (const text of ["example.com", "example.com =", "https://example.com = us", "*.example.com = us", "example.com = USA", "EXAMPLE.com = us\nexample.com. = jp"]) {
    assert.throws(() => parseDomainRegionRules(text), text);
  }
});
