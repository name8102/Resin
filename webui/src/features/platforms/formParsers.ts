import type { DomainRegionRule } from "./types";

export function parseLinesToList(input: string | undefined, normalize?: (value: string) => string): string[] {
  if (!input) {
    return [];
  }

  return input
    .split(/\n/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => (normalize ? normalize(item) : item));
}

export function parseHeaderLines(input: string | undefined): string[] {
  const lines = parseLinesToList(input);
  const seen = new Set<string>();
  const headers: string[] = [];
  for (const line of lines) {
    const key = line.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    headers.push(line);
  }
  return headers;
}


// Domain rules use one suffix and comma-separated region filters per line.
export function parseDomainRegionRules(input: string | undefined): DomainRegionRule[] {
  const seen = new Set<string>();
  return parseLinesToList(input).map((line) => {
    const parts = line.split("=");
    const rawDomain = parts[0]?.trim().toLowerCase().replace(/\.$/, "") ?? "";
    if (parts.length !== 2 || !rawDomain || /[\s/:@*?#]/.test(rawDomain)) {
      throw new Error("域名地区规则格式：域名 = 地区代码，如 chatgpt.com = us,jp");
    }
    const domain = new URL(`http://${rawDomain}`).hostname;
    if (seen.has(domain)) {
      throw new Error("域名地区规则不能包含重复域名");
    }
    seen.add(domain);
    const regions = parts[1].split(",").map((region) => region.trim().toLowerCase());
    if (regions.some((region) => !/^!?[a-z]{2}$/.test(region))) {
      throw new Error("地区代码须为两个字母，可用 ! 排除地区");
    }
    return { domain, regions };
  });
}
