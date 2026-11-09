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


// Bare domains include subdomains; patterns also accept glob stars.
export function parseDomainRegionRules(input: string | undefined): DomainRegionRule[] {
  const seen = new Set<string>();
  return parseLinesToList(input).map((line) => {
    const parts = line.split("=");
    const rawDomain = parts[0]?.trim().toLowerCase().replace(/\.$/, "") ?? "";
    if (parts.length !== 2 || !rawDomain || /[\s/:@?#]/.test(rawDomain)) {
      throw new Error("域名地区规则格式：域名 = 地区代码，如 chatgpt.com = us,jp");
    }
    const domain = rawDomain.split(".").map((label) => {
      if (label.includes("*")) {
        if (label.length > 63 || !/^[a-z0-9*](?:[a-z0-9*-]*[a-z0-9*])?$/.test(label)) {
          throw new Error("通配域名标签只能包含 ASCII 字母、数字、连字符和 *");
        }
        return label;
      }
      const ascii = new URL(`http://${label}.invalid`).hostname.slice(0, -8);
      if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(ascii) || ascii.length > 63) {
        throw new Error("域名标签格式无效");
      }
      return ascii;
    }).join(".");
    if (domain.length > 253 || /^\d+\.\d+\.\d+\.\d+$/.test(domain)) {
      throw new Error("须填写 DNS 域名，不支持 IP 地址");
    }
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
