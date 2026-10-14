import { create } from "@bufbuild/protobuf";
import type { UsageLimitEntry } from "@/hooks/useUsageLimitQueries";
import { InstanceSetting_UsageLimitsSetting_UsageLimitSchema } from "@/types/proto/api/instance_service_pb";
import { formatFileSize } from "@/utils/format";

/** The metrics an entry can limit, in display order. */
export const USAGE_METRICS = ["memoCount", "memoContentBytes", "storageBytes", "uploadBytes"] as const;
export type UsageMetric = (typeof USAGE_METRICS)[number];

export function newUsageLimitEntry(
  init: { users?: string[]; allUsers?: boolean } & Partial<Record<UsageMetric, bigint | undefined>>,
): UsageLimitEntry {
  return create(InstanceSetting_UsageLimitsSetting_UsageLimitSchema, init);
}

const MIB = 1024 * 1024;
const inputUnit = (metric: UsageMetric) => (metric === "memoContentBytes" ? 1024 : MIB);

/** Whether the metric counts bytes. Forms enter content limits in KiB and other byte limits in MiB. */
export const isByteMetric = (metric: UsageMetric) => metric !== "memoCount";

/** Whether the value restricts anything. Unset and 0 both mean unlimited. */
export const isLimited = (value: bigint | undefined): value is bigint => value !== undefined && value > 0n;

/** Formats a limit for a form field: memos as a count, content in KiB, other bytes in MiB. Unlimited is empty. */
export function toLimitInput(metric: UsageMetric, value: bigint | undefined): string {
  if (!isLimited(value)) return "";
  if (!isByteMetric(metric)) return value.toString();
  const size = Number(value) / inputUnit(metric);
  return Number.isInteger(size) ? String(size) : String(Number(size.toFixed(2)));
}

/**
 * Parses a form field into a limit value. An empty field, or 0, is unlimited
 * and stored as unset. Returns null when the input is not a valid limit.
 */
export function parseLimitInput(metric: UsageMetric, input: string): bigint | undefined | null {
  const text = input.trim();
  if (text === "") return undefined;
  let value: bigint;
  if (!isByteMetric(metric)) {
    if (!/^\d+$/.test(text)) return null;
    value = BigInt(text);
  } else {
    if (!/^\d+(\.\d+)?$/.test(text)) return null;
    value = BigInt(Math.round(Number(text) * inputUnit(metric)));
  }
  return value === 0n ? undefined : value;
}

/** Formats a limit or a usage figure for display. */
export function formatLimitValue(metric: UsageMetric, value: bigint): string {
  return isByteMetric(metric) ? formatFileSize(Number(value)) : value.toLocaleString();
}
