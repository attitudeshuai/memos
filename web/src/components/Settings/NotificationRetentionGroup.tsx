import { timestampDate } from "@bufbuild/protobuf/wkt";
import { Loader2Icon, PlayIcon } from "lucide-react";
import toast from "react-hot-toast";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { handleError } from "@/lib/error";
import { useNotificationCleanupRuns, useRunNotificationCleanup } from "@/hooks/useInstanceQueries";
import {
  type InstanceSetting_NotificationSetting_RetentionSetting,
  type NotificationCleanupRunInfo,
  NotificationCleanupRunInfo_State,
  NotificationCleanupRunInfo_Trigger,
} from "@/types/proto/api/instance_service_pb";
import { useTranslate } from "@/utils/i18n";

function cleanupStateLabelKey(state: NotificationCleanupRunInfo_State): string {
  switch (state) {
    case NotificationCleanupRunInfo_State.RUNNING:
      return "setting.notification.cleanup-state-running";
    case NotificationCleanupRunInfo_State.SUCCEEDED:
      return "setting.notification.cleanup-state-succeeded";
    case NotificationCleanupRunInfo_State.FAILED:
      return "setting.notification.cleanup-state-failed";
    default:
      return "setting.notification.cleanup-state-unspecified";
  }
}

interface Props {
  retention: InstanceSetting_NotificationSetting_RetentionSetting;
  onChange: (retention: InstanceSetting_NotificationSetting_RetentionSetting) => void;
}

function formatRunTime(value: Date): string {
  return value.toLocaleString();
}

function NotificationRetentionGroup({ retention, onChange }: Props) {
  const t = useTranslate();
  const runCleanup = useRunNotificationCleanup();
  const runsQuery = useNotificationCleanupRuns();

  const updateField = (field: keyof InstanceSetting_NotificationSetting_RetentionSetting, raw: string) => {
    const value = Math.max(0, Number.parseInt(raw, 10) || 0);
    onChange({ ...retention, [field]: value });
  };

  const handleRunCleanup = async () => {
    try {
      const run = await runCleanup.mutateAsync();
      toast.success(
        t("setting.notification.cleanup-run-finished", {
          age: run.deletedAgeCount,
          cap: run.deletedCapCount,
        }),
      );
    } catch (error) {
      await handleError(error, toast.error, { context: "Run notification cleanup" });
    }
  };

  const runs = runsQuery.data ?? [];

  return (
    <div className="w-full space-y-4">
      <div className="space-y-2">
        <h3 className="text-sm font-medium text-foreground">{t("setting.notification.retention-title")}</h3>
        <p className="text-xs text-muted-foreground">{t("setting.notification.retention-description")}</p>
      </div>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
        <label className="flex items-center gap-2 text-sm">
          <span className="text-muted-foreground whitespace-nowrap">{t("setting.notification.retention-max-age-days")}</span>
          <Input
            type="number"
            min={0}
            className="w-28"
            value={retention.maxAgeDays}
            onChange={(e) => updateField("maxAgeDays", e.target.value)}
          />
        </label>
        <label className="flex items-center gap-2 text-sm">
          <span className="text-muted-foreground whitespace-nowrap">{t("setting.notification.retention-max-per-user")}</span>
          <Input
            type="number"
            min={0}
            className="w-28"
            value={retention.maxPerUser}
            onChange={(e) => updateField("maxPerUser", e.target.value)}
          />
        </label>
        <span className="text-xs text-muted-foreground">{t("setting.notification.retention-zero-means-unlimited")}</span>
        <div className="sm:ml-auto">
          <Button variant="outline" size="sm" disabled={runCleanup.isPending} onClick={handleRunCleanup}>
            {runCleanup.isPending ? <Loader2Icon className="w-4 h-4 mr-1 animate-spin" /> : <PlayIcon className="w-4 h-4 mr-1" />}
            {t("setting.notification.cleanup-run-now")}
          </Button>
        </div>
      </div>

      {runs.length > 0 && (
        <div className="rounded-lg border border-border overflow-hidden">
          <table className="w-full text-xs">
            <thead className="bg-muted/40 text-muted-foreground">
              <tr>
                <th className="text-left font-medium px-3 py-2">{t("setting.notification.cleanup-time")}</th>
                <th className="text-left font-medium px-3 py-2">{t("setting.notification.cleanup-trigger")}</th>
                <th className="text-left font-medium px-3 py-2">{t("setting.notification.cleanup-state")}</th>
                <th className="text-right font-medium px-3 py-2">{t("setting.notification.cleanup-deleted-age")}</th>
                <th className="text-right font-medium px-3 py-2">{t("setting.notification.cleanup-deleted-cap")}</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run: NotificationCleanupRunInfo) => (
                <tr key={run.name} className="border-t border-border/60">
                  <td className="px-3 py-2 whitespace-nowrap">{formatRunTime(timestampDate(run.createTime))}</td>
                  <td className="px-3 py-2">{t(run.trigger === NotificationCleanupRunInfo_Trigger.MANUAL ? "setting.notification.cleanup-trigger-manual" : "setting.notification.cleanup-trigger-scheduled")}</td>
                  <td className="px-3 py-2">
                    <span
                      className={
                        run.state === NotificationCleanupRunInfo_State.FAILED
                          ? "text-destructive font-medium"
                          : run.state === NotificationCleanupRunInfo_State.SUCCEEDED
                            ? "text-foreground"
                            : "text-muted-foreground"
                      }
                    >
                        {t(cleanupStateLabelKey(run.state))}
                    </span>
                    {run.errorDetail && <span className="block text-[11px] text-destructive/80 truncate" title={run.errorDetail}>{run.errorDetail}</span>}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">{run.deletedAgeCount}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{run.deletedCapCount}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default NotificationRetentionGroup;
