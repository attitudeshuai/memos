import { CheckCheckIcon, TrashIcon } from "lucide-react";
import { useState } from "react";
import toast from "react-hot-toast";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { buildNotificationBatchScope, useBatchDeleteNotifications, useBatchUpdateNotifications } from "@/hooks/useUserQueries";
import { handleError } from "@/lib/error";
import { cn } from "@/lib/utils";
import { UserNotification_Type } from "@/types/proto/api/user_service_pb";
import { useTranslate } from "@/utils/i18n";

const typeOptions = [
  { value: UserNotification_Type.TYPE_UNSPECIFIED, labelKey: "inbox.batch.type-all" },
  { value: UserNotification_Type.MEMO_COMMENT, labelKey: "inbox.batch.type-comment" },
  { value: UserNotification_Type.MEMO_MENTION, labelKey: "inbox.batch.type-mention" },
  { value: UserNotification_Type.SPACE_INVITATION, labelKey: "inbox.batch.type-space-invitation" },
] as const;

function NotificationBatchToolbar() {
  const t = useTranslate();
  const batchRead = useBatchUpdateNotifications();
  const batchDelete = useBatchDeleteNotifications();
  const [typeValue, setTypeValue] = useState<UserNotification_Type>(UserNotification_Type.TYPE_UNSPECIFIED);
  const [beforeValue, setBeforeValue] = useState("");
  const [confirmOpen, setConfirmOpen] = useState(false);

  const buildInput = () => ({
    type: typeValue,
    beforeTime: beforeValue ? new Date(beforeValue) : undefined,
  });

  const isPending = batchRead.isPending || batchDelete.isPending;

  const handleMarkRead = async () => {
    try {
      const response = await batchRead.mutateAsync(buildInput());
      toast.success(t("inbox.batch.mark-read-success", { count: response.affectedCount }));
    } catch (error) {
      await handleError(error, toast.error, { context: "Batch mark notifications read" });
    }
  };

  const handleDelete = async () => {
    try {
      const response = await batchDelete.mutateAsync(buildInput());
      toast.success(t("inbox.batch.delete-success", { count: response.affectedCount }));
      setConfirmOpen(false);
    } catch (error) {
      await handleError(error, toast.error, { context: "Batch delete notifications" });
    }
  };

  return (
    <div className="flex flex-wrap items-center gap-2 px-4 py-2.5 border-b border-border/60 bg-muted/20">
      <select
        value={typeValue}
        onChange={(e) => setTypeValue(Number(e.target.value) as UserNotification_Type)}
        aria-label={t("inbox.batch.type-label")}
        className={cn(
          "h-9 rounded-md border border-input bg-background px-3 text-sm text-foreground shadow-sm",
          "focus:outline-none focus:ring-1 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50",
        )}
        disabled={isPending}
      >
        {typeOptions.map((option) => (
          <option key={option.value} value={option.value}>
            {t(option.labelKey)}
          </option>
        ))}
      </select>

      <label className="flex items-center gap-1.5 text-sm text-muted-foreground">
        <span>{t("inbox.batch.before")}</span>
        <input
          type="datetime-local"
          value={beforeValue}
          onChange={(e) => setBeforeValue(e.target.value)}
          disabled={isPending}
          className="h-9 rounded-md border border-input bg-background px-2 text-sm text-foreground shadow-sm focus:outline-none focus:ring-1 focus:ring-ring disabled:opacity-50"
        />
        {beforeValue && (
          <button type="button" className="text-xs text-muted-foreground hover:text-foreground" onClick={() => setBeforeValue("")}>
            {t("inbox.batch.before-clear")}
          </button>
        )}
      </label>

      <div className="ml-auto flex items-center gap-2">
        <Button variant="outline" size="sm" disabled={isPending} onClick={handleMarkRead}>
          <CheckCheckIcon className="w-4 h-4 mr-1" strokeWidth={2} />
          {t("inbox.batch.mark-read")}
        </Button>
        <Button variant="destructive" size="sm" disabled={isPending} onClick={() => setConfirmOpen(true)}>
          <TrashIcon className="w-4 h-4 mr-1" strokeWidth={2} />
          {t("inbox.batch.delete")}
        </Button>
      </div>

      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t("inbox.batch.delete-confirm-title")}</DialogTitle>
            <DialogDescription>{t("inbox.batch.delete-confirm-description")}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmOpen(false)}>
              {t("common.cancel")}
            </Button>
            <Button variant="destructive" disabled={batchDelete.isPending} onClick={handleDelete}>
              {t("common.delete")}
            </Button>
          </DialogFooter>
        </DialogContentContent>
      </Dialog>
    </div>
  );
}

export default NotificationBatchToolbar;
