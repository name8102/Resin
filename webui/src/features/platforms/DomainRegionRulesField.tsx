import type { UseFormReturn } from "react-hook-form";
import { Textarea } from "../../components/ui/Textarea";
import { useI18n } from "../../i18n";
import type { PlatformFormValues } from "./formModel";

export function DomainRegionRulesField({ id, form }: { id: string; form: UseFormReturn<PlatformFormValues> }) {
  const { t } = useI18n();
  const error = form.formState.errors.domain_region_rules_text?.message;
  return (
    <div className="field-group">
      <label className="field-label" htmlFor={id}>{t("域名地区规则")}</label>
      <Textarea
        id={id}
        rows={4}
        placeholder={t("每行一条，如 chatgpt.com = us,jp")}
        invalid={Boolean(error)}
        aria-invalid={Boolean(error)}
        aria-describedby={`${id}-hint${error ? ` ${id}-error` : ""}`}
        {...form.register("domain_region_rules_text")}
      />
      <p id={`${id}-hint`} className="muted" style={{ marginTop: 4, fontSize: 12 }}>
        {t("包含子域名，最具体的域名优先；与平台地区过滤取交集。没有符合地区的节点时请求失败。")}
      </p>
      {error ? <p id={`${id}-error`} className="field-error" role="alert">{t(error)}</p> : null}
    </div>
  );
}
