import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  ReactFlowProvider,
} from "@xyflow/react";
import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { CirclePlay, SkipForward, RotateCcw, RefreshCw, History, Database, Play, X, ChevronDown, Square, Terminal, Download } from "lucide-react";
import dagre from "dagre";
import "@xyflow/react/dist/style.css";
import "ag-grid-community/styles/ag-grid.css";
import "ag-grid-community/styles/ag-theme-quartz.css";

ModuleRegistry.registerModules([AllCommunityModule]);

const NODE_WIDTH = 280;
const NODE_HEIGHT = 142;
const FIT_OPTIONS = { padding: 0.18, minZoom: 0.2, maxZoom: 1.2 };
const RUN_SNAPSHOT = {
  runId: "7137cc46-5791-435f-a797-dde698549c1b",
  runLabel: "cli_stream_verify_20260405003908",
  runStatus: "success_with_warnings",
};
const PIPELINE_META = {
  pipelineId: "pipeline",
  pipelineName: "Policy Servicing Pipeline",
};

const EDGE_LIST = [
  ["python_113", "sql_121"],
  ["sql_107", "sql_105"],
  ["sql_107", "sql_121"],
  ["sql_115", "sql_121"],
  ["sql_117", "sql_121"],
  ["sql_119", "sql_121"],
  ["sql_121", "sql_126"],
  ["sql_121", "sql_131"],
  ["sql_78", "sql_91"],
  ["sql_79", "sql_141"],
  ["sql_79", "sql_95"],
  ["sql_81", "sql_93"],
  ["sql_83", "sql_97"],
  ["sql_85", "sql_97"],
  ["sql_87", "sql_97"],
  ["sql_89", "sql_97"],
  ["sql_91", "sql_103"],
  ["sql_91", "sql_107"],
  ["sql_91", "sql_133"],
  ["sql_93", "sql_91"],
  ["sql_95", "sql_91"],
  ["sql_97", "sql_91"],
  ["sql_99", "sql_97"],
];

const NODE_DETAILS = {
  sql_78: { kind: "postgres.ingress", artifact: "main.policy_core", rows: 18, startedAt: "2026-04-05T05:41:19.876666Z", finishedAt: "2026-04-05T05:41:21.024605Z" },
  sql_79: { kind: "postgres.ingress", artifact: "main.policy_coverage", rows: 50, startedAt: "2026-04-05T05:41:21.191295Z", finishedAt: "2026-04-05T05:41:22.519570Z" },
  sql_81: { kind: "postgres.ingress", artifact: "main.policy_claim", rows: 10, startedAt: "2026-04-05T05:41:22.737701Z", finishedAt: "2026-04-05T05:41:23.998346Z" },
  sql_83: { kind: "db2.ingress", artifact: "main.customer_policy_link", rows: 18, startedAt: "2026-04-05T05:41:24.212205Z", finishedAt: "2026-04-05T05:41:28.165386Z" },
  sql_85: { kind: "db2.ingress", artifact: "main.customer_person", rows: 12, startedAt: "2026-04-05T05:41:28.358372Z", finishedAt: "2026-04-05T05:41:30.434056Z" },
  sql_87: { kind: "db2.ingress", artifact: "main.customer_address", rows: 12, startedAt: "2026-04-05T05:41:30.574489Z", finishedAt: "2026-04-05T05:41:32.681755Z" },
  sql_89: { kind: "db2.ingress", artifact: "main.customer_phone", rows: 12, startedAt: "2026-04-05T05:41:32.858470Z", finishedAt: "2026-04-05T05:41:35.049841Z" },
  sql_99: { kind: "db2.ingress", artifact: "main.customer_email", rows: 12, startedAt: "2026-04-05T05:41:35.218956Z", finishedAt: "2026-04-05T05:41:37.350588Z" },
  sql_95: { kind: "model.sql", artifact: "main.coverage_summary", rows: 18, startedAt: "2026-04-05T05:41:38.505330Z", finishedAt: "2026-04-05T05:41:39.230817Z" },
  sql_93: { kind: "model.sql", artifact: "main.claim_summary", rows: 10, startedAt: "2026-04-05T05:41:39.408841Z", finishedAt: "2026-04-05T05:41:40.155747Z" },
  sql_97: { kind: "model.sql", artifact: "main.customer_profile", rows: 18, startedAt: "2026-04-05T05:41:37.532044Z", finishedAt: "2026-04-05T05:41:38.341683Z" },
  sql_141: { kind: "csv.egress", artifact: "exports/outputpolicy_coverage.csv", rows: 50, startedAt: "2026-04-05T05:41:40.319222Z", finishedAt: "2026-04-05T05:41:40.854060Z" },
  sql_91: { kind: "model.sql", artifact: "main.policy_customer_360", rows: 18, startedAt: "2026-04-05T05:41:41.004607Z", finishedAt: "2026-04-05T05:41:41.744003Z" },
  sql_103: { kind: "check.boolean", artifact: null, rows: null, startedAt: "2026-04-05T05:41:41.881281Z", finishedAt: "2026-04-05T05:41:42.364237Z" },
  sql_107: { kind: "model.sql", artifact: "main.policy_servicing_queue", rows: 18, startedAt: "2026-04-05T05:41:42.506254Z", finishedAt: "2026-04-05T05:41:43.192435Z" },
  sql_105: { kind: "check.count", artifact: null, rows: null, startedAt: "2026-04-05T05:41:43.321145Z", finishedAt: "2026-04-05T05:41:43.764540Z" },
  sql_133: { kind: "csv.egress", artifact: "exports/output.csv", rows: 18, startedAt: "2026-04-05T05:41:43.891006Z", finishedAt: "2026-04-05T05:41:44.345034Z" },
  python_113: { kind: "python.ingress", artifact: "main.service_playbook", rows: 3, startedAt: "2026-04-05T05:41:44.486829Z", finishedAt: "2026-04-05T05:41:45.191030Z" },
  sql_117: { kind: "csv.ingress", artifact: "main.vip_customers", rows: 2, startedAt: "2026-04-05T05:41:45.323851Z", finishedAt: "2026-04-05T05:41:46.020633Z" },
  sql_115: { kind: "jsonl.ingress", artifact: "main.servicing_overrides", rows: 2, startedAt: "2026-04-05T05:41:46.151150Z", finishedAt: "2026-04-05T05:41:46.895620Z" },
  sql_119: { kind: "parquet.ingress", artifact: "main.segment_targets", rows: 4, startedAt: "2026-04-05T05:41:47.044867Z", finishedAt: "2026-04-05T05:41:47.837047Z" },
  sql_121: { kind: "model.sql", artifact: "main.policy_servicing_enriched", rows: 18, startedAt: "2026-04-05T05:41:47.993551Z", finishedAt: "2026-04-05T05:41:48.801533Z" },
  sql_126: { kind: "db2.egress", artifact: '"DB2INST1"."policy_servicing_enriched_load"', rows: 18, startedAt: "2026-04-05T05:41:48.952901Z", finishedAt: "2026-04-05T05:41:50.565138Z", warnings: [{ code: "db2_precision_widened" }] },
  sql_131: { kind: "postgres.egress", artifact: '"public"."policy_servicing_enriched_load"', rows: 18, startedAt: "2026-04-05T05:41:50.736838Z", finishedAt: "2026-04-05T05:41:51.347323Z" },
};
const NODE_QUERY_DETAILS = {
  python_113: { resolvedSql: null },
  sql_115: { resolvedSql: null },
  sql_117: { resolvedSql: null },
  sql_119: { resolvedSql: null },
  sql_78: { resolvedSql: "SELECT\n    p.policy_id,\n    policy_number,\n    product_line,\n    underwriting_company,\n    status,\n    effective_date,\n    expiration_date,\n    written_premium,\n    deductible_amount,\n    billing_plan,\n    agent_code,\n    risk_state\nFROM \"public\".\"policy\" p" },
  sql_79: { resolvedSql: "SELECT\n    coverage_id,\n    policy_id,\n    coverage_code,\n    coverage_name,\n    coverage_limit AS coverage_limit1,\n    deductible_amount,\n    premium_amount,\n    is_optional\nFROM \"public\".\"coverage\"" },
  sql_81: { resolvedSql: "SELECT\n    claim_id,\n    claim_number,\n    policy_id,\n    loss_date,\n    reported_date,\n    claim_status,\n    loss_cause,\n    reserve_amount,\n    paid_amount,\n    claim_state,\n    adjuster_name,\n    closed_date\nFROM \"public\".\"claim\"" },
  sql_83: { resolvedSql: "SELECT\n    POLICY_KEY,\n    POLICY_NUMBER,\n    PERSON_ID,\n    RELATIONSHIP_ROLE,\n    EFFECTIVE_DATE,\n    EXPIRATION_DATE,\n    PREFERRED_CONTACT_CHANNEL\nFROM \"DB2INST1\".\"POLICY\"" },
  sql_85: { resolvedSql: "SELECT\n    PERSON_ID,\n    CUSTOMER_NUMBER,\n    FIRST_NAME,\n    LAST_NAME,\n    BIRTH_DATE,\n    OCCUPATION,\n    ANNUAL_INCOME,\n    PREFERRED_LANGUAGE\nFROM \"DB2INST1\".\"PERSON\"" },
  sql_87: { resolvedSql: "SELECT\n    PERSON_ID,\n    ADDRESS_TYPE,\n    ADDRESS_LINE1,\n    CITY,\n    STATE_CODE,\n    POSTAL_CODE,\n    COUNTRY_CODE,\n    IS_PRIMARY\nFROM \"DB2INST1\".\"ADDRESS\"\nWHERE IS_PRIMARY = 1" },
  sql_89: { resolvedSql: "SELECT\n    PERSON_ID,\n    PHONE_TYPE,\n    PHONE_NUMBER,\n    IS_PRIMARY\nFROM \"DB2INST1\".\"PHONE\"\nWHERE IS_PRIMARY = 1" },
  sql_99: { resolvedSql: "SELECT\n    PERSON_ID,\n    EMAIL_TYPE,\n    EMAIL_ADDRESS,\n    IS_PRIMARY\nFROM \"DB2INST1\".\"EMAIL\"\nWHERE IS_PRIMARY = 1" },
  sql_141: { resolvedSql: "SELECT * FROM \"main\".\"policy_coverage\"" },
  sql_95: { resolvedSql: "SELECT\n    policy_id,\n    COUNT(*) AS coverage_count,\n    SUM(premium_amount) AS total_coverage_premium,\n    MAX(CASE WHEN is_optional THEN 1 ELSE 0 END) AS has_optional_coverage\nFROM \"main\".\"policy_coverage\"\nGROUP BY policy_id" },
  sql_93: { resolvedSql: "SELECT\n    policy_id,\n    COUNT(*) AS claim_count,\n    SUM(reserve_amount) AS total_reserved_amount,\n    SUM(paid_amount) AS total_paid_amount,\n    MAX(loss_date) AS latest_loss_date,\n    MAX(CASE WHEN claim_status = 'OPEN' THEN 1 ELSE 0 END) AS has_open_claim\nFROM \"main\".\"policy_claim\"\nGROUP BY policy_id" },
  sql_97: { resolvedSql: "SELECT\n    cp.POLICY_NUMBER AS policy_number,\n    cp.PERSON_ID AS person_id,\n    p.CUSTOMER_NUMBER AS customer_number,\n    p.FIRST_NAME AS first_name,\n    p.LAST_NAME AS last_name,\n    p.BIRTH_DATE AS birth_date,\n    p.OCCUPATION AS occupation,\n    p.ANNUAL_INCOME AS annual_income,\n    p.PREFERRED_LANGUAGE AS preferred_language,\n    a.ADDRESS_LINE1 AS address_line1,\n    a.CITY AS city,\n    a.STATE_CODE AS state_code,\n    a.POSTAL_CODE AS postal_code,\n    ph.PHONE_NUMBER AS phone_number,\n    e.EMAIL_ADDRESS AS email_address,\n    cp.PREFERRED_CONTACT_CHANNEL AS preferred_contact_channel\nFROM \"main\".\"customer_policy_link\" cp\nJOIN \"main\".\"customer_person\" p\n  ON cp.PERSON_ID = p.PERSON_ID\nLEFT JOIN \"main\".\"customer_address\" a\n  ON cp.PERSON_ID = a.PERSON_ID\nLEFT JOIN \"main\".\"customer_phone\" ph\n  ON cp.PERSON_ID = ph.PERSON_ID\nLEFT JOIN \"main\".\"customer_email\" e\n  ON cp.PERSON_ID = e.PERSON_ID" },
  sql_91: { resolvedSql: "SELECT\n    p.policy_id,\n    p.policy_number,\n    p.product_line,\n    p.underwriting_company,\n    p.status AS policy_status,\n    p.effective_date,\n    p.expiration_date,\n    p.written_premium,\n    p.deductible_amount,\n    p.billing_plan,\n    p.agent_code,\n    p.risk_state,\n    c.customer_number,\n    c.first_name,\n    c.last_name,\n    c.birth_date,\n    c.occupation,\n    c.annual_income,\n    c.preferred_language,\n    c.address_line1,\n    c.city,\n    c.state_code AS customer_state,\n    c.postal_code,\n    c.phone_number,\n    c.email_address,\n    c.preferred_contact_channel,\n    COALESCE(cs.coverage_count, 0) AS coverage_count,\n    COALESCE(cs.total_coverage_premium, 0) AS total_coverage_premium,\n    COALESCE(cs.has_optional_coverage, 0) AS has_optional_coverage,\n    COALESCE(cl.claim_count, 0) AS claim_count,\n    COALESCE(cl.total_reserved_amount, 0) AS total_reserved_amount,\n    COALESCE(cl.total_paid_amount, 0) AS total_paid_amount,\n    cl.latest_loss_date,\n    COALESCE(cl.has_open_claim, 0) AS has_open_claim\nFROM \"main\".\"policy_core\" p\nLEFT JOIN \"main\".\"customer_profile\" c\n  ON p.policy_number = c.policy_number\nLEFT JOIN \"main\".\"coverage_summary\" cs\n  ON p.policy_id = cs.policy_id\nLEFT JOIN \"main\".\"claim_summary\" cl\n  ON p.policy_id = cl.policy_id" },
  sql_103: { resolvedSql: "SELECT EXISTS(\n        SELECT 1\n        FROM \"main\".\"policy_customer_360\"\n        WHERE policy_number IS NULL\n           OR customer_number IS NULL\n    )" },
  sql_107: { resolvedSql: "SELECT\n    policy_number,\n    customer_number,\n    first_name,\n    last_name,\n    product_line,\n    policy_status,\n    written_premium,\n    total_coverage_premium,\n    claim_count,\n    total_paid_amount,\n    CASE\n        WHEN has_open_claim = 1 THEN 'HIGH_TOUCH'\n        WHEN claim_count >= 2 THEN 'WATCHLIST'\n        WHEN written_premium >= 2500 THEN 'HIGH_VALUE'\n        ELSE 'STANDARD'\n    END AS servicing_segment\nFROM \"main\".\"policy_customer_360\"" },
  sql_133: { resolvedSql: "SELECT * FROM \"main\".\"policy_customer_360\"" },
  sql_105: { resolvedSql: "SELECT COUNT(*)\n    FROM \"main\".\"policy_servicing_queue\"\n    WHERE servicing_segment = 'HIGH_TOUCH'" },
  sql_121: { resolvedSql: "SELECT\n        q.policy_number,\n        q.customer_number,\n        q.first_name,\n        q.last_name,\n        q.product_line,\n        q.policy_status,\n        q.written_premium,\n        q.total_coverage_premium,\n        q.claim_count,\n        q.total_paid_amount,\n        q.servicing_segment,\n        COALESCE(o.servicing_segment_override, q.servicing_segment) AS final_segment,\n        COALESCE(v.vip_flag, FALSE) AS vip_flag,\n        v.vip_reason,\n        p.queue_owner,\n        p.base_sla_hours,\n        t.target_contact_hours\n    FROM \"main\".\"policy_servicing_queue\" q\n    LEFT JOIN \"main\".\"vip_customers\" v\n      ON q.customer_number = v.customer_number\n    LEFT JOIN \"main\".\"servicing_overrides\" o\n      ON q.policy_number = o.policy_number\n    LEFT JOIN \"main\".\"service_playbook\" p\n      ON q.product_line = p.product_line\n    LEFT JOIN \"main\".\"segment_targets\" t\n      ON COALESCE(o.servicing_segment_override, q.servicing_segment) = t.servicing_segment" },
  sql_126: { resolvedSql: "SELECT *\n    FROM \"main\".\"policy_servicing_enriched\"" },
  sql_131: { resolvedSql: "SELECT *\n    FROM \"main\".\"policy_servicing_enriched\"" },
};
const RUN_TIMING = (() => {
  const started = Object.values(NODE_DETAILS)
    .map((item) => item.startedAt)
    .filter(Boolean)
    .sort()[0];
  const finished = Object.values(NODE_DETAILS)
    .map((item) => item.finishedAt)
    .filter(Boolean)
    .sort()
    .at(-1);
  return {
    startedAt: started || null,
    finishedAt: finished || null,
  };
})();
const NODE_ORDER = Object.keys(NODE_DETAILS);
const PARENT_IDS_BY_NODE = NODE_ORDER.reduce((acc, nodeId) => ({ ...acc, [nodeId]: [] }), {});
const CHILD_IDS_BY_NODE = NODE_ORDER.reduce((acc, nodeId) => ({ ...acc, [nodeId]: [] }), {});
for (const [source, target] of EDGE_LIST) {
  if (!PARENT_IDS_BY_NODE[target]) PARENT_IDS_BY_NODE[target] = [];
  if (!CHILD_IDS_BY_NODE[source]) CHILD_IDS_BY_NODE[source] = [];
  PARENT_IDS_BY_NODE[target].push(source);
  CHILD_IDS_BY_NODE[source].push(target);
}

function humanize(text) {
  if (!text) return "Check";
  return text
    .replace(/^main\./, "")
    .replace(/^exports\//, "")
    .replace(/^"+|"+$/g, "")
    .trim();
}

  function titleForNode(id, details) {
    if (id === "sql_103") return "Boolean Check";
    if (id === "sql_105") return "Count Check";
    if (id === "python_113") return "Service Playbook";
    if (details.artifact) {
      const artifact = String(details.artifact);
      if (artifact.startsWith("main.") || artifact.startsWith("exports/")) {
        return artifact
          .replace(/^main\./, "")
          .replace(/^exports\//, "")
          .replace(/^"+|"+$/g, "")
          .trim();
      }
      const cleaned = artifact.replace(/^"+|"+$/g, "").trim();
      if (cleaned.includes(".")) {
        const lastSegment = cleaned.split(".").slice(-1)[0].replace(/^"+|"+$/g, "").trim();
        if (lastSegment) return lastSegment;
      }
      return humanize(artifact);
    }
    return humanize(id);
  }

function normalizeTone(kind, nodeId) {
  const lowered = String(kind || "").trim().toLowerCase();
  if (nodeId.startsWith("source:") || lowered === "source") return "source";
  if (lowered.includes("file.ingress") || lowered.includes("csv.ingress") || lowered.includes("jsonl.ingress") || lowered.includes("parquet.ingress")) return "file";
  if (lowered.includes("file.egress") || lowered.includes("csv.egress") || lowered.includes("jsonl.egress") || lowered.includes("parquet.egress")) return "fileEgress";
  if (lowered.includes("python")) return "python";
  if (lowered.includes("ingress")) return "ingress";
  if (lowered.includes("check")) return "check";
  if (lowered.includes("egress") || lowered.includes("export")) return "egress";
  if (lowered.includes("model") || lowered.includes("sql")) return "model";
  return "other";
}

function isLocalArtifactKind(kind) {
  const lowered = String(kind || "").trim().toLowerCase();
  return lowered.includes("ingress") || lowered.includes("model");
}

function toneClasses(tone) {
  if (tone === "source") return { border: "border-slate-300", bg: "bg-amber-50/80", badge: "bg-amber-100 text-amber-700", meta: "text-amber-700/80" };
  if (tone === "ingress") return { border: "border-slate-300", bg: "bg-blue-50/80", badge: "bg-blue-100 text-blue-700", meta: "text-blue-700/80" };
  if (tone === "file") return { border: "border-slate-300", bg: "bg-cyan-50/80", badge: "bg-cyan-100 text-cyan-700", meta: "text-cyan-700/80" };
  if (tone === "fileEgress") return { border: "border-slate-300", bg: "bg-teal-50/80", badge: "bg-teal-100 text-teal-700", meta: "text-teal-700/80" };
  if (tone === "python") return { border: "border-slate-300", bg: "bg-green-50/80", badge: "bg-green-100 text-green-700", meta: "text-green-700/80" };
  if (tone === "check") return { border: "border-slate-300", bg: "bg-slate-50", badge: "bg-slate-200 text-slate-700", meta: "text-slate-500" };
  if (tone === "egress") return { border: "border-slate-300", bg: "bg-violet-50/80", badge: "bg-violet-100 text-violet-700", meta: "text-violet-700/80" };
  if (tone === "model") return { border: "border-slate-300", bg: "bg-emerald-50/80", badge: "bg-emerald-100 text-emerald-700", meta: "text-emerald-700/80" };
  return { border: "border-slate-300", bg: "bg-white", badge: "bg-slate-100 text-slate-700", meta: "text-slate-500" };
}

function nodeStatusForDetails(details) {
  const explicitStatus = String(details?.nodeStatus || "").trim().toLowerCase();
  if (explicitStatus) return explicitStatus;
  const warningCount = Array.isArray(details?.warnings) ? details.warnings.length : 0;
  return warningCount > 0 ? "complete_with_warnings" : "complete";
}

function runtimeTone(status, warningCount) {
  const lowered = String(status || "").trim().toLowerCase();
  if (lowered === "running") return "running";
  if (lowered === "failed") return "failed";
  if (lowered === "cleared") return "cleared";
  if (lowered === "skipped") return "skipped";
  if (lowered === "complete_with_warnings" || lowered === "success_with_warnings") return "warning";
  if (lowered === "complete" || lowered === "success") return "success";
  if (lowered === "pending") return "ready";
  return "ready";
}

function runtimeClasses(tone) {
  if (tone === "running") return { box: "border-sky-400 ring-2 ring-sky-200/80", badge: "bg-sky-100 text-sky-700", dot: "bg-sky-500" };
  if (tone === "success") return { box: "border-emerald-400", badge: "bg-emerald-100 text-emerald-700", dot: "bg-emerald-500" };
  if (tone === "warning") return { box: "border-amber-400", badge: "bg-amber-100 text-amber-700", dot: "bg-amber-500" };
  if (tone === "failed") return { box: "border-rose-400 bg-rose-50/85", badge: "bg-rose-100 text-rose-700", dot: "bg-rose-500" };
  if (tone === "cleared") return { box: "border-slate-400 bg-slate-100/80", badge: "bg-slate-200 text-slate-700", dot: "bg-slate-500" };
  if (tone === "skipped") return { box: "border-slate-300 opacity-70", badge: "bg-slate-100 text-slate-500", dot: "bg-slate-400" };
  return { box: "", badge: "bg-slate-100 text-slate-600", dot: "bg-slate-400" };
}

function runtimeLabel(status, warningCount) {
  const lowered = String(status || "").trim().toLowerCase();
  if (lowered === "running") return "RUNNING";
  if (lowered === "failed") return "FAILED";
  if (lowered === "cleared") return "CLEARED";
  if (lowered === "skipped") return "SKIPPED";
  if (lowered === "complete_with_warnings" || lowered === "success_with_warnings") return "WARNINGS";
  if (lowered === "complete" || lowered === "success") return "COMPLETE";
  if (lowered === "pending") return "PENDING";
  return "READY";
}

function edgeStroke(tone) {
  if (tone === "running") return "#38bdf8";
  if (tone === "success") return "#34d399";
  if (tone === "warning") return "#f59e0b";
  if (tone === "failed") return "#fb7185";
  if (tone === "skipped") return "#cbd5e1";
  return "#94a3b8";
}

function formatCount(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString();
}

function buildFlowNodeData(id, details) {
  const warningCount = Array.isArray(details.warnings) ? details.warnings.length : 0;
  const status = nodeStatusForDetails(details);
  return {
    label: titleForNode(id, details),
    title: titleForNode(id, details),
    nodeName: id,
    id,
    kind: details.kind,
    tone: normalizeTone(details.kind, id),
    relationText: details.artifact || null,
    configText: details.kind.includes("egress") ? "delivery" : details.kind.includes("check") ? "validation" : "pipeline",
    runtimeTone: runtimeTone(status, warningCount),
    runtimeLabel: runtimeLabel(status, warningCount),
    runtimeHint: details.rows !== null && details.rows !== undefined ? `${formatCount(details.rows)} rows` : (details.artifact || null),
    rows: details.rows,
    duration: timeLabel(details.startedAt, details.finishedAt),
  };
}

function buildFlowEdge(source, target) {
  const details = NODE_DETAILS[target];
  const warningCount = Array.isArray(details?.warnings) ? details.warnings.length : 0;
  const tone = runtimeTone(nodeStatusForDetails(details), warningCount);
  const stroke = edgeStroke(tone);
  return {
    id: `edge-${source}-${target}`,
    source,
    target,
    type: "step",
    markerEnd: { type: MarkerType.ArrowClosed, color: stroke, width: 18, height: 18 },
    animated: false,
    style: { stroke, strokeWidth: 1.8 },
    pathOptions: { borderRadius: 14, offset: 18 },
    selectable: false,
    focusable: false,
  };
}

function layoutElements(nodes, edges) {
  const graph = new dagre.graphlib.Graph();
  graph.setDefaultEdgeLabel(() => ({}));
  graph.setGraph({ rankdir: "LR", ranksep: 96, nodesep: 44, marginx: 24, marginy: 24 });
  nodes.forEach((node) => graph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT }));
  edges.forEach((edge) => graph.setEdge(edge.source, edge.target));
  dagre.layout(graph);

  const positions = new Map();
  nodes.forEach((node) => {
    const placed = graph.node(node.id);
    positions.set(node.id, { x: placed.x, y: placed.y });
  });

  let minLeft = Number.POSITIVE_INFINITY;
  let minTop = Number.POSITIVE_INFINITY;
  positions.forEach((pos) => {
    minLeft = Math.min(minLeft, pos.x - NODE_WIDTH / 2);
    minTop = Math.min(minTop, pos.y - NODE_HEIGHT / 2);
  });

  const offsetX = minLeft < 56 ? 56 - minLeft : 0;
  const offsetY = minTop < 56 ? 56 - minTop : 0;

  return {
    nodes: nodes.map((node) => {
      const pos = positions.get(node.id) || { x: 0, y: 0 };
      return {
        ...node,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        position: {
          x: pos.x - NODE_WIDTH / 2 + offsetX,
          y: pos.y - NODE_HEIGHT / 2 + offsetY,
        },
      };
    }),
    edges,
  };
}

function timeLabel(startedAt, finishedAt) {
  if (!startedAt || !finishedAt) return "-";
  const ms = new Date(finishedAt).getTime() - new Date(startedAt).getTime();
  if (Number.isNaN(ms)) return "-";
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function createGridColumns(keys) {
  return keys.map((key) => ({
    field: key,
    headerName: key,
    sortable: true,
    filter: true,
    resizable: true,
    flex: key === "artifact" ? 1.4 : 1,
    minWidth: key === "artifact" ? 220 : 140,
    cellClass:
      key === "metric_value" || key === "loaded_at" || key === "row_id" || key === "row_count"
        ? "loom-grid-cell-mono"
        : "loom-grid-cell",
  }));
}

// ── AlaSQL query engine ───────────────────────────────────────────────────────
function clockLabel(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "-";
  return parsed.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
}

function collectRelatedNodeIds(startId, adjacencyMap) {
  const seen = new Set();
  const stack = [startId];
  while (stack.length) {
    const current = stack.pop();
    if (!current || seen.has(current)) continue;
    seen.add(current);
    for (const next of adjacencyMap[current] || []) {
      if (!seen.has(next)) stack.push(next);
    }
  }
  return seen;
}

function buildInspectEntry(nodeId) {
  const details = NODE_DETAILS[nodeId];
  if (!details) return null;
  const warningCount = Array.isArray(details.warnings) ? details.warnings.length : 0;
  const status = nodeStatusForDetails(details);
  const kind = String(details.kind || "").trim().toLowerCase();
  const isLocalArtifact =
    kind.includes("ingress") ||
    kind.includes("model");
  return {
    id: nodeId,
    title: titleForNode(nodeId, details),
    kind: details.kind,
    tone: normalizeTone(details.kind, nodeId),
    displayState: runtimeLabel(status, warningCount),
    currentState: status,
    nodeRunStatus: status,
    logicalArtifact: details.artifact || null,
    artifactName: isLocalArtifact ? details.artifact || null : null,
    rows: details.rows,
    duration: timeLabel(details.startedAt, details.finishedAt),
    startedAt: details.startedAt,
    finishedAt: details.finishedAt,
    dependencies: [...(PARENT_IDS_BY_NODE[nodeId] || [])],
    dependents: [...(CHILD_IDS_BY_NODE[nodeId] || [])],
    resolvedSql: NODE_QUERY_DETAILS[nodeId]?.resolvedSql || null,
  };
}

function buildInspectPanelData(nodeId) {
  const upstreamSet = collectRelatedNodeIds(nodeId, PARENT_IDS_BY_NODE);
  const downstreamSet = collectRelatedNodeIds(nodeId, CHILD_IDS_BY_NODE);
  upstreamSet.delete(nodeId);
  downstreamSet.delete(nodeId);
  const selected = buildInspectEntry(nodeId);
  const nodeHistory = selected
    ? [
        {
          state: "READY",
          trigger: "run_initialized",
          timestamp: RUN_TIMING.startedAt,
        },
        {
          state: "RUNNING",
          trigger: "node_started",
          timestamp: selected.startedAt,
        },
        {
          state: selected.displayState,
          trigger:
            selected.currentState === "failed"
              ? "node_failed"
              : selected.currentState === "skipped"
                ? "node_skipped"
                : "node_completed",
          timestamp: selected.finishedAt,
        },
      ]
    : [];
  return {
    run: {
      ...RUN_SNAPSHOT,
      ...RUN_TIMING,
      duration: timeLabel(RUN_TIMING.startedAt, RUN_TIMING.finishedAt),
    },
    selected,
    upstream: NODE_ORDER.filter((currentId) => upstreamSet.has(currentId))
      .map((currentId) => buildInspectEntry(currentId))
      .filter(Boolean),
    downstream: NODE_ORDER.filter((currentId) => downstreamSet.has(currentId))
      .map((currentId) => buildInspectEntry(currentId))
      .filter(Boolean),
    history: nodeHistory,
  };
}

function requiresCleanExistingPrompt(payload) {
  const message = String(payload?.error || "").toLowerCase();
  if (payload?.requires_clean_existing) return true;
  if (!message.includes("already exists")) return false;
  return (
    message.includes("table with name") ||
    message.includes("schema with name") ||
    message.includes("run will purge")
  );
}

function buildFlowNodeDataFromApi(node) {
  const id = String(node?.name || "");
  const kind = String(node?.kind || "");
  const status = String(node?.node_run_status || node?.current_state || "ready");
  const artifactName = node?.artifact_name || null;
  const rows = node?.row_count_out ?? null;
  const runtimeVarNames = Array.isArray(node?.runtime_var_names) ? node.runtime_var_names : [];
  return {
    label: titleForNode(id, { artifact: artifactName, kind }),
    title: titleForNode(id, { artifact: artifactName, kind }),
    nodeName: id,
    id,
    kind,
    tone: normalizeTone(kind, id),
    relationText: artifactName,
    configText: kind.includes("egress") ? "delivery" : kind.includes("check") ? "validation" : "pipeline",
    runtimeTone: runtimeTone(status, 0),
    runtimeLabel: runtimeLabel(status, 0),
    runtimeHint: artifactName || null,
    hasRuntimeVars: Boolean(node?.has_runtime_vars || runtimeVarNames.length),
    rows,
    duration: timeLabel(node?.started_at, node?.finished_at),
  };
}

function buildFlowEdgeFromApi(source, target, nodeById) {
  const details = nodeById.get(target);
  const status = String(details?.node_run_status || details?.current_state || "ready");
  const stroke = edgeStroke(runtimeTone(status, 0));
  return {
    id: `edge-${source}-${target}`,
    source,
    target,
    type: "step",
    markerEnd: { type: MarkerType.ArrowClosed, color: stroke, width: 18, height: 18 },
    animated: false,
    style: { stroke, strokeWidth: 1.8 },
    pathOptions: { borderRadius: 14, offset: 18 },
    selectable: false,
    focusable: false,
  };
}

function buildInspectEntryFromApi(node, query = null) {
  if (!node) return null;
  const id = String(node.name || "");
  const kind = String(node.kind || "");
  const status = String(node.node_run_status || node.current_state || "ready");
  const titleArtifact = node.logical_artifact || node.artifact_name || null;
  const artifactName = node.artifact_name || null;
  const runtimeVarNames = Array.isArray(node.runtime_var_names) ? node.runtime_var_names : [];
  return {
    static: {
      id,
      title: titleForNode(id, { artifact: titleArtifact, kind }),
      kind,
      tone: normalizeTone(kind, id),
      logicalArtifact: node.logical_artifact || null,
      dependencies: Array.isArray(node.dependencies) ? node.dependencies : [],
      dependents: Array.isArray(node.dependents) ? node.dependents : [],
      rawSql: query?.sql || null,
      resolvedSql: query?.resolved_sql || null,
      hasRuntimeVars: Boolean(node.has_runtime_vars || runtimeVarNames.length),
      runtimeVarNames,
      checkOperator: node.operator || null,
      checkValue: node.value ?? null,
      columnMappings: Array.isArray(node.column_mappings) ? node.column_mappings : [],
    },
    live: {
      displayState: runtimeLabel(status, 0),
      currentState: status,
      nodeRunStatus: status,
      artifactName,
      rows: node.row_count_out ?? null,
      rowCountIn: node.row_count_in ?? null,
      rowCountOut: node.row_count_out ?? null,
      duration: timeLabel(node.started_at, node.finished_at),
      startedAt: node.started_at || null,
      finishedAt: node.finished_at || null,
    },
  };
}

function buildInspectPanelDataFromApi(payload) {
  const selected = buildInspectEntryFromApi(payload?.selected, payload?.query || null);
  return {
    run: {
      runId: payload?.run_id || null,
      runLabel: payload?.run_label || null,
      runStatus: payload?.run_status || null,
      startedAt: selected?.live?.startedAt || null,
      finishedAt: selected?.live?.finishedAt || null,
      duration: timeLabel(selected?.live?.startedAt, selected?.live?.finishedAt),
    },
    selectedStatic: selected?.static || null,
    selectedLive: selected?.live || null,
    upstream: null,
    downstream: null,
    historyInitial: null,
    historyLive: [],
    logsInitial: null,
    logsLive: [],
  };
}

function buildInspectItemsFromApi(items, selectedId) {
  return Array.isArray(items)
    ? items
        .map((item) => buildInspectEntryFromApi(item))
        .filter(Boolean)
        .map((item) => ({ ...item.static, ...item.live }))
        .filter((item) => item.id !== selectedId)
    : [];
}

function buildInspectHistoryFromApi(payload) {
  return Array.isArray(payload?.history?.states)
    ? payload.history.states.map((item) => ({
        state: String(item.state || "").toUpperCase() || "UNKNOWN",
        trigger: String(item.trigger || "state_change"),
        timestamp: item.created_at || null,
      }))
    : [];
}

function patchGraphDataWithEvent(current, event) {
  if (!current || !event || event.type !== "runtime_log") return current;
  const code = String(event.code || "").trim().toLowerCase();
  const eventNodeName = String(event.node_name || event.node_id || "").trim();
  const eventTimestamp = event.timestamp || null;
  const eventArtifactName = event.artifact_name || null;
  const details = event.details || {};
  const successCodes = new Set([
    "node_rows_written",
    "node_artifact_created",
    "node_egress_written",
    "node_export_written",
    "node_check_passed",
  ]);

  let next = current;

  if (code === "pipeline_run_started" || code === "pipeline_execution_started") {
    next = {
      ...next,
      run_id: event.run_id || next.run_id,
      run_status: "running",
      is_final: false,
      nodes: (Array.isArray(next.nodes) ? next.nodes : []).map((node) => ({
        ...node,
        current_state: "ready",
        node_run_status: "ready",
        started_at: null,
        finished_at: null,
        row_count_in: null,
        row_count_out: null,
      })),
    };
  } else if (code === "pipeline_execution_failed") {
    next = {
      ...next,
      run_id: event.run_id || next.run_id,
      run_status: "failed",
      is_final: false,
    };
  } else if (code === "pipeline_execution_finished") {
    next = {
      ...next,
      run_id: event.run_id || next.run_id,
      run_status: next.run_status === "success_with_warnings" ? "success_with_warnings" : "success",
      is_final: false,
    };
  }

  if (!eventNodeName) return next;

  const patchedNodes = (Array.isArray(next.nodes) ? next.nodes : []).map((node) => {
    if (String(node.name || "") !== eventNodeName) return node;
    const patched = { ...node };
    if (code === "node_execution_started") {
      patched.current_state = "running";
      patched.node_run_status = "running";
      patched.started_at = patched.started_at || eventTimestamp;
      patched.finished_at = null;
    } else if (code === "node_execution_failed") {
      patched.current_state = "failed";
      patched.node_run_status = "failed";
      patched.finished_at = eventTimestamp;
    } else if (code === "node_skipped") {
      patched.current_state = "skipped";
      patched.node_run_status = "skipped";
      patched.finished_at = eventTimestamp;
    } else if (code === "node_warning") {
      patched.current_state = "complete_with_warnings";
      patched.node_run_status = "complete_with_warnings";
    } else if (code === "node_rows_extracted") {
      if (details.row_count !== undefined) patched.row_count_out = details.row_count;
    } else if (successCodes.has(code)) {
      const alreadyWarning =
        String(patched.current_state || "").trim().toLowerCase() === "complete_with_warnings" ||
        String(patched.node_run_status || "").trim().toLowerCase() === "complete_with_warnings";
      patched.current_state = alreadyWarning ? "complete_with_warnings" : "complete";
      patched.node_run_status = alreadyWarning ? "complete_with_warnings" : "complete";
      patched.finished_at = eventTimestamp;
      patched.artifact_name = eventArtifactName || patched.artifact_name;
      if (details.row_count_out !== undefined) patched.row_count_out = details.row_count_out;
      if (details.row_count !== undefined) patched.row_count_out = details.row_count;
    }
    return patched;
  });

  return {
    ...next,
    nodes: patchedNodes,
  };
}

function patchPanelDataWithEvent(current, event, selectedNodeId) {
  if (!current || !current.selectedStatic || !current.selectedLive || !event || event.type !== "runtime_log") return current;
  const eventNodeName = String(event.node_name || event.node_id || "").trim();
  const logEvent =
    eventNodeName && eventNodeName === selectedNodeId
      ? {
          timestamp: event.timestamp || null,
          code: event.code || null,
          severity: event.severity || null,
          message: event.message || null,
        }
      : null;
  const nextLogsLive = logEvent
    ? [logEvent, ...(Array.isArray(current.logsLive) ? current.logsLive : [])]
    : current.logsLive;
  if (!eventNodeName || eventNodeName !== selectedNodeId) {
    if (String(event.code || "").trim().toLowerCase() === "pipeline_execution_started") {
      return {
        ...current,
        run: {
          ...current.run,
          runId: event.run_id || current.run?.runId || null,
          runStatus: "running",
        },
        selectedLive: {
          ...current.selectedLive,
          currentState: "ready",
          nodeRunStatus: "ready",
          displayState: runtimeLabel("ready", 0),
          artifactName: null,
          startedAt: null,
          finishedAt: null,
          duration: "-",
          rowCountIn: null,
          rowCountOut: null,
          rows: null,
        },
        historyInitial: [],
        historyLive: [],
        logsInitial: [],
        logsLive: nextLogsLive || [],
      };
    }
    if (String(event.code || "").trim().toLowerCase() === "pipeline_execution_failed") {
      return {
        ...current,
        run: {
          ...current.run,
          runId: event.run_id || current.run?.runId || null,
          runStatus: "failed",
        },
        logsLive: nextLogsLive,
      };
    }
    return current;
  }

  const code = String(event.code || "").trim().toLowerCase();
  const details = event.details || {};
  const eventTimestamp = event.timestamp || null;
  const eventArtifactName = event.artifact_name || null;
  const nextSelected = { ...current.selectedLive };
  const nextRun = { ...current.run, runId: event.run_id || current.run?.runId || null };
  let historyState = null;

  if (code === "node_execution_started") {
    nextSelected.currentState = "running";
    nextSelected.nodeRunStatus = "running";
    nextSelected.displayState = runtimeLabel("running", 0);
    nextSelected.startedAt = nextSelected.startedAt || eventTimestamp;
    nextSelected.finishedAt = null;
    historyState = "RUNNING";
  } else if (code === "node_execution_failed") {
    nextSelected.currentState = "failed";
    nextSelected.nodeRunStatus = "failed";
    nextSelected.displayState = runtimeLabel("failed", 0);
    nextSelected.finishedAt = eventTimestamp;
    historyState = "FAILED";
  } else if (code === "node_skipped") {
    nextSelected.currentState = "skipped";
    nextSelected.nodeRunStatus = "skipped";
    nextSelected.displayState = runtimeLabel("skipped", 0);
    nextSelected.finishedAt = eventTimestamp;
    historyState = "SKIPPED";
  } else if (code === "node_warning") {
    nextSelected.currentState = "complete_with_warnings";
    nextSelected.nodeRunStatus = "complete_with_warnings";
    nextSelected.displayState = runtimeLabel("complete_with_warnings", 1);
  } else if (code === "node_rows_extracted") {
    if (details.row_count !== undefined) nextSelected.rowCountOut = details.row_count;
    if (details.row_count !== undefined) nextSelected.rows = details.row_count;
  } else if (["node_rows_written", "node_artifact_created", "node_egress_written", "node_export_written", "node_check_passed"].includes(code)) {
    const alreadyWarning =
      String(nextSelected.currentState || "").trim().toLowerCase() === "complete_with_warnings" ||
      String(nextSelected.nodeRunStatus || "").trim().toLowerCase() === "complete_with_warnings";
    nextSelected.currentState = alreadyWarning ? "complete_with_warnings" : "complete";
    nextSelected.nodeRunStatus = alreadyWarning ? "complete_with_warnings" : "complete";
    nextSelected.displayState = runtimeLabel(alreadyWarning ? "complete_with_warnings" : "complete", alreadyWarning ? 1 : 0);
    nextSelected.finishedAt = eventTimestamp;
    nextSelected.artifactName = eventArtifactName || nextSelected.artifactName;
    if (details.row_count_out !== undefined) nextSelected.rowCountOut = details.row_count_out;
    if (details.row_count !== undefined) nextSelected.rowCountOut = details.row_count;
    if (details.row_count !== undefined && nextSelected.rows == null) nextSelected.rows = details.row_count;
    historyState = "COMPLETE";
  }

  nextSelected.duration = timeLabel(nextSelected.startedAt, nextSelected.finishedAt);
  nextRun.startedAt = current.run?.startedAt || nextSelected.startedAt || null;
  nextRun.finishedAt = nextSelected.finishedAt || current.run?.finishedAt || null;
  nextRun.duration = timeLabel(nextRun.startedAt, nextRun.finishedAt);

  const nextHistoryLive = historyState
    ? [{ state: historyState, trigger: code, timestamp: eventTimestamp }, ...(Array.isArray(current.historyLive) ? current.historyLive : [])]
    : current.historyLive;

  return {
    ...current,
    run: nextRun,
    selectedLive: nextSelected,
    historyLive: nextHistoryLive,
    logsLive: nextLogsLive,
  };
}

function FlowCardNode({ data, selected }) {
  const toneStyles = {
    source: { accent: "#d3b165", tint: "#fdf5df", icon: "#7b5d18", badgeBg: "#f8e7b8", badgeText: "#7b5d18" },
    ingress: { accent: "#5eaef5", tint: "#eaf5ff", icon: "#165387", badgeBg: "#dbeeff", badgeText: "#165387" },
    file: { accent: "#59c4d8", tint: "#e7fbff", icon: "#116575", badgeBg: "#d7f6fb", badgeText: "#116575" },
    fileEgress: { accent: "#2dd4bf", tint: "#ecfdf8", icon: "#0f766e", badgeBg: "#ccfbf1", badgeText: "#0f766e" },
    model: { accent: "#efb247", tint: "#fff6e7", icon: "#7c5710", badgeBg: "#fdebc8", badgeText: "#7c5710" },
    check: { accent: "#f39e8d", tint: "#fff0ec", icon: "#8a3e31", badgeBg: "#ffe0d9", badgeText: "#8a3e31" },
    python: { accent: "#b79df6", tint: "#f3edff", icon: "#5c45a5", badgeBg: "#e6ddff", badgeText: "#5c45a5" },
    egress: { accent: "#9ecbad", tint: "#eef8f1", icon: "#44725a", badgeBg: "#dff0e5", badgeText: "#44725a" },
    other: { accent: "#b8bdc7", tint: "#f2f4f7", icon: "#58606f", badgeBg: "#e7ebf0", badgeText: "#58606f" },
  }[data.tone] || { accent: "#b8bdc7", tint: "#f2f4f7", icon: "#58606f", badgeBg: "#e7ebf0", badgeText: "#58606f" };
  const stateStyles = {
    running: { bg: "#dff4ff", text: "#0b6ea8" },
    success: { bg: "#dff4e6", text: "#1e7b45" },
    warning: { bg: "#fdebc8", text: "#9a5b00" },
    failed: { bg: "#ffe1df", text: "#a73c34" },
    cleared: { bg: "#eceef2", text: "#5f6673" },
    skipped: { bg: "#eceef2", text: "#6c7380" },
    ready: { bg: "#eceef2", text: "#5f6673" },
  }[data.runtimeTone] || { bg: "#eceef2", text: "#5f6673" };
  const isDecisionNode = data.tone === "check";

  if (isDecisionNode) {
    const decisionShape = "polygon(9% 0%, 91% 0%, 100% 50%, 91% 100%, 9% 100%, 0% 50%)";
    const decisionPalette = selected
      ? { border: "#bcb6b0", fill: "#ffffff", shadow: "drop-shadow(0 10px 30px rgba(15,23,42,0.10))" }
      : { border: "#dddfe3", fill: "#ffffff", shadow: "none" };

    return (
      <div
        style={{
          width: NODE_WIDTH,
          height: NODE_HEIGHT,
          position: "relative",
          overflow: "visible",
        }}
      >
        <Handle
          type="target"
          position={Position.Left}
          style={{
            width: 8,
            height: 8,
            left: -4,
            top: "50%",
            background: "#ffffff",
            border: "1.5px solid #a5a5a5",
          }}
        />

        <div
          style={{
            position: "absolute",
            inset: 0,
            background: decisionPalette.border,
            clipPath: decisionShape,
            filter: decisionPalette.shadow,
          }}
        />

        <div
          style={{
            position: "absolute",
            inset: 2,
            background: decisionPalette.fill,
            clipPath: decisionShape,
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            textAlign: "center",
            padding: "16px 30px",
          }}
        >
          <div
            style={{
              fontSize: 13,
              fontWeight: 700,
              color: "#2d3037",
              lineHeight: 1.2,
              marginBottom: 6,
              maxWidth: "100%",
              overflowWrap: "anywhere",
            }}
          >
            {data.title}
          </div>
          <div
            style={{
              fontSize: 10,
              color: "#858890",
              letterSpacing: 0.3,
              textTransform: "uppercase",
              marginBottom: 8,
              maxWidth: "100%",
              overflowWrap: "anywhere",
            }}
          >
            {data.id}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
            <span
              style={{
                borderRadius: 999,
                background: stateStyles.bg,
                color: stateStyles.text,
                padding: "4px 8px",
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                lineHeight: 1,
              }}
            >
              {data.runtimeLabel}
            </span>
            {data.hasRuntimeVars ? (
              <span
                style={{
                  borderRadius: 999,
                  background: "#fdebc8",
                  color: "#9a5b00",
                  padding: "4px 8px",
                  fontSize: 9,
                  fontWeight: 700,
                  letterSpacing: "0.08em",
                  textTransform: "uppercase",
                  lineHeight: 1,
                }}
              >
                Vars
              </span>
            ) : null}
          </div>
          <div
            style={{
              fontSize: 11,
              fontWeight: 600,
              color: "#4c5057",
              lineHeight: 1.2,
            }}
          >
            {data.duration}
          </div>
        </div>

        <Handle
          type="source"
          position={Position.Right}
          style={{
            width: 8,
            height: 8,
            right: -4,
            top: "50%",
            background: "#ffffff",
            border: "1.5px solid #a5a5a5",
          }}
        />
      </div>
    );
  }

  return (
    <div
      style={{
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
        background: "#ffffff",
        border: selected ? "1px solid #bcb6b0" : "1px solid #dddfe3",
        borderRadius: 6,
        boxShadow: selected ? "0 10px 30px rgba(0,0,0,0.08)" : "0 1px 1px rgba(0,0,0,0.03)",
        position: "relative",
        overflow: "visible",
      }}
    >
      <div
        style={{
          position: "absolute",
          left: 0,
          top: 0,
          bottom: 0,
          width: 5,
          borderRadius: "6px 0 0 6px",
          background: toneStyles.accent,
        }}
      />

      <Handle
        type="target"
        position={Position.Left}
        style={{
          width: 8,
          height: 8,
          left: -4,
          background: "#ffffff",
          border: "1.5px solid #a5a5a5",
        }}
      />

      <div className="flex items-start justify-between gap-2 px-3 pt-3 pb-2">
        <div className="min-w-0 flex-1 leading-tight">
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 2 }}>
            <div
              style={{
                minWidth: 0,
                flex: 1,
                overflow: "hidden",
                whiteSpace: "nowrap",
                textOverflow: "ellipsis",
                paddingRight: 4,
                fontSize: 12,
                fontWeight: 700,
                color: "#2d3037",
                lineHeight: 1.2,
              }}
            >
              {data.title}
            </div>
            {data.hasRuntimeVars ? (
              <span
                style={{
                  flexShrink: 0,
                  borderRadius: 999,
                  background: "#fdebc8",
                  color: "#9a5b00",
                  padding: "4px 8px",
                  fontSize: 9,
                  fontWeight: 700,
                  letterSpacing: "0.08em",
                  textTransform: "uppercase",
                  lineHeight: 1,
                }}
              >
                Vars
              </span>
            ) : null}
          </div>
          <div
            style={{
              fontSize: 10,
              color: "#858890",
              letterSpacing: 0.3,
              textTransform: "uppercase",
              marginBottom: 3,
            }}
          >
            {data.id}
          </div>
          <div
            style={{
              fontSize: 11,
              color: "#4c5057",
              lineHeight: 1.25,
            }}
          >
            {data.kind}
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "flex-start", gap: 6, flexShrink: 0 }}>
          <span
            style={{
              alignSelf: "flex-start",
              borderRadius: 999,
              background: stateStyles.bg,
              color: stateStyles.text,
              padding: "4px 8px",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              lineHeight: 1,
            }}
          >
            {data.runtimeLabel}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 px-3 pb-3">
        <div className="rounded-[4px] border border-[#ececef] bg-[#fbfbfc] px-2 py-2">
          <div className="mb-1 text-[9px] uppercase tracking-[0.08em] text-[#9a9ca4]">Rows</div>
          <div className="text-[11px] font-semibold text-[#2b2e34]">
            {data.rows === null || data.rows === undefined ? "-" : data.rows}
          </div>
        </div>
        <div className="rounded-[4px] border border-[#ececef] bg-[#fbfbfc] px-2 py-2">
          <div className="mb-1 text-[9px] uppercase tracking-[0.08em] text-[#9a9ca4]">Time</div>
          <div className="text-[11px] font-semibold text-[#2b2e34]">{data.duration}</div>
        </div>
      </div>

      <Handle
        type="source"
        position={Position.Right}
        style={{
          width: 8,
          height: 8,
          right: -4,
          background: "transparent",
          border: "none",
          opacity: 0,
        }}
      />
    </div>
  );
}

const nodeTypes = { flowCard: FlowCardNode };

function statusBadgeStyle(status) {
  const tone = runtimeTone(status, 0);
  return {
    running: { bg: "#dff4ff", text: "#0b6ea8" },
    success: { bg: "#dff4e6", text: "#1e7b45" },
    warning: { bg: "#fdebc8", text: "#9a5b00" },
    failed: { bg: "#ffe1df", text: "#a73c34" },
    cleared: { bg: "#eceef2", text: "#5f6673" },
    skipped: { bg: "#eceef2", text: "#6c7380" },
    ready: { bg: "#eceef2", text: "#5f6673" },
  }[tone] || { bg: "#eceef2", text: "#5f6673" };
}

function kindDotStyle(tone) {
  return {
    source: { bg: "#d3b165" },
    ingress: { bg: "#3b82f6" },
    file: { bg: "#06b6d4" },
    fileEgress: { bg: "#2dd4bf" },
    model: { bg: "#f59e0b" },
    check: { bg: "#8b5cf6" },
    python: { bg: "#a78bfa" },
    egress: { bg: "#10b981" },
    other: { bg: "#94a3b8" },
  }[tone] || { bg: "#94a3b8" };
}

function HistoryTable({ items }) {
  return (
    <div className="h-full min-h-0 overflow-y-scroll pr-2" style={{ scrollbarGutter: "stable" }}>
      <table className="w-full table-fixed border-collapse">
        <colgroup>
          <col style={{ width: "26%" }} />
          <col style={{ width: "42%" }} />
          <col style={{ width: "32%" }} />
        </colgroup>
        <thead>
          <tr className="border-b border-slate-200/90">
            <th className="sticky top-0 z-10 bg-white px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">State</th>
            <th className="sticky top-0 z-10 bg-white px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">Trigger</th>
            <th className="sticky top-0 z-10 bg-white px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">Time</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item, index) => {
            const badgeStyle = statusBadgeStyle(String(item.state || "").toLowerCase());
            return (
              <tr key={`history-row-${index}`} className="border-t border-slate-100">
                <td className="px-4 py-2 align-middle text-left">
                  <span
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      borderRadius: 4,
                      background: badgeStyle.bg,
                      color: badgeStyle.text,
                      padding: "3px 7px",
                      fontSize: 10,
                      fontWeight: 800,
                      letterSpacing: "0.08em",
                      textTransform: "uppercase",
                      lineHeight: 1,
                    }}
                  >
                    {item.state}
                  </span>
                </td>
                <td className="px-4 py-2 align-middle text-left text-[12px] text-slate-600">{item.trigger}</td>
                <td className="px-4 py-2 align-middle text-left text-[12px] font-semibold text-slate-700">{clockLabel(item.timestamp)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function FlatNodeTable({ items, selectedId }) {
  const rows = Array.isArray(items) ? items : [];
  return (
    <div className="h-full min-h-0 overflow-y-scroll pr-2" style={{ scrollbarGutter: "stable" }}>
      <table className="w-full table-fixed border-collapse">
        <colgroup>
          <col style={{ width: "42%" }} />
          <col style={{ width: "16%" }} />
          <col style={{ width: "24%" }} />
          <col style={{ width: "18%" }} />
        </colgroup>
        <thead>
          <tr className="border-b border-slate-200/90">
            <th className="sticky top-0 z-10 bg-white px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">Node</th>
            <th className="sticky top-0 z-10 bg-white px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">Status</th>
            <th className="sticky top-0 z-10 bg-white px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">Kind</th>
            <th className="sticky top-0 z-10 bg-white px-4 py-2 text-right text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-400">Time</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((item) => {
            const badgeStyle = statusBadgeStyle(item.currentState);
            const dotStyle = kindDotStyle(item.tone);
            const active = item.id === selectedId;
            return (
              <tr key={`node-row-${item.id}`} className={`border-t border-slate-100 ${active ? "bg-slate-50/70" : ""}`}>
                <td className="px-4 py-2 align-middle">
                  <div className="min-w-0 text-left">
                    <div className="truncate text-[13px] font-semibold text-slate-950">{item.title}</div>
                    <div className="mt-0.5 text-[10px] uppercase tracking-[0.12em] text-slate-400">{item.id}</div>
                  </div>
                </td>
                <td className="px-4 py-2 align-middle text-left">
                  <span
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      borderRadius: 4,
                      background: badgeStyle.bg,
                      color: badgeStyle.text,
                      padding: "3px 7px",
                      fontSize: 10,
                      fontWeight: 800,
                      letterSpacing: "0.08em",
                      textTransform: "uppercase",
                      lineHeight: 1,
                    }}
                  >
                    {item.displayState}
                  </span>
                </td>
                <td className="px-4 py-2 align-middle text-left text-slate-600">
                  <div className="flex items-center gap-1.5 text-[12px]">
                    <span
                      style={{
                        width: 6,
                        height: 6,
                        borderRadius: 999,
                        background: dotStyle.bg,
                        flexShrink: 0,
                      }}
                    />
                    <span className="truncate">{item.kind}</span>
                  </div>
                </td>
                <td className="px-4 py-2 align-middle text-right text-[12px] font-semibold text-slate-700">{item.duration}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function highlightSqlVars(sqlText) {
  const text = String(sqlText || "");
  if (!text) return null;
  const pattern = /(\{\{\s*queron\.var\(\s*["'][^"']+["'](?:\s*,\s*.*?)?\s*\)\s*\}\})/gis;
  const parts = text.split(pattern);
  return parts.map((part, index) => {
    if (!part) return null;
    if (pattern.test(part)) {
      pattern.lastIndex = 0;
      return (
        <span
          key={`var-${index}`}
          className="rounded bg-amber-100 px-1 py-0.5 font-semibold text-amber-800"
        >
          {part}
        </span>
      );
    }
    pattern.lastIndex = 0;
    return <Fragment key={`txt-${index}`}>{part}</Fragment>;
  });
}

function QueryPanel({ rawSql, resolvedSql, hasRuntimeVars, kind, checkOperator, checkValue }) {
  const [copied, setCopied] = useState(false);
  const primarySql = hasRuntimeVars && rawSql ? rawSql : resolvedSql;
  const showCountCondition = String(kind || "").trim() === "check.count" && checkOperator && checkValue !== null && checkValue !== undefined;

  async function handleCopy() {
    if (!primarySql) return;
    try {
      await navigator.clipboard.writeText(primarySql);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch (_error) {
      setCopied(false);
    }
  }

  if (!primarySql && !resolvedSql) {
    return (
      <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-slate-400">
        No resolved SQL is available for this node.
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden">
      <div className="relative min-h-0 flex-1 overflow-auto">
        <button
          type="button"
          onClick={handleCopy}
          className="absolute right-4 top-4 z-10 rounded-md border border-slate-200 bg-white px-3 py-1 text-[11px] font-semibold text-slate-600 shadow-sm transition hover:bg-slate-50 hover:text-slate-900"
        >
          {copied ? "Copied" : "Copy"}
        </button>
        {showCountCondition ? (
          <div className="border-b border-slate-200 px-4 py-3 text-[12px] text-slate-600">
            <span className="font-semibold text-slate-700">Threshold:</span>{" "}
            <span className="font-mono">
              count {String(checkOperator)} {String(checkValue)}
            </span>
          </div>
        ) : null}
        <pre className="min-h-full whitespace-pre-wrap break-words px-4 py-4 font-mono text-[12px] leading-5 text-slate-700">
          {hasRuntimeVars && rawSql ? highlightSqlVars(rawSql) : resolvedSql}
        </pre>
      </div>
    </div>
  );
}

function ArtifactPreviewPanel({ preview, loading = false, error = "", onOpenArtifact, blocked = false, blockedMessage = "" }) {
  if (loading) {
    return (
      <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-slate-400">
        Loading artifact preview...
      </div>
    );
  }

  if (blocked) {
    return (
      <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-slate-400">
        {blockedMessage || "Data preview is unavailable while the pipeline is running."}
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-rose-600">
        {error}
      </div>
    );
  }

  if (!preview || !Array.isArray(preview.rows) || !preview.artifactName) {
    return (
      <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-slate-400">
        No local artifact preview is available for this node.
      </div>
    );
  }

  const rowData = preview.rows;
  const columnDefs = createGridColumns(Array.isArray(preview.columns) ? preview.columns : (rowData.length ? Object.keys(rowData[0]) : []));

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden">
      <div className="ag-theme-quartz loom-grid min-h-0 flex-1 overflow-hidden">
        <AgGridReact
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={{
            sortable: true,
            filter: true,
            resizable: true,
            minWidth: 120,
          }}
          headerHeight={26}
          rowHeight={26}
          domLayout="normal"
        />
      </div>
    </div>
  );
}

function InspectBottomSheet({
  open,
  data,
  onClose,
  activeTab,
  onTabChange,
  onOpenArtifact,
  artifactPreview,
  artifactPreviewLoading = false,
  artifactPreviewError = "",
  loading = false,
  error = "",
  tabLoading = "",
  tabError = "",
  suppressLoadingOverlay = false,
}) {
  if (!open) return null;
  if (loading && !suppressLoadingOverlay) {
    return (
      <div className="pointer-events-none absolute inset-x-0 bottom-0 z-20 border-t border-slate-200 bg-slate-50/95">
        <div className="pointer-events-auto flex h-[160px] w-full items-center justify-center px-6 text-[13px] text-slate-500">
          Loading node details...
        </div>
      </div>
    );
  }
  if (error && !suppressLoadingOverlay) {
    return (
      <div className="pointer-events-none absolute inset-x-0 bottom-0 z-20 border-t border-slate-200 bg-slate-50/95">
        <div className="pointer-events-auto flex h-[160px] w-full items-center justify-center px-6 text-[13px] text-rose-600">
          {error}
        </div>
      </div>
    );
  }
  if (!data || !data.selectedStatic || !data.selectedLive) return null;
  const { upstream, downstream, run } = data;
  const history = [
    ...(Array.isArray(data.historyLive) ? data.historyLive : []),
    ...(Array.isArray(data.historyInitial) ? data.historyInitial : []),
  ];
  const logs = [
    ...(Array.isArray(data.logsLive) ? data.logsLive : []),
    ...(Array.isArray(data.logsInitial) ? data.logsInitial : []),
  ];
  const selected = { ...data.selectedStatic, ...data.selectedLive };
  const tabItems = activeTab === "downstream" ? (downstream || []) : (upstream || []);
  const selectedTone = kindDotStyle(selected.tone);
  const selectedBadge = statusBadgeStyle(selected.currentState);
  const nodeRunStatus = String(selected.nodeRunStatus || selected.currentState || "").trim().toLowerCase();
  const dataTabBlocked = !["complete", "complete_with_warnings"].includes(nodeRunStatus);

  return (
    <div
      className={`pointer-events-none absolute inset-x-0 bottom-0 z-20 transform-gpu border-t border-slate-200 bg-slate-50/95 transition-[transform,opacity] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] ${
        open ? "translate-y-0 opacity-100" : "translate-y-[108%] opacity-0"
      }`}
    >

      <button
        type="button"
        onClick={onClose}
        aria-label="Hide details"
        className="pointer-events-auto absolute right-6 top-0 z-30 -translate-y-full flex items-center gap-1.5 rounded-t-md border border-b-0 border-slate-200 bg-white px-3 py-1.5 text-[12px] font-medium text-slate-500 shadow-sm transition hover:bg-slate-50 hover:text-slate-800"
      >
        <X size={12} strokeWidth={2.5} />
        <span>Close</span>
      </button>

      <div className="pointer-events-auto relative flex h-[245px] w-full flex-col overflow-hidden bg-white md:h-[245px]">
        <div className="grid h-full min-h-0 overflow-hidden lg:grid-cols-[320px_minmax(0,1fr)]">
          <aside
            className="relative flex min-h-0 flex-col overflow-hidden border-r border-slate-200 bg-white"
          >
            <div
              aria-hidden="true"
              className="absolute inset-y-0 left-0 w-[6px]"
              style={{ backgroundColor: selectedTone.bg }}
            />
            <div className="flex items-start justify-between px-4 py-3">
              <div className="min-w-0">
                <div className="truncate text-[14px] font-bold tracking-[-0.01em] text-slate-950">{selected.title}</div>
                <div className="mt-0.5 flex min-w-0 items-center gap-2 text-[11px] text-slate-500">
                  <span className="shrink-0">{selected.id}</span>
                  {(selected.artifactName || selected.logicalArtifact) ? (
                    <>
                      <span className="text-slate-300">•</span>
                      <span className="truncate">
                        {String(selected.artifactName || selected.logicalArtifact || "")
                          .replace(/^"+|"+$/g, "")
                          .split(".")
                          .slice(-1)[0]}
                      </span>
                    </>
                  ) : null}
                  {selected.hasRuntimeVars ? (
                    <>
                      <span className="text-slate-300">•</span>
                      <span className="rounded-full bg-amber-100 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-[0.08em] text-amber-700">
                        Vars
                      </span>
                    </>
                  ) : null}
                </div>
              </div>
              <span
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  borderRadius: 6,
                  background: selectedBadge.bg,
                  color: selectedBadge.text,
                  padding: "4px 10px",
                  fontSize: 11,
                  fontWeight: 800,
                  letterSpacing: "0.08em",
                  textTransform: "uppercase",
                  lineHeight: 1,
                }}
              >
                {selected.displayState}
              </span>
            </div>

            <div className="min-h-0 overflow-y-auto px-4 pb-3">
              <div className="overflow-hidden border-t border-slate-200">
                <div className="border-b border-slate-200 px-3 py-2">
                  <div>
                    <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-400">Run time</div>
                    <div className="mt-0.5 text-[12px] font-semibold text-slate-950">{run.duration}</div>
                  </div>
                </div>
                <div className="grid grid-cols-2 border-b border-slate-200">
                  <div className="min-w-0 border-r border-slate-200 px-3 py-2">
                    <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-400">Node status</div>
                    <div className="mt-0.5 break-words text-[12px] font-semibold leading-snug text-slate-950">
                      {selected.currentState}
                    </div>
                  </div>
                  <div className="min-w-0 px-3 py-2">
                    <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-400">Cell type</div>
                    <div className="mt-0.5 break-words text-[12px] font-semibold leading-snug text-slate-950">
                      {selected.kind}
                    </div>
                  </div>
                </div>
                <div className="grid grid-cols-2">
                  <div className="border-r border-slate-200 px-3 py-2">
                    <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-400">Start time</div>
                    <div className="mt-0.5 text-[12px] font-semibold text-slate-950">{clockLabel(run.startedAt)}</div>
                  </div>
                  <div className="px-3 py-2">
                    <div className="text-[10px] font-bold uppercase tracking-[0.08em] text-slate-400">End time</div>
                    <div className="mt-0.5 text-[12px] font-semibold text-slate-950">{clockLabel(run.finishedAt)}</div>
                  </div>
                </div>
              </div>
            </div>
          </aside>

          <main className="flex min-h-0 flex-col overflow-hidden bg-white">
            <div className="flex items-center gap-1.5 border-b border-slate-200 px-4 py-2">
              <button
                type="button"
                onClick={() => onTabChange("query")}
                className={`rounded-md px-3 py-1 text-[12px] font-semibold transition ${
                  activeTab === "query"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                SQL
              </button>
              <button
                type="button"
                onClick={() => onTabChange("data")}
                disabled={dataTabBlocked}
                className={`rounded-md px-3 py-1 text-[12px] font-semibold transition ${
                  activeTab === "data"
                    ? "bg-slate-900 text-white"
                    : dataTabBlocked
                      ? "cursor-not-allowed text-slate-300"
                      : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Data
              </button>
              <button
                type="button"
                onClick={onOpenArtifact}
                disabled={dataTabBlocked}
                aria-label="Open artifact explorer"
                title="Open artifact explorer"
                className={`inline-flex h-7 w-7 items-center justify-center rounded-md border transition ${
                  dataTabBlocked
                    ? "cursor-not-allowed border-slate-200 text-slate-300"
                    : "border-slate-200 text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                <Database size={14} strokeWidth={1.9} />
              </button>
              <button
                type="button"
                onClick={() => onTabChange("columns")}
                className={`rounded-md px-3 py-1 text-[12px] font-semibold transition ${
                  activeTab === "columns"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Columns
              </button>
              <button
                type="button"
                onClick={() => onTabChange("history")}
                className={`rounded-md px-3 py-1 text-[12px] font-semibold transition ${
                  activeTab === "history"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Node history
              </button>
              <button
                type="button"
                onClick={() => onTabChange("logs")}
                className={`rounded-md px-3 py-1 text-[12px] font-semibold transition ${
                  activeTab === "logs"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Logs
              </button>
              <button
                type="button"
                onClick={() => onTabChange("upstream")}
                className={`rounded-md px-3 py-1 text-[12px] font-semibold transition ${
                  activeTab === "upstream"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Upstream nodes
              </button>
              <button
                type="button"
                onClick={() => onTabChange("downstream")}
                className={`rounded-md px-3 py-1 text-[12px] font-semibold transition ${
                  activeTab === "downstream"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Downstream nodes
              </button>
            </div>

            <div className="min-h-0 flex-1 overflow-hidden">
              {tabError ? (
                <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-rose-600">
                  {tabError}
                </div>
              ) : tabLoading === activeTab ? (
                <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-slate-400">
                  Loading...
                </div>
              ) : activeTab === "history" ? (
                <HistoryTable items={history} />
              ) : activeTab === "data" ? (
                <ArtifactPreviewPanel
                  preview={artifactPreview}
                  loading={artifactPreviewLoading}
                  error={artifactPreviewError}
                  onOpenArtifact={onOpenArtifact}
                  blocked={dataTabBlocked}
                  blockedMessage="Data preview is available only for completed nodes."
                />
              ) : activeTab === "columns" ? (
                <ColumnMappingsPanel items={selected.columnMappings} />
              ) : activeTab === "logs" ? (
                logs.length ? (
                  <div className="flex h-full min-h-0 flex-col gap-2 overflow-y-auto px-2 py-1 text-[12px] text-slate-600">
                    {logs.map((entry, index) => (
                      <div
                        key={`${entry.timestamp || ""}-${index}`}
                        className={`rounded-md border px-3 py-2 ${
                          String(entry.severity || "").toLowerCase() === "error"
                            ? "border-rose-200 bg-rose-50"
                            : String(entry.severity || "").toLowerCase() === "warning"
                              ? "border-amber-200 bg-amber-50"
                              : "border-slate-200 bg-white"
                        }`}
                      >
                        <div className="flex flex-wrap items-center gap-2 text-[10px] uppercase tracking-[0.12em] text-slate-400">
                          <span>{entry.severity || "info"}</span>
                          <span className="text-slate-300">•</span>
                          <span>{entry.code || "log"}</span>
                          {entry.timestamp ? (
                            <>
                              <span className="text-slate-300">•</span>
                              <span>{entry.timestamp}</span>
                            </>
                          ) : null}
                        </div>
                        <div
                          className={`mt-1 text-[12px] font-medium ${
                            String(entry.severity || "").toLowerCase() === "error"
                              ? "text-rose-700"
                              : String(entry.severity || "").toLowerCase() === "warning"
                                ? "text-amber-800"
                                : "text-slate-700"
                          }`}
                        >
                          {entry.message || "-"}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-slate-400">
                    No log entries are available for this node.
                  </div>
                )
              ) : activeTab === "query" ? (
                <QueryPanel
                  rawSql={selected.rawSql}
                  resolvedSql={selected.resolvedSql}
                  hasRuntimeVars={selected.hasRuntimeVars}
                  kind={selected.kind}
                  checkOperator={selected.checkOperator}
                  checkValue={selected.checkValue}
                />
              ) : (
                <FlatNodeTable items={tabItems} selectedId={selected.id} />
              )}
            </div>
          </main>
        </div>
      </div>
    </div>
  );
}

function ColumnMappingsPanel({ items }) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) {
    return (
      <div className="flex h-full items-center justify-center px-8 text-center text-[13px] text-slate-400">
        No column mappings available for this node.
      </div>
    );
  }
  return (
    <div className="h-full min-h-0 overflow-y-auto px-2 py-1">
      <div className="min-h-full rounded-md border border-slate-200">
        <table className="min-w-full border-collapse text-[12px]">
          <thead className="bg-slate-50 text-slate-500">
            <tr>
              <th className="border-b border-slate-200 px-3 py-2 text-left font-semibold">Source</th>
              <th className="border-b border-slate-200 px-3 py-2 text-left font-semibold">Source Type</th>
              <th className="border-b border-slate-200 px-3 py-2 text-left font-semibold">Target</th>
              <th className="border-b border-slate-200 px-3 py-2 text-left font-semibold">Target Type</th>
              <th className="border-b border-slate-200 px-3 py-2 text-left font-semibold">Connector</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((item, index) => (
              <tr key={`${item.target_column || "col"}-${index}`} className="border-b border-slate-100 last:border-b-0">
                <td className="px-3 py-2 font-medium text-slate-700">{item.source_column || "-"}</td>
                <td className="px-3 py-2 text-slate-500">{item.source_type || "-"}</td>
                <td className="px-3 py-2 font-medium text-slate-700">{item.target_column || "-"}</td>
                <td className="px-3 py-2 text-slate-500">{item.target_type || "-"}</td>
                <td className="px-3 py-2 text-slate-500">{item.connector_type || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PipelineLogsPanel({
  open,
  onClose,
  logs = [],
  loading = false,
  error = "",
  runId = null,
  runLabel = null,
}) {
  if (!open) return null;
  return (
    <div className="pointer-events-none absolute inset-y-0 right-0 z-30 flex">
      <div className="pointer-events-auto h-full w-[720px] max-w-[96vw] border-l border-slate-200 bg-white shadow-xl">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <div className="min-w-0">
            <div className="text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-400">Pipeline Logs</div>
            <div className="mt-1 truncate text-[12px] font-medium text-slate-700">
              {runLabel ? `${runId || "-"} • ${runLabel}` : (runId || "-")}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex items-center gap-1 rounded-md px-2 py-1 text-[12px] font-medium text-slate-500 transition hover:bg-slate-100 hover:text-slate-900"
          >
            <X size={14} strokeWidth={2.25} />
            <span>Close</span>
          </button>
        </div>
        <div className="h-[calc(100%-61px)] overflow-y-auto px-4 py-3">
          {loading ? (
            <div className="text-[13px] text-slate-500">Loading pipeline logs...</div>
          ) : error ? (
            <div className="text-[13px] text-rose-600">{error}</div>
          ) : !logs.length ? (
            <div className="text-[13px] text-slate-400">No pipeline logs found.</div>
          ) : (
            <div className="space-y-2">
              {logs.map((entry, index) => {
                const timestamp = String(entry?.timestamp || "").trim() || "-";
                const severity = String(entry?.severity || "info").trim().toLowerCase() || "info";
                const code = String(entry?.code || "").trim() || "-";
                const nodeName = String(entry?.node_name || "").trim() || "-";
                const message = String(entry?.message || "").trim() || "-";
                return (
                  <div
                    key={`${timestamp}-${code}-${index}`}
                    className={`rounded-md border px-3 py-2 ${
                      severity === "error"
                        ? "border-rose-200 bg-rose-50"
                        : severity === "warning"
                          ? "border-amber-200 bg-amber-50"
                          : "border-slate-200 bg-white"
                    }`}
                  >
                    <div className="grid grid-cols-[minmax(155px,auto)_auto_minmax(130px,auto)_minmax(150px,1fr)] items-center gap-x-2 gap-y-1 text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-400">
                      <span className="whitespace-nowrap">{timestamp}</span>
                      <span className="whitespace-nowrap">{severity}</span>
                      <span className="break-words">{code}</span>
                      <span className="break-words">{nodeName}</span>
                    </div>
                    <div
                      className={`mt-1 break-words text-[12px] font-medium leading-5 ${
                        severity === "error"
                          ? "text-rose-700"
                          : severity === "warning"
                            ? "text-amber-800"
                            : "text-slate-700"
                      }`}
                    >
                      {message}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function RuntimeVarsPanel({
  open,
  onClose,
  contract = [],
  values = {},
  onChangeValue,
  pipelineId = null,
  error = "",
  submitLabel = "",
  onSubmit,
  submitDisabled = false,
  lockImmutableValues = false,
}) {
  const items = Array.isArray(contract) ? contract : [];
  return (
    <div
      className={`pointer-events-none fixed inset-y-0 right-0 z-40 flex transform-gpu transition-[transform,opacity] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] ${
        open ? "translate-x-0 opacity-100" : "translate-x-full opacity-0"
      }`}
    >
      <div className="pointer-events-auto h-full w-[560px] max-w-[92vw] border-l border-slate-200 bg-gradient-to-b from-white to-slate-50 shadow-xl">
        <div className="flex items-center justify-between border-b border-slate-200 bg-white/90 px-5 py-4">
          <div className="min-w-0">
            <div className="text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-400">Runtime Vars</div>
            <div className="mt-1 truncate text-[13px] font-semibold tracking-[-0.01em] text-slate-800">{pipelineId || "-"}</div>
          </div>
          <div className="flex items-center gap-2">
            {submitLabel ? (
              <button
                type="button"
                onClick={onSubmit}
                disabled={submitDisabled}
                className={`inline-flex items-center rounded-lg px-3 py-1.5 text-[12px] font-semibold transition ${
                  submitDisabled
                    ? "cursor-not-allowed bg-slate-200 text-slate-400"
                    : "bg-slate-900 text-white hover:bg-slate-800"
                }`}
              >
                {submitLabel}
              </button>
            ) : null}
            <button
              type="button"
              onClick={onClose}
              className="inline-flex items-center gap-1 rounded-md px-2 py-1 text-[12px] font-medium text-slate-500 transition hover:bg-slate-100 hover:text-slate-900"
            >
              <X size={14} strokeWidth={2.25} />
              <span>Close</span>
            </button>
          </div>
        </div>
        <div className="flex h-[calc(100%-69px)] flex-col gap-4 px-5 py-5">
          <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-[12px] text-slate-500 shadow-[0_1px_2px_rgba(15,23,42,0.04)]">
            Enter values for the declared runtime vars. List inputs accept comma-separated values or JSON arrays.
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto pr-1">
            {items.length ? (
              <div className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-[0_1px_2px_rgba(15,23,42,0.04)]">
                {items.map((item) => {
                  const name = String(item?.name || "").trim();
                  const kind = String(item?.kind || "scalar").trim().toLowerCase();
                  const currentValue = String(values?.[name] || "");
                  const defaultValue =
                    item?.default !== undefined && item?.default !== null
                      ? (Array.isArray(item.default) ? item.default.join(", ") : String(item.default))
                      : "";
                  const mutableAfterStart = Boolean(item?.mutable_after_start);
                  const readOnly = lockImmutableValues && !mutableAfterStart;
                  return (
                    <div key={name} className="border-b border-slate-200 px-4 py-3 last:border-b-0">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="text-[12px] font-semibold text-slate-900">{name}</div>
                          <div className="mt-0.5 text-[10px] uppercase tracking-[0.12em] text-slate-400">
                            {kind === "list" ? "List" : "Scalar"}
                          </div>
                        </div>
                        <span
                          className={`rounded-full px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.08em] ${
                            mutableAfterStart
                              ? "bg-emerald-50 text-emerald-700"
                              : "bg-slate-100 text-slate-500"
                          }`}
                        >
                          {mutableAfterStart ? "Mutable after start" : "Locked after start"}
                        </span>
                      </div>
                      {defaultValue ? (
                        <div className="mt-1.5 text-[10px] text-slate-400">
                          Default: <span className="font-mono text-slate-500">{defaultValue}</span>
                        </div>
                      ) : null}
                      <div className="mt-2">
                      {kind === "list" ? (
                        <textarea
                          value={currentValue}
                          onChange={(event) => onChangeValue?.(name, event.target.value)}
                          readOnly={readOnly}
                          spellCheck={false}
                          className={`min-h-[92px] w-full resize-y rounded-lg border border-slate-200 px-3 py-2.5 font-mono text-[11px] leading-5 outline-none transition ${
                            readOnly
                              ? "cursor-not-allowed bg-slate-100 text-slate-500"
                              : "bg-slate-50 text-slate-700 focus:border-slate-300 focus:bg-white"
                          }`}
                          placeholder={defaultValue || "TX, CA, WA"}
                        />
                      ) : (
                        <input
                          value={currentValue}
                          onChange={(event) => onChangeValue?.(name, event.target.value)}
                          readOnly={readOnly}
                          spellCheck={false}
                          className={`w-full rounded-lg border border-slate-200 px-3 py-2.5 font-mono text-[11px] leading-5 outline-none transition ${
                            readOnly
                              ? "cursor-not-allowed bg-slate-100 text-slate-500"
                              : "bg-slate-50 text-slate-700 focus:border-slate-300 focus:bg-white"
                          }`}
                          placeholder={defaultValue || "value"}
                        />
                      )}
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-[12px] text-slate-500 shadow-[0_1px_2px_rgba(15,23,42,0.04)]">
                No runtime vars declared for this pipeline.
              </div>
            )}
          </div>
          {error ? <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-[12px] text-rose-700">{error}</div> : null}
        </div>
      </div>
    </div>
  );
}

function buildRuntimeVarInputDefaults(contract) {
  const items = Array.isArray(contract) ? contract : [];
  const defaults = {};
  for (const item of items) {
    const name = String(item?.name || "").trim();
    if (!name) continue;
    if (item?.default === undefined || item?.default === null) continue;
    defaults[name] = Array.isArray(item.default) ? item.default.join(", ") : String(item.default);
  }
  return defaults;
}

function buildRuntimeVarInputsFromValues(contract, sourceValues) {
  const items = Array.isArray(contract) ? contract : [];
  const values = sourceValues && typeof sourceValues === "object" && !Array.isArray(sourceValues) ? sourceValues : {};
  const inputs = {};
  for (const item of items) {
    const name = String(item?.name || "").trim();
    if (!name) continue;
    if (values[name] === undefined || values[name] === null) continue;
    inputs[name] = Array.isArray(values[name]) ? values[name].join(", ") : String(values[name]);
  }
  return inputs;
}

function GraphCanvas({
  graphData,
  graphLoading,
  graphError,
  layoutVersion,
  selectedNodeId,
  onSelectNode,
  sheetOpen,
  onCloseSheet,
  activeTab,
  onTabChange,
  panelData,
  panelLoading,
  panelError,
  panelTabLoading,
  panelTabError,
  onOpenArtifact,
  artifactPreview,
  artifactPreviewLoading,
  artifactPreviewError,
  suppressLoadingOverlay = false,
}) {
  const { nodes, edges } = useMemo(() => {
    const liveNodes = Array.isArray(graphData?.nodes) ? graphData.nodes : [];
    const liveEdges = Array.isArray(graphData?.edges) ? graphData.edges : [];
    const nodeById = new Map(liveNodes.map((node) => [String(node.name || ""), node]));
    const baseNodes = liveNodes.map((node) => ({
      id: String(node.name || ""),
      type: "flowCard",
      position: { x: 0, y: 0 },
      data: buildFlowNodeDataFromApi(node),
      selected: String(node.name || "") === selectedNodeId,
      draggable: false,
      selectable: true,
    }));
    const baseEdges = liveEdges.map(([source, target]) => buildFlowEdgeFromApi(source, target, nodeById));
    return layoutElements(baseNodes, baseEdges);
  }, [graphData, layoutVersion, selectedNodeId]);

  return (
    <div className="relative h-full w-full overflow-hidden">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodeClick={(_, node) => onSelectNode(String(node.id))}
        fitView
        fitViewOptions={FIT_OPTIONS}
        minZoom={0.15}
        maxZoom={1.6}
        nodesConnectable={false}
        nodesDraggable={false}
        elementsSelectable
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#dbe4ee" gap={18} />
        <Controls position="top-left" showInteractive={false} fitViewOptions={FIT_OPTIONS} />
      </ReactFlow>

      {(graphLoading || graphError) && !suppressLoadingOverlay && (
        <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center bg-white/75 px-6 text-center">
          <div className={`rounded-lg border px-4 py-3 text-[13px] shadow-sm ${graphError ? "border-rose-200 bg-rose-50 text-rose-700" : "border-slate-200 bg-white text-slate-500"}`}>
            {graphError || "Loading graph..."}
          </div>
        </div>
      )}

      <InspectBottomSheet
        open={sheetOpen}
        data={panelData}
        onClose={onCloseSheet}
        activeTab={activeTab}
        onTabChange={onTabChange}
        onOpenArtifact={onOpenArtifact}
        artifactPreview={artifactPreview}
        artifactPreviewLoading={artifactPreviewLoading}
        artifactPreviewError={artifactPreviewError}
        loading={panelLoading}
        error={panelError}
        tabLoading={panelTabLoading}
        tabError={panelTabError}
        suppressLoadingOverlay={suppressLoadingOverlay}
      />
    </div>
  );
}

function ArtifactExplorerPage({
  open,
  query,
  onQueryChange,
  onClose,
  onExecute,
  onDownload,
  result,
  loading = false,
  error,
  selectedNodeId,
  selectedArtifactName,
  artifacts,
  artifactsLoading = false,
  onSelectArtifact,
  runId,
}) {
  const [downloadMenuOpen, setDownloadMenuOpen] = useState(false);
  const artifactBlocked = !artifactsLoading && (!artifacts || artifacts.length === 0);
  const selectedArtifact = Array.isArray(artifacts)
    ? artifacts.find((artifact) => artifact.artifactName === selectedArtifactName) || null
    : null;
  const selectedArtifactLabel = String(selectedArtifactName || "").trim()
    .replace(/^"+|"+$/g, "")
    .split(".")
    .slice(-1)[0];
  const hasRows = Array.isArray(result?.rowData) && result.rowData.length > 0;
  const hasQueryResult = Boolean(result);
  return (
    <div
      className={`pointer-events-none absolute inset-0 z-40 transform-gpu bg-white/80 transition-[transform,opacity] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] ${
        open ? "translate-x-0 opacity-100" : "translate-x-full opacity-0"
      }`}
    >
      <div className="pointer-events-auto absolute inset-0 flex flex-col bg-white shadow-[-24px_0_80px_rgba(15,23,42,0.12)]">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-1.5">
          <div>
            <div className="text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-400">Run ID</div>
            <div className="mt-0.5 text-[12px] font-medium text-slate-700">{runId || "-"}</div>
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onExecute}
              title="Execute query"
              aria-label="Execute query"
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-950"
            >
              <Play size={16} strokeWidth={1.9} />
            </button>
            <div className="relative">
              <button
                type="button"
                onClick={() => setDownloadMenuOpen((current) => !current)}
                title="Download query result"
                aria-label="Download query result"
                className="inline-flex h-9 items-center justify-center gap-1 rounded-lg border border-slate-200 bg-white px-2.5 text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-950"
              >
                <Download size={16} strokeWidth={1.9} />
                <ChevronDown size={14} strokeWidth={1.9} />
              </button>
              {downloadMenuOpen ? (
                <div className="absolute right-0 top-11 z-20 min-w-[132px] overflow-hidden rounded-lg border border-slate-200 bg-white shadow-lg">
                  {[
                    ["csv", "CSV"],
                    ["parquet", "Parquet"],
                    ["json", "JSON"],
                  ].map(([format, label]) => (
                    <button
                      key={format}
                      type="button"
                      onClick={() => {
                        setDownloadMenuOpen(false);
                        onDownload?.(format);
                      }}
                      className="block w-full px-3 py-2 text-left text-[12px] font-medium text-slate-700 transition hover:bg-slate-50"
                    >
                      {label}
                    </button>
                  ))}
                </div>
              ) : null}
            </div>
            <button
              type="button"
              onClick={onClose}
              title="Close artifacts"
              aria-label="Close artifacts"
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-950"
            >
              <X size={16} strokeWidth={1.9} />
            </button>
          </div>
        </div>

        <div className="grid min-h-0 flex-1 gap-0 lg:grid-cols-[280px_minmax(0,1fr)]">
          <aside className="flex min-h-0 flex-col border-r border-slate-200 bg-slate-50">
            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-5">
              <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Available Tables</div>
              <div className="mt-3 space-y-2">
                {artifactsLoading ? (
                  <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-[12px] text-slate-500 shadow-[0_1px_3px_rgba(15,23,42,0.04)]">
                    Loading artifacts...
                  </div>
                ) : artifactBlocked ? (
                  <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-[12px] text-slate-500 shadow-[0_1px_3px_rgba(15,23,42,0.04)]">
                    Artifacts are available after the run finishes.
                  </div>
                ) : Array.isArray(artifacts) && artifacts.length ? (
                  [...artifacts]
                    .sort((left, right) => {
                      const leftName = String(left?.artifactName || "")
                        .replace(/^"+|"+$/g, "")
                        .split(".")
                        .slice(-1)[0]
                        .toLowerCase();
                      const rightName = String(right?.artifactName || "")
                        .replace(/^"+|"+$/g, "")
                        .split(".")
                        .slice(-1)[0]
                        .toLowerCase();
                      return leftName.localeCompare(rightName);
                    })
                    .map((artifact) => {
                    const displayName = String(artifact.artifactName || "")
                      .replace(/^"+|"+$/g, "")
                      .split(".")
                      .slice(-1)[0];
                    const tone = normalizeTone(artifact.nodeKind || "", artifact.nodeName || "");
                    const dotStyle = kindDotStyle(tone);
                    return (
                      <button
                        key={artifact.artifactName}
                        type="button"
                        onClick={() => onSelectArtifact?.(artifact)}
                        className="w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-left shadow-[0_1px_3px_rgba(15,23,42,0.04)] transition-transform transition-colors duration-150 hover:border-slate-300 hover:bg-slate-50 active:scale-[0.985] active:bg-slate-100"
                      >
                        <div className="flex items-start gap-2">
                          <span
                            className="mt-[6px] h-2 w-2 shrink-0 rounded-full"
                            style={{ backgroundColor: dotStyle.bg }}
                          />
                          <div className="min-w-0">
                            <div className="truncate text-[12px] font-semibold text-slate-900">
                              {displayName || artifact.artifactName}
                            </div>
                            <div className="mt-0.5 truncate text-[10px] text-slate-500">
                              {artifact.nodeName || selectedNodeId || "selected run"}
                            </div>
                          </div>
                        </div>
                      </button>
                    );
                  })
                ) : (
                  <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-[12px] text-slate-500 shadow-[0_1px_3px_rgba(15,23,42,0.04)]">
                    No materialized artifacts are available for the selected run.
                  </div>
                )}
              </div>
            </div>
          </aside>

          <main className="flex min-h-0 flex-col bg-white">
            <div className="border-b border-slate-200 px-6 py-5">
              <textarea
                value={query}
                onChange={(event) => onQueryChange(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
                    event.preventDefault();
                    onExecute();
                  }
                }}
                placeholder={"SHOW TABLES\nSELECT * FROM main.policy_core LIMIT 25\nSELECT * FROM main.policy_core WHERE category = 'policy' ORDER BY metric_value DESC\nSELECT entity_key, metric_value FROM main.policy_core WHERE metric_value > 50 LIMIT 5"}
                className="h-48 w-full resize-none bg-transparent px-0 py-0 text-[12px] leading-6 text-slate-800 outline-none placeholder:text-slate-300"
                spellCheck={false}
              />
              {error ? <div className="mt-3 text-[13px] text-rose-600">{error}</div> : null}
              {result?.summary ? <div className="mt-3 text-[13px] text-slate-500">{result.summary}</div> : null}
            </div>

            <div className="min-h-0 flex-1 border-t border-slate-200 bg-slate-50/95">
              <div className="h-full p-4">
                <div className="relative flex h-full w-full flex-col overflow-hidden bg-white">
                  <div className="min-h-0 flex-1 overflow-hidden">
                    {loading ? (
                      <div className="flex h-full items-center justify-center px-8 text-center">
                        <div className="flex max-w-[280px] flex-col items-center">
                          <div className="h-8 w-8 animate-spin rounded-full border-2 border-slate-200 border-t-slate-500" />
                          <div className="mt-3 text-[13px] font-medium text-slate-400">
                            Running query...
                          </div>
                        </div>
                      </div>
                    ) : hasQueryResult && hasRows ? (
                      <div className="ag-theme-quartz loom-grid min-h-0 h-full flex-1 overflow-hidden">
                        <AgGridReact
                          rowData={result?.rowData || []}
                          columnDefs={result?.columnDefs || []}
                          defaultColDef={{
                            sortable: true,
                            filter: true,
                            resizable: true,
                            minWidth: 120,
                          }}
                          headerHeight={26}
                          rowHeight={26}
                          animateRows
                          rowSelection="single"
                          suppressCellFocus
                        />
                      </div>
                    ) : (
                      <div className="flex h-full items-center justify-center px-8 text-center">
                        <div className="flex max-w-[280px] flex-col items-center">
                          <Database size={24} strokeWidth={1.7} className="text-slate-300" />
                          <div className="mt-3 text-[13px] font-medium text-slate-400">
                            {hasQueryResult ? "No rows to show" : "Run a query to see results"}
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </main>
        </div>
      </div>
    </div>
  );
}

function HeaderActionButton({ title, icon: Icon, onClick, disabled = false }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={title}
      aria-label={title}
      className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-500 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-900 disabled:cursor-not-allowed disabled:opacity-40"
    >
      <Icon size={16} strokeWidth={1.9} />
    </button>
  );
}

export default function App() {
  const [graphData, setGraphData] = useState(null);
  const [graphLoading, setGraphLoading] = useState(true);
  const [graphError, setGraphError] = useState("");
  const [selectedNodeId, setSelectedNodeId] = useState("");
  const [panelData, setPanelData] = useState(null);
  const [panelLoading, setPanelLoading] = useState(false);
  const [panelError, setPanelError] = useState("");
  const [sheetOpen, setSheetOpen] = useState(false);
  const [activeTab, setActiveTab] = useState("query");
  const [panelTabLoading, setPanelTabLoading] = useState("");
  const [panelTabError, setPanelTabError] = useState("");
  const [pendingAction, setPendingAction] = useState("");
  const [actionError, setActionError] = useState("");
  const [artifactPageOpen, setArtifactPageOpen] = useState(false);
  const [artifactQuery, setArtifactQuery] = useState("");
  const [artifactResult, setArtifactResult] = useState(null);
  const [artifactQueryLoading, setArtifactQueryLoading] = useState(false);
  const [artifactResultUpdatedAt, setArtifactResultUpdatedAt] = useState(null);
  const [artifactError, setArtifactError] = useState("");
  const [selectedArtifactName, setSelectedArtifactName] = useState("");
  const [artifactPreview, setArtifactPreview] = useState(null);
  const [artifactPreviewLoading, setArtifactPreviewLoading] = useState(false);
  const [artifactPreviewError, setArtifactPreviewError] = useState("");
  const [artifactList, setArtifactList] = useState([]);
  const [artifactListLoading, setArtifactListLoading] = useState(false);
  const [pipelineLogsOpen, setPipelineLogsOpen] = useState(false);
  const [pipelineLogs, setPipelineLogs] = useState([]);
  const [pipelineLogsLoading, setPipelineLogsLoading] = useState(false);
  const [pipelineLogsError, setPipelineLogsError] = useState("");
  const [runtimeVarsOpen, setRuntimeVarsOpen] = useState(false);
  const [runtimeVarsSubmitEndpoint, setRuntimeVarsSubmitEndpoint] = useState("");
  const [runtimeVarInputs, setRuntimeVarInputs] = useState({});
  const [runtimeVarsError, setRuntimeVarsError] = useState("");
  const [layoutVersion, setLayoutVersion] = useState(0);
  const [runOptions, setRunOptions] = useState([]);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [runMenuOpen, setRunMenuOpen] = useState(false);
  const [isSynchronizedRefreshRunning, setIsSynchronizedRefreshRunning] = useState(false);
  const [pageRefreshActive, setPageRefreshActive] = useState(false);
  const runStatus = String(graphData?.run_status || "").trim().toLowerCase();
  const isFinalRun = Boolean(graphData?.is_final);
  const hasSelectedNode = Boolean(selectedNodeId);
  const isRunning = runStatus === "running";
  const isFailed = runStatus === "failed";
  const canRun = !isRunning;
  const canStop = isRunning && !isFinalRun;
  const canResume = isFailed && !isFinalRun;
  const canResetAll = isFailed && !isFinalRun;
  const canResetNode = isFailed && hasSelectedNode && !isFinalRun;
  const canResetUpstream = isFailed && hasSelectedNode && !isFinalRun;
  const controlsDisabled = Boolean(pendingAction) || isRunning;
  const stopDisabled = pendingAction === "/api/stop" || !canStop;
  const forceStopDisabled = pendingAction === "/api/force-stop" || !canStop;
  const selectedNodeIdRef = useRef("");
  const selectedRunIdRef = useRef("");
  const runMenuRef = useRef(null);
  const sheetOpenRef = useRef(false);
  const activeTabRef = useRef("query");
  const artifactPageOpenRef = useRef(false);
  const runtimeVarsStorageKey = useMemo(() => {
    const pipelineId = String(graphData?.pipeline_id || "").trim();
    return pipelineId ? `queron.runtimeVars.${pipelineId}` : "";
  }, [graphData?.pipeline_id]);
  const runtimeVarsContract = useMemo(
    () =>
      (Array.isArray(graphData?.active_runtime_vars_contract)
        ? graphData.active_runtime_vars_contract
        : Array.isArray(graphData?.runtime_vars_contract)
          ? graphData.runtime_vars_contract
          : []),
    [graphData?.active_runtime_vars_contract, graphData?.runtime_vars_contract],
  );
  const runtimeVarInputDefaults = useMemo(
    () => buildRuntimeVarInputDefaults(runtimeVarsContract),
    [runtimeVarsContract],
  );
  function buildRunScopedPath(path, params = {}) {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== null && value !== undefined && String(value).trim() !== "") {
        search.set(key, String(value));
      }
    });
    const activeRunId = String(selectedRunIdRef.current || "").trim();
    if (activeRunId) {
      search.set("run_id", activeRunId);
    }
    const suffix = search.toString();
    return suffix ? `${path}?${suffix}` : path;
  }

  async function loadRunOptions() {
    try {
      const response = await fetch("/api/runs");
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "Failed to load runs.");
      setRunOptions(Array.isArray(payload.runs) ? payload.runs : []);
    } catch (_error) {
      setRunOptions([]);
    }
  }

  async function loadGraph() {
    setGraphLoading(true);
    try {
      const response = await fetch(buildRunScopedPath("/api/graph"));
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "Failed to load graph.");
      setGraphData(payload);
      setGraphError("");
    } catch (error) {
      setGraphError(error instanceof Error ? error.message : String(error));
    } finally {
      setGraphLoading(false);
    }
  }

  async function loadNodePanel(nodeId) {
    if (!nodeId) return;
    setPanelLoading(true);
    try {
      const response = await fetch(buildRunScopedPath("/api/node", { node_name: nodeId }));
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "Failed to load node details.");
      setPanelData((current) => {
        const next = buildInspectPanelDataFromApi(payload);
        if (
          current?.selectedStatic?.id &&
          next?.selectedStatic?.id &&
          String(current.selectedStatic.id) === String(next.selectedStatic.id)
        ) {
          next.selectedStatic = {
            ...next.selectedStatic,
            rawSql: next.selectedStatic.rawSql ?? current.selectedStatic.rawSql ?? null,
            resolvedSql: next.selectedStatic.resolvedSql ?? current.selectedStatic.resolvedSql ?? null,
            hasRuntimeVars:
              Boolean(next.selectedStatic.hasRuntimeVars) || Boolean(current.selectedStatic.hasRuntimeVars),
            runtimeVarNames:
              Array.isArray(next.selectedStatic.runtimeVarNames) && next.selectedStatic.runtimeVarNames.length
                ? next.selectedStatic.runtimeVarNames
                : (Array.isArray(current.selectedStatic.runtimeVarNames) ? current.selectedStatic.runtimeVarNames : []),
          };
        }
        return next;
      });
      setPanelError("");
    } catch (error) {
      setPanelData(null);
      setPanelError(error instanceof Error ? error.message : String(error));
    } finally {
      setPanelLoading(false);
    }
  }

  async function loadNodeTab(nodeId, tabName) {
    if (!nodeId) return;
    setPanelTabLoading(tabName);
    setPanelTabError("");
    try {
      let endpoint = "";
      if (tabName === "query") endpoint = buildRunScopedPath("/api/node/query", { node_name: nodeId });
      else if (tabName === "history") endpoint = buildRunScopedPath("/api/node/history", { node_name: nodeId });
      else if (tabName === "logs") endpoint = buildRunScopedPath("/api/node/logs", { node_name: nodeId, tail: 200 });
      else if (tabName === "upstream") endpoint = buildRunScopedPath("/api/node/upstream", { node_name: nodeId });
      else if (tabName === "downstream") endpoint = buildRunScopedPath("/api/node/downstream", { node_name: nodeId });
      else return;
      const response = await fetch(endpoint);
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.error || `Failed to load ${tabName}.`);
      setPanelData((current) => {
        if (!current) return current;
        if (tabName === "query") {
          return {
            ...current,
            selectedStatic: {
              ...(current.selectedStatic || {}),
              rawSql: payload?.query?.sql || null,
              resolvedSql: payload?.query?.resolved_sql || null,
              hasRuntimeVars: Boolean(current.selectedStatic?.hasRuntimeVars || payload?.query?.sql?.includes("queron.var(")),
            },
          };
        }
        if (tabName === "logs") {
          return {
            ...current,
            logsInitial: Array.isArray(payload?.logs) ? payload.logs : [],
          };
        }
        if (tabName === "history") {
          const startedAt = payload?.history?.started_at || current.run?.startedAt || null;
          const finishedAt = payload?.history?.finished_at || current.run?.finishedAt || null;
          return {
            ...current,
            run: {
              ...current.run,
              startedAt,
              finishedAt,
              duration: timeLabel(startedAt, finishedAt),
            },
            historyInitial: buildInspectHistoryFromApi(payload),
          };
        }
        if (tabName === "upstream") {
          return {
            ...current,
            upstream: buildInspectItemsFromApi(payload?.nodes, current.selectedStatic?.id || ""),
          };
        }
        if (tabName === "downstream") {
          return {
            ...current,
            downstream: buildInspectItemsFromApi(payload?.nodes, current.selectedStatic?.id || ""),
          };
        }
        return current;
      });
    } catch (error) {
      setPanelTabError(error instanceof Error ? error.message : String(error));
    } finally {
      setPanelTabLoading("");
    }
  }

  function currentRunIdForNodePanel() {
    const activeRunId = String(selectedRunIdRef.current || selectedRunId || "").trim();
    if (activeRunId) return activeRunId;
    return panelData?.run?.runId || graphData?.run_id || null;
  }

  async function loadArtifactList() {
    setArtifactListLoading(true);
    try {
      const params = new URLSearchParams();
      const runId = currentRunIdForNodePanel();
      if (runId) {
        params.set("run_id", runId);
      }
      const suffix = params.toString() ? `?${params.toString()}` : "";
      const response = await fetch(`/api/run/artifacts${suffix}`);
      const payload = await response.json();
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.error || "Failed to load artifacts.");
      }
      if (payload.blocked) {
        setArtifactList([]);
        setArtifactListLoading(false);
        return;
      }
        setArtifactList(
          Array.isArray(payload.artifacts)
            ? payload.artifacts.map((item) => ({
                artifactName: item.artifact_name || "",
                logicalArtifact: item.logical_artifact || null,
                nodeName: item.node_name || null,
                nodeKind: item.node_kind || null,
              }))
            : [],
      );
    } catch (_error) {
      setArtifactList([]);
    } finally {
      setArtifactListLoading(false);
    }
  }

  async function loadPipelineLogs() {
    setPipelineLogsLoading(true);
    setPipelineLogsError("");
    try {
      const response = await fetch(buildRunScopedPath("/api/pipeline/logs", { tail: 200 }));
      const payload = await response.json();
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.error || "Failed to load pipeline logs.");
      }
      setPipelineLogs(Array.isArray(payload.logs) ? payload.logs : []);
    } catch (error) {
      setPipelineLogs([]);
      setPipelineLogsError(error instanceof Error ? error.message : String(error));
    } finally {
      setPipelineLogsLoading(false);
    }
  }

  async function refreshGraphAndDrawer({ refreshActiveTab = false, showPageRefresh = false } = {}) {
    if (showPageRefresh) {
      setPageRefreshActive(true);
    }
    setIsSynchronizedRefreshRunning(true);
    try {
      await loadRunOptions();
      await loadGraph();
      if (artifactPageOpenRef.current) {
        await loadArtifactList();
      }
      if (pipelineLogsOpen) {
        await loadPipelineLogs();
      }
      if (!sheetOpenRef.current || !selectedNodeIdRef.current) return;
      await loadNodePanel(selectedNodeIdRef.current);
      if (refreshActiveTab && activeTabRef.current) {
        await ensurePanelTabLoaded(activeTabRef.current, selectedNodeIdRef.current, { force: true });
      }
    } finally {
      setIsSynchronizedRefreshRunning(false);
      if (showPageRefresh) {
        setPageRefreshActive(false);
      }
    }
  }

  async function ensurePanelTabLoaded(tabName, nodeId = selectedNodeIdRef.current, options = {}) {
    const force = Boolean(options?.force);
    if (!nodeId) return;
    if (isSynchronizedRefreshRunning && !force) return;
    if (tabName === "data") {
      setArtifactPreviewLoading(true);
      setArtifactPreviewError("");
      try {
        const params = new URLSearchParams({
          node_name: nodeId,
          limit: "5",
        });
        const runId = currentRunIdForNodePanel();
        if (runId) {
          params.set("run_id", runId);
        }
        const response = await fetch(`/api/node/artifact-preview?${params.toString()}`);
        const payload = await response.json();
        if (!response.ok || payload.ok === false) throw new Error(payload.error || "Failed to load data preview.");
        setArtifactPreview({
          artifactName: payload.artifact_name || null,
          logicalArtifact: payload.logical_artifact || null,
          columns: Array.isArray(payload.columns) ? payload.columns : [],
          rows: Array.isArray(payload.rows) ? payload.rows : [],
          runId: payload.run_id || null,
        });
      } catch (error) {
        setArtifactPreview(null);
        setArtifactPreviewError(error instanceof Error ? error.message : String(error));
      } finally {
        setArtifactPreviewLoading(false);
      }
      return;
    }
    const current = panelData;
    if (!current) return;
    if (!force && tabName === "query" && current.selectedStatic?.resolvedSql) return;
    if (!force && tabName === "history" && Array.isArray(current.historyInitial)) return;
    if (!force && tabName === "logs" && Array.isArray(current.logsInitial)) return;
    if (!force && tabName === "upstream" && Array.isArray(current.upstream)) return;
    if (!force && tabName === "downstream" && Array.isArray(current.downstream)) return;
    await loadNodeTab(nodeId, tabName);
  }

  useEffect(() => {
    loadRunOptions();
    loadGraph();
  }, []);

  useEffect(() => {
    selectedNodeIdRef.current = selectedNodeId;
  }, [selectedNodeId]);

  useEffect(() => {
    selectedRunIdRef.current = selectedRunId;
  }, [selectedRunId]);

  useEffect(() => {
    function handlePointerDown(event) {
      if (!runMenuRef.current) return;
      if (!runMenuRef.current.contains(event.target)) {
        setRunMenuOpen(false);
      }
    }
    window.addEventListener("mousedown", handlePointerDown);
    return () => {
      window.removeEventListener("mousedown", handlePointerDown);
    };
  }, []);

  useEffect(() => {
    sheetOpenRef.current = sheetOpen;
  }, [sheetOpen]);

  useEffect(() => {
    activeTabRef.current = activeTab;
  }, [activeTab]);

  useEffect(() => {
    artifactPageOpenRef.current = artifactPageOpen;
  }, [artifactPageOpen]);

  useEffect(() => {
    if (!runtimeVarsStorageKey) return;
    try {
      const stored = window.localStorage.getItem(runtimeVarsStorageKey);
      if (stored !== null) {
        const parsed = JSON.parse(stored);
        setRuntimeVarInputs(
          parsed && typeof parsed === "object" && !Array.isArray(parsed)
            ? { ...runtimeVarInputDefaults, ...parsed }
            : { ...runtimeVarInputDefaults },
        );
        setRuntimeVarsError("");
      } else {
        setRuntimeVarInputs({ ...runtimeVarInputDefaults });
      }
    } catch (_error) {
      setRuntimeVarInputs({ ...runtimeVarInputDefaults });
    }
  }, [runtimeVarsStorageKey, runtimeVarInputDefaults]);

  useEffect(() => {
    if (!pipelineLogsOpen) return;
    void loadPipelineLogs();
  }, [pipelineLogsOpen, selectedRunId]);

  useEffect(() => {
    const eventSource = new EventSource("/api/events");

    eventSource.onmessage = (messageEvent) => {
      try {
        const payload = JSON.parse(messageEvent.data);
        if (!payload || payload.type !== "runtime_log") return;
        const eventRunId = String(payload.run_id || "").trim();
        const viewedRunId = String(selectedRunIdRef.current || "").trim();
        if (viewedRunId && eventRunId && viewedRunId !== eventRunId) {
          return;
        }

        setGraphData((current) => patchGraphDataWithEvent(current, payload));
        setPanelData((current) => patchPanelDataWithEvent(current, payload, selectedNodeIdRef.current));
        if (String(payload.code || "").trim().toLowerCase() === "pipeline_execution_started") {
          setArtifactPreview(null);
          setArtifactPreviewError("");
          if (!String(selectedRunIdRef.current || "").trim()) {
            window.setTimeout(() => {
              void refreshGraphAndDrawer({ refreshActiveTab: true, showPageRefresh: false });
            }, 0);
          }
        }

        const code = String(payload.code || "").trim().toLowerCase();
        if (
          artifactPageOpenRef.current &&
          (
            code === "pipeline_execution_started" ||
            code === "node_rows_written" ||
            code === "node_artifact_created" ||
            code === "node_egress_written" ||
            code === "node_export_written" ||
            code === "node_check_passed" ||
            code === "node_warning" ||
            code === "node_execution_failed" ||
            code === "node_skipped"
          )
        ) {
          void loadArtifactList();
        }
        if (code === "pipeline_execution_finished" || code === "pipeline_execution_failed") {
          window.setTimeout(() => {
            void refreshGraphAndDrawer({ refreshActiveTab: true, showPageRefresh: true });
          }, 150);
        }
      } catch (_error) {
        // Ignore malformed SSE payloads.
      }
    };

    eventSource.onerror = () => {
      // Native EventSource reconnect is good enough here.
    };

    return () => {
      eventSource.close();
    };
  }, []);

  async function handleSelectNode(nodeId) {
    setSelectedNodeId(nodeId);
    setActiveTab("query");
    setPanelTabError("");
    setArtifactPreview(null);
    setArtifactPreviewError("");
    setSheetOpen(true);
    await loadNodePanel(nodeId);
    void loadNodeTab(nodeId, "query");
  }

  async function handleTabChange(nextTab) {
    setActiveTab(nextTab);
    setPanelTabError("");
    if (isSynchronizedRefreshRunning) return;
    await ensurePanelTabLoaded(nextTab);
  }

  function parseRuntimeVarScalar(rawValue) {
    const text = String(rawValue || "").trim();
    if (!text) return undefined;
    try {
      const parsed = JSON.parse(text);
      if (parsed !== null && typeof parsed === "object") {
        return text;
      }
      return parsed;
    } catch (_error) {
      return text;
    }
  }

  function parseRuntimeVarList(rawValue) {
    const text = String(rawValue || "").trim();
    if (!text) return undefined;
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        return parsed;
      }
    } catch (_error) {
      // fall through to split parsing
    }
    return text
      .split(/[\n,]/)
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .map((item) => parseRuntimeVarScalar(item));
  }

  function parsedRuntimeVarsOrThrow() {
    const payload = {};
    for (const item of runtimeVarsContract) {
      const name = String(item?.name || "").trim();
      if (!name) continue;
      const kind = String(item?.kind || "scalar").trim().toLowerCase();
      const rawValue = runtimeVarInputs?.[name];
      const parsedValue = kind === "list" ? parseRuntimeVarList(rawValue) : parseRuntimeVarScalar(rawValue);
      if (parsedValue !== undefined) {
        payload[name] = parsedValue;
      }
    }
    return Object.keys(payload).length ? payload : null;
  }

  function handleRuntimeVarInputChange(name, nextValue) {
    setRuntimeVarInputs((current) => ({
      ...(current || {}),
      [name]: nextValue,
    }));
    setRuntimeVarsError("");
  }

  function requestRunAction(endpoint) {
    const requiresRuntimeVars = (endpoint === "/api/run" || endpoint === "/api/resume") && runtimeVarsContract.length > 0;
    if (requiresRuntimeVars) {
      setRuntimeVarsSubmitEndpoint(endpoint);
      setRuntimeVarsError("");
      if (endpoint === "/api/resume" && Object.keys(activeRunRuntimeVarInputs).length) {
        setRuntimeVarInputs((current) => ({ ...(current || {}), ...activeRunRuntimeVarInputs }));
      }
      setRuntimeVarsOpen(true);
      return;
    }
    void runAction(endpoint);
  }

  function submitRuntimeVarsAction() {
    if (!runtimeVarsSubmitEndpoint) return;
    const endpoint = runtimeVarsSubmitEndpoint;
    setRuntimeVarsOpen(false);
    setRuntimeVarsSubmitEndpoint("");
    void runAction(endpoint);
  }

  async function runAction(endpoint, nodeName = "") {
    setPendingAction(endpoint);
    setActionError("");
    setRuntimeVarsError("");
    try {
      const runtimeVars = endpoint === "/api/run" || endpoint === "/api/resume" ? parsedRuntimeVarsOrThrow() : null;
      const requestBody = {
        ...(nodeName ? { node_name: nodeName } : {}),
        ...((endpoint === "/api/run" || endpoint === "/api/resume") ? { runtime_vars: runtimeVars } : {}),
      };
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody),
      });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) {
        if (endpoint === "/api/run" && requiresCleanExistingPrompt(payload)) {
          const shouldClean = window.confirm(
            "This run is blocked because local artifact tables already exist. Selecting OK will remove the current local artifacts for this pipeline and run it again."
          );
          if (!shouldClean) {
            return;
          }
          const cleanResponse = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ clean_existing: true, runtime_vars: runtimeVars }),
          });
          const cleanPayload = await cleanResponse.json();
          if (!cleanResponse.ok || cleanPayload.ok === false) {
            throw new Error(cleanPayload.error || "Action failed.");
          }
        } else {
          throw new Error(payload.error || "Action failed.");
        }
      }
      if ((endpoint === "/api/run" || endpoint === "/api/resume") && runtimeVarsStorageKey) {
        try {
          window.localStorage.setItem(runtimeVarsStorageKey, JSON.stringify(runtimeVarInputs || {}));
        } catch (_error) {
          // Ignore local storage failures.
        }
        setRuntimeVarsOpen(false);
        setRuntimeVarsSubmitEndpoint("");
      }
      if (endpoint === "/api/stop") {
        setActionError("Stop requested.");
      }
      if (endpoint === "/api/force-stop") {
        setActionError("Force stop requested.");
      }
      if (endpoint !== "/api/stop" && endpoint !== "/api/force-stop") {
        await refreshGraphAndDrawer();
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (endpoint === "/api/run" || endpoint === "/api/resume") {
        setRuntimeVarsError(message);
        await refreshGraphAndDrawer();
      }
      setActionError(message);
    } finally {
      setPendingAction("");
    }
  }

  async function handleRunSelectionChange(nextRunId) {
    setSelectedRunId(nextRunId);
    selectedRunIdRef.current = nextRunId;
    setRunMenuOpen(false);
    setPanelError("");
    setPanelTabError("");
    setArtifactPreview(null);
    setArtifactPreviewError("");
    setArtifactResult(null);
    setArtifactResultUpdatedAt(null);
    await refreshGraphAndDrawer({ refreshActiveTab: true });
  }

  const selectedRunOption = useMemo(
    () => runOptions.find((item) => String(item?.run_id || "") === String(selectedRunId || "")) || null,
    [runOptions, selectedRunId],
  );
  const activeRunOption = useMemo(
    () =>
      selectedRunOption ||
      runOptions.find((item) => String(item?.run_id || "") === String(graphData?.run_id || "")) ||
      null,
    [graphData?.run_id, runOptions, selectedRunOption],
  );
  const activeRunRuntimeVarInputs = useMemo(
    () => buildRuntimeVarInputsFromValues(runtimeVarsContract, activeRunOption?.runtime_vars_json),
    [activeRunOption?.runtime_vars_json, runtimeVarsContract],
  );
  const latestRunOptionLabel = useMemo(() => {
    const runId = String(graphData?.run_id || "").trim();
    const runLabel = String(graphData?.run_label || "").trim();
    if (!runId) return "Latest";
    if (runLabel) return `${runId} • ${runLabel} • Latest`;
    return `${runId} • Latest`;
  }, [graphData?.run_id, graphData?.run_label]);
  const selectedRunLabel = useMemo(() => {
    if (!selectedRunOption) return latestRunOptionLabel;
    const runId = String(selectedRunOption?.run_id || "").trim();
    const runLabel = String(selectedRunOption?.run_label || "").trim();
    if (runLabel) return `${runId} • ${runLabel}`;
    return runId || latestRunOptionLabel;
  }, [latestRunOptionLabel, selectedRunOption]);

  function buildArtifactQuery(artifactName) {
    const text = String(artifactName || "").trim();
    if (text) {
      return `SELECT * FROM ${text} LIMIT 25;`;
    }
    return "SELECT * FROM {{artifact}} LIMIT 25;";
  }

  function lastArtifactQueryStatement(text) {
    const statements = String(text || "")
      .split(";")
      .map((item) => item.trim())
      .filter(Boolean);
    return statements.length ? statements[statements.length - 1] : "";
  }

  function appendArtifactQueryLine(currentQuery, artifactName) {
    const nextQuery = buildArtifactQuery(artifactName);
    const current = String(currentQuery || "").trimEnd();
    if (!current) {
      return nextQuery;
    }
    const separator = current.endsWith(";") ? "\n" : ";\n";
    return `${current}${separator}${nextQuery}`;
  }

  function openArtifactExplorerForTable() {
    const preferred = artifactPreview?.artifactName || selectedArtifactName;
    setArtifactQuery(buildArtifactQuery(preferred));
    setArtifactPageOpen(true);
    void loadArtifactList();
  }

  function handleSelectArtifact(artifact) {
    const nextName = artifact?.artifactName || "";
    setSelectedArtifactName(nextName);
    setArtifactQuery((current) => appendArtifactQueryLine(current, nextName));
    if (artifact?.nodeName) {
      setSelectedNodeId(artifact.nodeName);
      selectedNodeIdRef.current = artifact.nodeName;
    }
  }

  useEffect(() => {
    if (!artifactPageOpen) return;
    const desired = selectedArtifactName || artifactPreview?.artifactName;
    if (!desired) return;
    if (artifactQuery.includes("{{artifact}}")) {
      setArtifactQuery(buildArtifactQuery(desired));
    }
  }, [artifactPageOpen, selectedArtifactName, artifactPreview?.artifactName]);

  const executeArtifactQuery = async () => {
    try {
      setArtifactQueryLoading(true);
      const sqlToExecute = lastArtifactQueryStatement(artifactQuery);
      if (!sqlToExecute) {
        throw new Error("Write a query first.");
      }
      const runId = currentRunIdForNodePanel();
      const response = await fetch("/api/run/artifact-query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sql: sqlToExecute,
          run_id: runId,
          artifact_name: selectedArtifactName || artifactPreview?.artifactName || null,
        }),
      });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "Artifact query failed.");
      setArtifactResult({
        rowData: Array.isArray(payload.rows) ? payload.rows : [],
        columnDefs: createGridColumns(Array.isArray(payload.columns) ? payload.columns : []),
        summary: `${Number(payload.row_count || 0)} row${Number(payload.row_count || 0) === 1 ? "" : "s"} returned`,
      });
      setArtifactResultUpdatedAt(new Date().toISOString());
      setArtifactError("");
    } catch (error) {
      setArtifactResult(null);
      setArtifactResultUpdatedAt(null);
      setArtifactError(error instanceof Error ? error.message : String(error));
    } finally {
      setArtifactQueryLoading(false);
    }
  };

  const downloadArtifactQuery = async (format = "csv") => {
    try {
      const sqlToExecute = lastArtifactQueryStatement(artifactQuery);
      if (!sqlToExecute) {
        throw new Error("Write a query first.");
      }
      const runId = currentRunIdForNodePanel();
      const response = await fetch("/api/run/artifact-download", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sql: sqlToExecute,
          run_id: runId,
          format,
          artifact_name: selectedArtifactName || artifactPreview?.artifactName || null,
        }),
      });
      if (!response.ok) {
        let message = "Artifact download failed.";
        try {
          const payload = await response.json();
          message = payload.error || message;
        } catch (_error) {
          // Ignore non-JSON error response.
        }
        throw new Error(message);
      }
      const blob = await response.blob();
      const disposition = response.headers.get("Content-Disposition") || "";
      const match = disposition.match(/filename=\"?([^"]+)\"?/i);
      const filename = match?.[1] || `query_result.${format}`;
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);
      setArtifactError("");
    } catch (error) {
      setArtifactError(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <div className="relative h-screen w-full overflow-hidden bg-slate-100">
      <div className="flex h-full w-full min-h-0 flex-col bg-white">
        <header className="flex items-center justify-between border-b border-slate-200 bg-white px-4 py-1.5">
          <div className="flex items-center gap-4">
            <div>
              <div className="text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-400">Pipeline ID</div>
              <div className="mt-0.5 text-[12px] font-semibold tracking-[-0.01em] text-slate-950">{graphData?.pipeline_id || "-"}</div>
            </div>
            <div>
              <div className="text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-400">Node Count</div>
              <div className="mt-0.5 text-[12px] font-semibold tracking-[-0.01em] text-slate-950">{graphData?.node_count ?? "-"}</div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <div className="flex min-w-0 items-start gap-6">
              <div className="min-w-0 flex-1 text-right">
                <div className="text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-400">Run</div>
                <div className="mt-0.5 flex justify-end">
                  <div ref={runMenuRef} className="relative inline-flex min-w-0 max-w-full flex-col items-end">
                    <button
                      type="button"
                      onClick={() => setRunMenuOpen((current) => !current)}
                      className="inline-flex min-w-0 max-w-full items-center justify-end gap-2 pl-3 text-[12px] font-medium text-slate-700"
                    >
                      <span className="truncate text-right">{selectedRunLabel}</span>
                      <ChevronDown className={`h-3.5 w-3.5 shrink-0 text-slate-400 transition ${runMenuOpen ? "rotate-180" : ""}`} />
                    </button>
                    {runMenuOpen ? (
                      <div className="absolute right-0 top-full z-30 mt-2 w-[420px] max-w-[42vw] overflow-hidden rounded-lg border border-slate-200 bg-white shadow-lg">
                        <div
                          className="max-h-80 overflow-y-auto py-1 [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
                          style={{ msOverflowStyle: "none", scrollbarWidth: "none" }}
                        >
                          <button
                            type="button"
                            onClick={() => {
                              void handleRunSelectionChange("");
                            }}
                            className={`block w-full truncate px-4 py-2 text-right text-[12px] font-medium ${
                              !selectedRunId ? "bg-slate-100 text-slate-950" : "text-slate-700 hover:bg-slate-50"
                            }`}
                          >
                            {latestRunOptionLabel}
                          </button>
                          {runOptions.map((item) => {
                            const runId = String(item?.run_id || "").trim();
                            if (!runId) return null;
                            if (runId === String(graphData?.run_id || "").trim()) return null;
                            const runLabel = String(item?.run_label || "").trim();
                            const label = runLabel ? `${runId} • ${runLabel}` : runId;
                            const isSelected = runId === selectedRunId;
                            return (
                              <button
                                key={runId}
                                type="button"
                                onClick={() => {
                                  void handleRunSelectionChange(runId);
                                }}
                                className={`block w-full truncate px-4 py-2 text-right text-[12px] font-medium ${
                                  isSelected ? "bg-slate-100 text-slate-950" : "text-slate-700 hover:bg-slate-50"
                                }`}
                              >
                                {label}
                              </button>
                            );
                          })}
                        </div>
                      </div>
                    ) : null}
                  </div>
                </div>
              </div>
              <div className="w-[72px] text-right">
                <div className="text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-400">Active</div>
                <div className="mt-0.5 flex h-[18px] items-center justify-end">
                  <span
                    className={`inline-block h-2.5 w-2.5 rounded-full ${isFinalRun ? "bg-slate-300" : "bg-emerald-500"}`}
                    aria-label={isFinalRun ? "Final run" : "Active run"}
                    title={isFinalRun ? "Final run" : "Active run"}
                  />
                </div>
              </div>
            </div>

            <div className="flex items-center gap-1.5">
              <HeaderActionButton
                title="Run from Scratch"
                icon={CirclePlay}
                onClick={() => requestRunAction("/api/run")}
                disabled={controlsDisabled || !canRun}
              />
              <HeaderActionButton
                title="Resume"
                icon={SkipForward}
                onClick={() => requestRunAction("/api/resume")}
                disabled={controlsDisabled || !canResume}
              />
              <HeaderActionButton
                title="Stop"
                icon={Square}
                onClick={() => runAction("/api/stop")}
                disabled={stopDisabled}
              />
              <HeaderActionButton
                title="Force Stop"
                icon={X}
                onClick={() => runAction("/api/force-stop")}
                disabled={forceStopDisabled}
              />
              <HeaderActionButton
                title="Reset All"
                icon={RefreshCw}
                onClick={() => runAction("/api/reset-all")}
                disabled={controlsDisabled || !canResetAll}
              />
              <HeaderActionButton
                title="Reset Node"
                icon={RotateCcw}
                onClick={() => runAction("/api/reset-node", selectedNodeId)}
                disabled={controlsDisabled || !canResetNode}
              />
              <HeaderActionButton
                title="Reset Upstream"
                icon={History}
                onClick={() => runAction("/api/reset-upstream", selectedNodeId)}
                disabled={controlsDisabled || !canResetUpstream}
              />
              <div className="mx-1 h-6 w-px bg-slate-200" aria-hidden="true" />
              <button
                type="button"
                onClick={() => {
                  setArtifactPageOpen(true);
                  void loadArtifactList();
                }}
                className="inline-flex h-9 items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 text-[12px] font-medium text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-950"
              >
                <Database size={15} strokeWidth={1.9} />
                <span>Artifacts</span>
              </button>
              <button
                type="button"
                onClick={() => {
                  setPipelineLogsOpen(true);
                  void loadPipelineLogs();
                }}
                title="Pipeline Logs"
                aria-label="Pipeline Logs"
                className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white text-[12px] font-medium text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-950"
              >
                <Terminal size={15} strokeWidth={1.9} />
              </button>
            </div>
          </div>
        </header>

        {actionError ? (
          <div className="border-b border-rose-200 bg-rose-50 px-4 py-2 text-[13px] text-rose-700">{actionError}</div>
        ) : null}

        <div className="min-h-0 flex-1">
        <ReactFlowProvider>
          <GraphCanvas
            graphData={graphData}
            graphLoading={graphLoading}
            graphError={graphError}
            layoutVersion={layoutVersion}
            selectedNodeId={selectedNodeId}
            onSelectNode={handleSelectNode}
            sheetOpen={sheetOpen}
            onCloseSheet={() => setSheetOpen(false)}
            activeTab={activeTab}
            onTabChange={handleTabChange}
            panelData={panelData}
            panelLoading={panelLoading}
            panelError={panelError}
            panelTabLoading={panelTabLoading}
            panelTabError={panelTabError}
            onOpenArtifact={openArtifactExplorerForTable}
            artifactPreview={artifactPreview}
            artifactPreviewLoading={artifactPreviewLoading}
            artifactPreviewError={artifactPreviewError}
            suppressLoadingOverlay={pageRefreshActive}
          />
        </ReactFlowProvider>
        </div>
      </div>

      {pageRefreshActive ? (
        <div className="absolute inset-0 z-50 flex items-center justify-center bg-white/70">
          <div className="rounded-lg border border-slate-200 bg-white px-4 py-3 text-[13px] text-slate-600 shadow-sm">
            Refreshing pipeline state...
          </div>
        </div>
      ) : null}

        <ArtifactExplorerPage
          open={artifactPageOpen}
          query={artifactQuery}
          onQueryChange={setArtifactQuery}
          onClose={() => setArtifactPageOpen(false)}
          onExecute={executeArtifactQuery}
          onDownload={(format) => void downloadArtifactQuery(format || "csv")}
          result={artifactResult}
          loading={artifactQueryLoading}
          error={artifactError}
          selectedNodeId={selectedNodeId}
          selectedArtifactName={selectedArtifactName || artifactPreview?.artifactName || ""}
          artifacts={artifactList}
          artifactsLoading={artifactListLoading}
          onSelectArtifact={handleSelectArtifact}
          runId={selectedRunOption?.run_id || graphData?.run_id || null}
        />
        <PipelineLogsPanel
          open={pipelineLogsOpen}
          onClose={() => setPipelineLogsOpen(false)}
          logs={pipelineLogs}
          loading={pipelineLogsLoading}
          error={pipelineLogsError}
          runId={selectedRunOption?.run_id || graphData?.run_id || null}
          runLabel={selectedRunOption?.run_label || graphData?.run_label || null}
        />
        <RuntimeVarsPanel
          open={runtimeVarsOpen}
          onClose={() => {
            setRuntimeVarsOpen(false);
            setRuntimeVarsSubmitEndpoint("");
          }}
          contract={runtimeVarsContract}
          values={runtimeVarInputs}
          onChangeValue={handleRuntimeVarInputChange}
          pipelineId={graphData?.pipeline_id || null}
          error={runtimeVarsError}
          submitLabel={
            runtimeVarsSubmitEndpoint === "/api/resume"
              ? "Resume"
              : runtimeVarsSubmitEndpoint === "/api/run"
                ? "Run"
                : ""
          }
          onSubmit={() => {
            submitRuntimeVarsAction();
          }}
          submitDisabled={Boolean(pendingAction) || !runtimeVarsSubmitEndpoint}
          lockImmutableValues={runtimeVarsSubmitEndpoint === "/api/resume"}
        />
    </div>
  );
}

