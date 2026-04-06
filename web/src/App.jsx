import { useMemo, useState } from "react";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  ReactFlowProvider,
} from "@xyflow/react";
import { CirclePlay, SkipForward, RotateCcw, RefreshCw, History } from "lucide-react";
import dagre from "dagre";
import "@xyflow/react/dist/style.css";

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
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function titleForNode(id, details) {
  if (id === "sql_103") return "Boolean Check";
  if (id === "sql_105") return "Count Check";
  if (id === "python_113") return "Service Playbook";
  if (details.artifact) return humanize(details.artifact);
  return humanize(id);
}

function normalizeTone(kind, nodeId) {
  const lowered = String(kind || "").trim().toLowerCase();
  if (nodeId.startsWith("source:") || lowered === "source") return "source";
  if (lowered.includes("file.ingress")) return "file";
  if (lowered.includes("python")) return "python";
  if (lowered.includes("ingress")) return "ingress";
  if (lowered.includes("check")) return "check";
  if (lowered.includes("egress") || lowered.includes("export")) return "egress";
  if (lowered.includes("model") || lowered.includes("sql")) return "model";
  return "other";
}

function toneClasses(tone) {
  if (tone === "source") return { border: "border-slate-300", bg: "bg-amber-50/80", badge: "bg-amber-100 text-amber-700", meta: "text-amber-700/80" };
  if (tone === "ingress") return { border: "border-slate-300", bg: "bg-blue-50/80", badge: "bg-blue-100 text-blue-700", meta: "text-blue-700/80" };
  if (tone === "file") return { border: "border-slate-300", bg: "bg-cyan-50/80", badge: "bg-cyan-100 text-cyan-700", meta: "text-cyan-700/80" };
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

function FlowCardNode({ data, selected }) {
  const toneStyles = {
    source: { accent: "#d3b165", tint: "#fdf5df", icon: "#7b5d18", badgeBg: "#f8e7b8", badgeText: "#7b5d18" },
    ingress: { accent: "#5eaef5", tint: "#eaf5ff", icon: "#165387", badgeBg: "#dbeeff", badgeText: "#165387" },
    file: { accent: "#59c4d8", tint: "#e7fbff", icon: "#116575", badgeBg: "#d7f6fb", badgeText: "#116575" },
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
          fontFamily: "Arial, Helvetica, sans-serif",
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
            clipPath: "polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%)",
            filter: decisionPalette.shadow,
          }}
        />

        <div
          style={{
            position: "absolute",
            inset: 2,
            background: decisionPalette.fill,
            clipPath: "polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%)",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            textAlign: "center",
            padding: "18px 34px",
          }}
        >
          <div
            style={{
              fontSize: 13,
              fontWeight: 700,
              color: "#2d3037",
              lineHeight: 1.2,
              marginBottom: 6,
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
            }}
          >
            {data.id}
          </div>
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
              marginBottom: 8,
            }}
          >
            {data.runtimeLabel}
          </span>
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
        fontFamily: "Arial, Helvetica, sans-serif",
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
          <div
            style={{
              fontSize: 12,
              fontWeight: 700,
              color: "#2d3037",
              marginBottom: 2,
              lineHeight: 1.2,
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

        <span
          style={{
            flexShrink: 0,
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
            <th className="sticky top-0 z-10 bg-white px-6 py-4 text-left text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">State</th>
            <th className="sticky top-0 z-10 bg-white px-6 py-4 text-left text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Trigger</th>
            <th className="sticky top-0 z-10 bg-white px-6 py-4 text-left text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Time</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item, index) => {
            const badgeStyle = statusBadgeStyle(String(item.state || "").toLowerCase());
            return (
              <tr key={`history-row-${index}`} className="border-t border-slate-100">
                <td className="px-6 py-5 align-top text-left">
                  <span
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      borderRadius: 6,
                      background: badgeStyle.bg,
                      color: badgeStyle.text,
                      padding: "5px 10px",
                      fontSize: 11,
                      fontWeight: 800,
                      letterSpacing: "0.08em",
                      textTransform: "uppercase",
                      lineHeight: 1,
                    }}
                  >
                    {item.state}
                  </span>
                </td>
                <td className="px-6 py-5 align-top text-left text-[14px] text-slate-600">{item.trigger}</td>
                <td className="px-6 py-5 align-top text-left text-[14px] font-semibold text-slate-700">{clockLabel(item.timestamp)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function FlatNodeTable({ items, selectedId }) {
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
            <th className="sticky top-0 z-10 bg-white px-6 py-4 text-left text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Node</th>
            <th className="sticky top-0 z-10 bg-white px-6 py-4 text-left text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Status</th>
            <th className="sticky top-0 z-10 bg-white px-6 py-4 text-left text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Kind</th>
            <th className="sticky top-0 z-10 bg-white px-6 py-4 text-right text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Time</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item) => {
            const badgeStyle = statusBadgeStyle(item.currentState);
            const dotStyle = kindDotStyle(item.tone);
            const active = item.id === selectedId;
            return (
              <tr key={`node-row-${item.id}`} className={`border-t border-slate-100 ${active ? "bg-slate-50/70" : ""}`}>
                <td className="px-6 py-5 align-top">
                  <div className="min-w-0 text-left">
                    <div className="truncate text-[15px] font-semibold text-slate-950">{item.title}</div>
                    <div className="mt-1 text-[12px] uppercase tracking-[0.12em] text-slate-400">{item.id}</div>
                  </div>
                </td>
                <td className="px-6 py-5 align-top text-left">
                  <span
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      borderRadius: 6,
                      background: badgeStyle.bg,
                      color: badgeStyle.text,
                      padding: "5px 10px",
                      fontSize: 11,
                      fontWeight: 800,
                      letterSpacing: "0.08em",
                      textTransform: "uppercase",
                      lineHeight: 1,
                    }}
                  >
                    {item.displayState}
                  </span>
                </td>
                <td className="px-6 py-5 align-top text-left text-slate-600">
                  <div className="flex items-center gap-2 text-[14px]">
                    <span
                      style={{
                        width: 8,
                        height: 8,
                        borderRadius: 999,
                        background: dotStyle.bg,
                        flexShrink: 0,
                      }}
                    />
                    <span className="truncate">{item.kind}</span>
                  </div>
                </td>
                <td className="px-6 py-5 align-top text-right text-[14px] font-semibold text-slate-700">{item.duration}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function InspectBottomSheet({ open, data, onClose, activeTab, onTabChange }) {
  if (!data || !data.selected) return null;
  const { selected, upstream, downstream, history, run } = data;
  const tabItems = activeTab === "downstream" ? downstream : upstream;
  const selectedTone = kindDotStyle(selected.tone);
  const selectedBadge = statusBadgeStyle(selected.currentState);

  return (
    <div
      className={`pointer-events-none absolute inset-x-0 bottom-0 z-20 transform-gpu border-t border-slate-200 bg-slate-50/95 transition-[transform,opacity] duration-500 ease-[cubic-bezier(0.16,1,0.3,1)] ${
        open ? "translate-y-0 opacity-100" : "translate-y-[108%] opacity-0"
      }`}
      style={{ fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif" }}
    >
      <button
        type="button"
        onClick={onClose}
        className="pointer-events-auto absolute left-6 top-0 z-30 flex h-10 w-10 -translate-y-full items-center justify-center border border-b-0 border-slate-200 bg-white text-lg font-medium leading-none text-slate-500 shadow-sm transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-900"
        aria-label="Hide details"
      >
        v
      </button>

      <div className="pointer-events-auto relative mx-auto flex h-[440px] w-full max-w-[1600px] flex-col overflow-visible px-6 py-6 md:h-[460px]">
        <div className="grid h-full min-h-0 gap-6 overflow-hidden lg:grid-cols-[380px_minmax(0,1fr)]">
          <aside
            className="flex min-h-0 flex-col overflow-hidden rounded-xl border border-slate-200 bg-white shadow-[0_4px_12px_rgba(15,23,42,0.05)]"
            style={{ borderLeftWidth: 6, borderLeftColor: selectedTone.bg }}
          >
            <div className="flex items-start justify-between px-6 py-6">
              <div className="min-w-0">
                <div className="truncate text-[19px] font-bold tracking-[-0.02em] text-slate-950">{selected.title}</div>
                <div className="mt-1 flex min-w-0 items-center gap-2 text-[12px] text-slate-500">
                  <span className="shrink-0">{selected.id}</span>
                  {selected.artifactName ? (
                    <>
                      <span className="text-slate-300">•</span>
                      <span className="truncate">{selected.artifactName}</span>
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

            <div className="min-h-0 overflow-y-auto px-6 pb-6">
              <div className="mb-6 grid grid-cols-2 gap-x-10 gap-y-4">
                <div>
                  <div className="text-[11px] font-bold uppercase tracking-[0.08em] text-slate-400">Node status</div>
                  <div className="mt-1 text-[15px] font-semibold text-slate-950">{selected.currentState}</div>
                </div>
                <div>
                  <div className="text-[11px] font-bold uppercase tracking-[0.08em] text-slate-400">Cell type</div>
                  <div className="mt-1 text-[15px] font-semibold text-slate-950">{selected.kind}</div>
                </div>
              </div>

              <div className="overflow-hidden rounded-lg border border-slate-200 bg-slate-50/40">
                <div className="border-b border-slate-200 px-4 py-3">
                  <div>
                    <div className="text-[11px] font-bold uppercase tracking-[0.08em] text-slate-400">Run time</div>
                    <div className="mt-1 text-[15px] font-semibold text-slate-950">{run.duration}</div>
                  </div>
                </div>
                <div className="grid grid-cols-2 border-b border-slate-200">
                  <div className="border-r border-slate-200 px-4 py-3">
                    <div className="text-[11px] font-bold uppercase tracking-[0.08em] text-slate-400">Start time</div>
                    <div className="mt-1 text-[15px] font-semibold text-slate-950">{clockLabel(run.startedAt)}</div>
                  </div>
                  <div className="px-4 py-3">
                    <div className="text-[11px] font-bold uppercase tracking-[0.08em] text-slate-400">End time</div>
                    <div className="mt-1 text-[15px] font-semibold text-slate-950">{clockLabel(run.finishedAt)}</div>
                  </div>
                </div>
                <div className="grid grid-cols-2">
                  <div className="border-r border-slate-200 px-4 py-3">
                    <div className="text-[11px] font-bold uppercase tracking-[0.08em] text-slate-400">Downstream</div>
                    <div className="mt-1 text-[15px] font-semibold text-slate-950">{downstream.length}</div>
                  </div>
                  <div className="px-4 py-3">
                    <div className="text-[11px] font-bold uppercase tracking-[0.08em] text-slate-400">Upstream</div>
                    <div className="mt-1 text-[15px] font-semibold text-slate-950">{upstream.length}</div>
                  </div>
                </div>
              </div>
            </div>
          </aside>

          <main className="flex min-h-0 flex-col overflow-hidden rounded-xl border border-slate-200 bg-white shadow-[0_4px_12px_rgba(15,23,42,0.05)]">
            <div className="flex items-center gap-2 border-b border-slate-200 px-6 py-4">
              <button
                type="button"
                onClick={() => onTabChange("history")}
                className={`rounded-full px-4 py-2 text-sm font-semibold transition ${
                  activeTab === "history"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Node history
              </button>
              <button
                type="button"
                onClick={() => onTabChange("upstream")}
                className={`rounded-full px-4 py-2 text-sm font-semibold transition ${
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
                className={`rounded-full px-4 py-2 text-sm font-semibold transition ${
                  activeTab === "downstream"
                    ? "bg-slate-900 text-white"
                    : "text-slate-500 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                Downstream nodes
              </button>
            </div>

            <div className="min-h-0 flex-1 overflow-hidden px-2 pb-2">
              {activeTab === "history" ? (
                <HistoryTable items={history} />
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

function GraphCanvas() {
  const [selectedNodeId, setSelectedNodeId] = useState("sql_126");
  const [sheetOpen, setSheetOpen] = useState(false);
  const [activeTab, setActiveTab] = useState("history");
  const panelData = useMemo(
    () => (selectedNodeId ? buildInspectPanelData(selectedNodeId) : null),
    [selectedNodeId],
  );

  const { nodes, edges } = useMemo(() => {
    const baseNodes = Object.entries(NODE_DETAILS).map(([id, details]) => ({
      id,
      type: "flowCard",
      position: { x: 0, y: 0 },
      data: buildFlowNodeData(id, details),
      selected: id === selectedNodeId,
      draggable: false,
      selectable: true,
    }));

    const baseEdges = EDGE_LIST.map(([source, target]) => buildFlowEdge(source, target));
    return layoutElements(baseNodes, baseEdges);
  }, [selectedNodeId]);

  return (
    <div className="relative h-full w-full overflow-hidden">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodeClick={(_, node) => {
          setSelectedNodeId(String(node.id));
          setActiveTab("history");
          setSheetOpen(true);
        }}
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
        <Controls showInteractive={false} fitViewOptions={FIT_OPTIONS} />
      </ReactFlow>

      <InspectBottomSheet
        open={sheetOpen}
        data={panelData}
        onClose={() => setSheetOpen(false)}
        activeTab={activeTab}
        onTabChange={setActiveTab}
      />
    </div>
  );
}

function HeaderActionButton({ title, icon: Icon }) {
  return (
    <button
      type="button"
      title={title}
      aria-label={title}
      className="inline-flex h-10 w-10 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-500 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-900"
    >
      <Icon size={17} strokeWidth={1.9} />
    </button>
  );
}

export default function App() {
  return (
    <div className="h-screen w-full overflow-hidden bg-slate-100">
      <div className="flex h-full w-full min-h-0 flex-col bg-white">
        <header className="flex items-center justify-between border-b border-slate-200 bg-white px-6 py-4">
          <div className="flex items-center gap-8">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Pipeline ID</div>
              <div className="mt-1 text-[18px] font-semibold tracking-[-0.02em] text-slate-950">{PIPELINE_META.pipelineId}</div>
            </div>
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Node Count</div>
              <div className="mt-1 text-[18px] font-semibold tracking-[-0.02em] text-slate-950">{NODE_ORDER.length}</div>
            </div>
          </div>

          <div className="flex items-center gap-6">
            <div className="flex items-center gap-6">
              <div className="text-right">
                <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Run ID</div>
                <div className="mt-1 text-[14px] font-medium text-slate-700">{RUN_SNAPSHOT.runId}</div>
              </div>
              <div className="text-right">
                <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">Run Label</div>
                <div className="mt-1 text-[14px] font-medium text-slate-700">{RUN_SNAPSHOT.runLabel}</div>
              </div>
            </div>

            <div className="flex items-center gap-2">
              <HeaderActionButton title="Run" icon={CirclePlay} />
              <HeaderActionButton title="Resume" icon={SkipForward} />
              <HeaderActionButton title="Reset All" icon={RefreshCw} />
              <HeaderActionButton title="Reset Node" icon={RotateCcw} />
              <HeaderActionButton title="Reset Upstream" icon={History} />
            </div>
          </div>
        </header>

        <div className="min-h-0 flex-1">
        <ReactFlowProvider>
          <GraphCanvas />
        </ReactFlowProvider>
        </div>
      </div>
    </div>
  );
}
