'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { FaCode, FaChevronRight, FaExclamationTriangle, FaCheck, FaCopy, FaTrash, FaDownload, FaShieldAlt } from 'react-icons/fa';
import EtlLineageVisualizer, { type LineageMap } from '@/components/EtlLineageVisualizer';
import {
  EngineRecommendationCard,
  RelationshipsCard,
  ManyToManyCard,
  OverallReadinessBanner,
  StepEvidenceTooltip,
  applyEngineRecommendation,
  getPlanFromRecord,
  getStepNarration,
  type EngineRecommendation,
  type StepEvidence,
} from '@/components/EtlIntelligencePreview';
import ManualReviewPanel, { type ManualReviewItem } from '@/components/ManualReviewPanel';

type Step = 'rules' | 'plan' | 'preview' | 'code';

export type EtlEngine = 'python' | 'sql' | 'spark' | 'adf';

type StepRow = {
  id: string;
  dataset: string;
  order: number;
  column: string | null;
  action: string;
  bucket?: string;
};

function bucketBadgeClass(bucket: string | undefined, dm: boolean): string {
  const b = (bucket || 'auto').toLowerCase();
  if (b === 'blocked') return dm ? 'bg-rose-500/30 text-rose-100' : 'bg-rose-100 text-rose-900';
  if (b === 'review') return dm ? 'bg-amber-500/30 text-amber-100' : 'bg-amber-100 text-amber-900';
  return dm ? 'bg-emerald-500/25 text-emerald-100' : 'bg-emerald-100 text-emerald-900';
}

function planToRows(plan: Record<string, unknown>): StepRow[] {
  const dsPlan = (plan.datasets || {}) as Record<string, { steps?: Record<string, unknown>[] }>;
  const out: StepRow[] = [];
  for (const [ds, block] of Object.entries(dsPlan)) {
    for (const st of block.steps || []) {
      const order = Number(st.order ?? 0);
      const col = (st.column as string | null | undefined) ?? null;
      const action = String(st.action ?? '');
      out.push({
        id: `${ds}|${order}|${col}|${action}`,
        dataset: ds,
        order,
        column: col,
        action,
        bucket: typeof st.bucket === 'string' ? st.bucket : 'auto',
      });
    }
  }
  return out.sort((a, b) => a.dataset.localeCompare(b.dataset) || a.order - b.order);
}

function rowsToPlan(base: Record<string, unknown>, rows: StepRow[]): Record<string, unknown> {
  const byDs: Record<string, StepRow[]> = {};
  for (const r of rows) {
    if (!byDs[r.dataset]) byDs[r.dataset] = [];
    byDs[r.dataset].push(r);
  }
  const datasets: Record<string, { steps: Record<string, unknown>[] }> = {};
  for (const [ds, list] of Object.entries(byDs)) {
    const sorted = [...list].sort((a, b) => a.order - b.order);
    datasets[ds] = {
      steps: sorted.map((r, i) => ({
        order: i + 1,
        column: r.column,
        action: r.action,
        bucket: r.bucket || 'auto',
      })),
    };
  }
  return { ...base, datasets };
}

function parseCodegenEngine(raw: string | undefined): EtlEngine {
  const e = (raw || 'python').toLowerCase();
  if (e === 'sql' || e === 'ansi' || e === 'tsql') return 'sql';
  if (e === 'spark' || e === 'pyspark') return 'spark';
  if (e === 'adf') return 'adf';
  return 'python';
}

export type EtlPipelineMode = 'full' | 'requirements' | 'etl';

export interface EtlGenerationPanelProps {
  sessionId: string;
  assessment: Record<string, unknown> | null;
  /** pipeline = light theme; chat = can use darkMode */
  variant?: 'pipeline' | 'chat';
  darkMode?: boolean;
  /** Split data-pipeline UX: rules+plan vs preview+code vs full (chat). */
  pipelineMode?: EtlPipelineMode;
  onContinueToEtlStep?: () => void;
  /** From ETL step: go back to Requirements to edit rules/plan. */
  onEditPlanInRequirements?: () => void;
  onCodeGenerated?: (code: string) => void;
  onContinueAfterCode?: () => void;
}

export default function EtlGenerationPanel({
  sessionId,
  assessment,
  variant = 'pipeline',
  darkMode = false,
  pipelineMode = 'full',
  onContinueToEtlStep,
  onEditPlanInRequirements,
  onCodeGenerated,
  onContinueAfterCode,
}: EtlGenerationPanelProps) {
  const dm = darkMode && variant === 'chat';
  const shell = dm
    ? 'rounded-3xl border border-emerald-500/30 bg-[#001a2e]/90 p-6 shadow-sm text-zinc-100'
    : 'rounded-3xl border border-emerald-500/30 bg-gradient-to-br from-emerald-50/80 to-white/90 p-6 shadow-sm';
  const sub = dm ? 'text-emerald-200/80' : 'text-black/55';
  const label = dm ? 'text-emerald-100/70' : 'text-black/45';
  const field = dm
    ? 'rounded-xl border border-white/10 bg-black/20 px-3 py-2 text-sm text-zinc-100 placeholder:text-white/30'
    : 'rounded-xl border border-black/10 bg-white px-3 py-2 text-sm text-zinc-900';

  const [step, setStep] = useState<Step>(() => (pipelineMode === 'etl' ? 'preview' : 'rules'));
  const [etlSessionLoading, setEtlSessionLoading] = useState(pipelineMode === 'etl');
  const [engine, setEngine] = useState<EtlEngine>('python');
  const [engineUserOverride, setEngineUserOverride] = useState(false);
  const [sqlDialect, setSqlDialect] = useState<'tsql' | 'ansi'>('tsql');
  const [neverDropRows, setNeverDropRows] = useState(false);
  const [targetDestination, setTargetDestination] = useState<'dataframe_only' | 'new_path' | 'overwrite'>(
    'dataframe_only'
  );
  const [targetPath, setTargetPath] = useState('cleaned/');
  const [lineage, setLineage] = useState<Record<string, unknown> | null>(null);
  const [planValidationErrors, setPlanValidationErrors] = useState<string[]>([]);
  const [tenantId, setTenantId] = useState('default');
  const [tenantOptions, setTenantOptions] = useState<string[]>(['default', 'acme']);
  const [gxCheckpoint, setGxCheckpoint] = useState<Record<string, unknown> | null>(null);
  const [gxBusy, setGxBusy] = useState(false);
  const [requiredColumns, setRequiredColumns] = useState('');
  const [excludeColumns, setExcludeColumns] = useState('');
  const [notes, setNotes] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [plan, setPlan] = useState<Record<string, unknown> | null>(null);
  const [planJson, setPlanJson] = useState('');
  const [planTab, setPlanTab] = useState<'table' | 'json'>('table');
  const [planRows, setPlanRows] = useState<StepRow[]>([]);
  const [preview, setPreview] = useState<Record<string, unknown> | null>(null);
  const [code, setCode] = useState('');
  const [validationOk, setValidationOk] = useState<boolean | null>(null);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [artifactPath, setArtifactPath] = useState<string | null>(null);
  const [generatedBy, setGeneratedBy] = useState<string | null>(null);
  const [isDraft, setIsDraft] = useState(false);
  const [copyStatus, setCopyStatus] = useState<'idle' | 'copied' | 'failed'>('idle');
  const [useAiCodegen, setUseAiCodegen] = useState(false);
  const [generateStatus, setGenerateStatus] = useState<string | null>(null);
  const codeTextareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (plan) {
      setPlanJson(JSON.stringify(plan, null, 2));
      setPlanRows(planToRows(plan));
    }
  }, [plan]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch('/api/etl/tenants');
        const data = await res.json().catch(() => null);
        if (cancelled || !res.ok || !data?.ok) return;
        const ids = Array.isArray(data.tenants) ? data.tenants.filter((t: unknown) => typeof t === 'string') : [];
        if (ids.length > 0) setTenantOptions(ids);
      } catch {
        /* keep defaults */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const businessRulesPayload = useCallback(() => {
    const req = requiredColumns
      .split(/[\n,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    const excl = excludeColumns
      .split(/[\n,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    return {
      never_drop_rows: neverDropRows,
      required_columns: req,
      exclude_columns: excl,
      notes: notes.trim(),
    };
  }, [neverDropRows, requiredColumns, excludeColumns, notes]);

  const stepBadges = useMemo(() => {
    if (pipelineMode === 'requirements') return ['rules', 'plan'] as const;
    if (pipelineMode === 'etl') return ['preview', 'code'] as const;
    return ['rules', 'plan', 'preview', 'code'] as const;
  }, [pipelineMode]);

  const badgeLabel = (s: string) => {
    const map: Record<string, string> = {
      rules: 'RULES',
      plan: 'PLAN',
      preview: 'PREVIEW',
      code: 'CODE',
    };
    return map[s] || s.toUpperCase();
  };

  useEffect(() => {
    if (pipelineMode !== 'etl') {
      setEtlSessionLoading(false);
      return;
    }
    let cancelled = false;
    setEtlSessionLoading(true);
    setErr(null);
    (async () => {
      try {
        const res = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}`);
        const data = await res.json().catch(() => null);
        if (cancelled) return;
        const flow = data?.session?.context?.etl_flow;
        if (!flow?.approved_plan) {
          setErr(
            'Complete ETL rules and plan on the Requirements step first (confirm the plan to approve it).',
          );
          setEtlSessionLoading(false);
          return;
        }
        setPlan(flow.approved_plan as Record<string, unknown>);
        if (flow.preview) setPreview(flow.preview as Record<string, unknown>);
        if (flow.lineage) setLineage(flow.lineage as Record<string, unknown>);
        const ce = parseCodegenEngine(flow.codegen_engine ?? flow.target_engine);
        setEngine(ce);
        const sd = flow.sql_dialect;
        if (sd === 'ansi' || sd === 'tsql') setSqlDialect(sd);
        if (typeof flow.code === 'string' && flow.code.trim().length > 0) {
          setCode(flow.code);
          setValidationOk(flow.validation_ok != null ? Boolean(flow.validation_ok) : null);
          setValidationErrors(Array.isArray(flow.validation_errors) ? flow.validation_errors : []);
          setArtifactPath(typeof flow.artifact_rel_path === 'string' ? flow.artifact_rel_path : null);
          setStep('code');
        } else {
          setStep('preview');
        }
      } catch (e: unknown) {
        if (!cancelled) setErr(e instanceof Error ? e.message : 'Failed to load session');
      } finally {
        if (!cancelled) setEtlSessionLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [pipelineMode, sessionId]);

  const runPlan = async () => {
    if (!assessment) {
      setErr('No assessment loaded yet.');
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      const planEngine = engine === 'spark' || engine === 'adf' ? 'python' : engine;
      const res = await fetch('/api/etl/plan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: sessionId,
          business_rules: businessRulesPayload(),
          assessment_result: assessment,
          engine: planEngine,
          codegen_engine: engine,
          sql_dialect: sqlDialect,
          target_destination: targetDestination,
          target_path: targetDestination === 'new_path' ? targetPath : undefined,
          tenant_id: tenantId,
          engine_user_override: engineUserOverride,
        }),
      });
      const data = await res.json().catch(() => null);
      const blocked = Array.isArray(data?.blocked) ? data.blocked : [];
      const builtPlan = (data?.plan || null) as Record<string, unknown> | null;
      if (!res.ok) {
        setErr(data?.message || data?.error || `Plan failed (${res.status})`);
        return;
      }
      if (!data?.ok) {
        const perrs = Array.isArray(data.plan_validation_errors) ? data.plan_validation_errors : [];
        setPlanValidationErrors(perrs);
        if (builtPlan) setPlan(builtPlan);
        setErr(
          data?.message ||
            (blocked.length
              ? `Blocked: ${blocked.map((b: { message?: string }) => b.message || JSON.stringify(b)).join(' | ')}`
              : `Plan has validation warnings (${perrs.length}). Review before confirming.`)
        );
        setStep('plan');
        return;
      }
      if (blocked.length > 0) {
        setErr(`Blocked: ${blocked.map((b: { message?: string }) => b.message || JSON.stringify(b)).join(' | ')}`);
        setPlan(builtPlan);
        setStep('plan');
        return;
      }
      setPlan(builtPlan);
      if (!engineUserOverride) {
        const rec =
          (data.engine_recommendation as EngineRecommendation | undefined) ||
          (builtPlan?.engine_recommendation as EngineRecommendation | undefined);
        const applied = applyEngineRecommendation(rec);
        if (applied) {
          setEngine(applied.engine);
          if (applied.sqlDialect) setSqlDialect(applied.sqlDialect);
        } else if (typeof data.recommended_codegen_engine === 'string') {
          setEngine(parseCodegenEngine(data.recommended_codegen_engine));
          if (data.recommended_sql_dialect === 'ansi' || data.recommended_sql_dialect === 'tsql') {
            setSqlDialect(data.recommended_sql_dialect);
          }
        }
      }
      setPlanValidationErrors(
        Array.isArray(data.plan_validation_errors) ? data.plan_validation_errors : []
      );
      setStep('plan');
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : 'Plan request failed');
    } finally {
      setBusy(false);
    }
  };

  const applyJsonPlan = () => {
    setErr(null);
    try {
      const parsed = JSON.parse(planJson) as Record<string, unknown>;
      if (!parsed.datasets || typeof parsed.datasets !== 'object') {
        setErr('Invalid plan: missing datasets object');
        return;
      }
      setPlan(parsed);
    } catch {
      setErr('Invalid JSON — fix syntax and try again');
    }
  };

  const removePlanRow = (id: string) => {
    if (!plan) return;
    const next = planRows.filter((r) => r.id !== id);
    setPlanRows(next);
    setPlan(rowsToPlan(plan, next));
  };

  const manualReviewItems = useMemo((): ManualReviewItem[] => {
    const raw = (plan as { manual_review?: unknown[] } | null)?.manual_review;
    if (!Array.isArray(raw)) return [];
    return raw.filter((m): m is ManualReviewItem => typeof m === 'object' && m !== null && 'id' in m);
  }, [plan]);

  const pendingManualCount = useMemo(
    () => manualReviewItems.filter((m) => (m.status || 'pending') === 'pending').length,
    [manualReviewItems]
  );

  const applyManualResolutions = async (
    resolutions: Array<{ item_id: string; resolution_id: string }>
  ) => {
    setBusy(true);
    setErr(null);
    let bodyPlan: Record<string, unknown> | undefined = plan ?? undefined;
    if (planTab === 'json' && planJson) {
      try {
        bodyPlan = JSON.parse(planJson) as Record<string, unknown>;
      } catch {
        setErr('Invalid plan JSON — fix or switch to table view');
        setBusy(false);
        return;
      }
    }
    try {
      const res = await fetch('/api/etl/apply-manual-resolutions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: sessionId,
          plan: bodyPlan,
          resolutions,
        }),
      });
      const data = await res.json().catch(() => null);
      if (!res.ok && !data?.plan) {
        setErr(data?.message || data?.error || `Apply resolutions failed (${res.status})`);
        return;
      }
      if (data?.plan) setPlan(data.plan as Record<string, unknown>);
      setPlanValidationErrors(
        Array.isArray(data?.plan_validation_errors) ? data.plan_validation_errors : []
      );
      if (data?.pending_manual_review > 0) {
        setErr(
          data?.message ||
            `${data.pending_manual_review} manual review item(s) still pending — resolve all before confirm.`
        );
      } else if (Array.isArray(data?.errors) && data.errors.length > 0) {
        setErr(data.errors.join(' | '));
      } else {
        setErr(null);
      }
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : 'Apply resolutions failed');
    } finally {
      setBusy(false);
    }
  };

  const runConfirm = async () => {
    if (pendingManualCount > 0) {
      setErr(`Resolve ${pendingManualCount} manual review item(s) below before confirming.`);
      return;
    }
    setBusy(true);
    setErr(null);
    let bodyPlan: Record<string, unknown> | undefined = plan ?? undefined;
    if (planTab === 'json') {
      try {
        bodyPlan = JSON.parse(planJson) as Record<string, unknown>;
      } catch {
        setErr('Invalid plan JSON — fix or switch to table view');
        setBusy(false);
        return;
      }
    }
    try {
      const res = await fetch('/api/etl/confirm', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: sessionId, plan: bodyPlan ?? undefined }),
      });
      const data = await res.json().catch(() => null);
      if (!res.ok || !data?.ok) {
        const errs = data?.plan_validation_errors ?? data?.detail?.plan_validation_errors;
        if (Array.isArray(errs) && errs.length > 0) {
          setPlanValidationErrors(errs);
        }
        setErr(data?.message || data?.error || data?.detail?.message || `Confirm failed (${res.status})`);
        return;
      }
      setPreview((data.preview as Record<string, unknown>) || null);
      if (data.lineage) setLineage(data.lineage as Record<string, unknown>);
      if (data.approved_plan) setPlan(data.approved_plan as Record<string, unknown>);
      if (pipelineMode === 'requirements') {
        onContinueToEtlStep?.();
        return;
      }
      setStep('preview');
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : 'Confirm request failed');
    } finally {
      setBusy(false);
    }
  };

  const runGxCheckpoint = async () => {
    setGxBusy(true);
    setErr(null);
    try {
      const res = await fetch('/api/etl/gx-checkpoint', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: sessionId, run_gx_if_available: true }),
      });
      const data = await res.json().catch(() => null);
      if (!res.ok || !data?.ok) {
        setErr(data?.message || data?.error || `GX checkpoint failed (${res.status})`);
        return;
      }
      setGxCheckpoint((data.checkpoint as Record<string, unknown>) || null);
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : 'GX checkpoint request failed');
    } finally {
      setGxBusy(false);
    }
  };

  const runGenerate = async () => {
    setBusy(true);
    setErr(null);
    const codegenMode = useAiCodegen ? 'llm_then_template' : 'template';
    setGenerateStatus(
      useAiCodegen
        ? 'Calling AI to generate code (may take 30–90 seconds)…'
        : 'Generating production template code (usually a few seconds)…'
    );
    try {
      const eng =
        engine === 'spark'
          ? 'pyspark'
          : engine === 'sql'
            ? sqlDialect === 'ansi'
              ? 'ansi'
              : 'sql'
            : engine;
      const res = await fetch('/api/etl/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: sessionId,
          engine: eng,
          sql_dialect: sqlDialect,
          codegen_mode: codegenMode,
          run_gx_on_generate: !useAiCodegen,
        }),
      });
      const data = await res.json().catch(() => null);
      if (res.status === 409) {
        const detail = data?.detail ?? data;
        setErr(
          detail?.message ||
            'Plan not approved. Go back to Requirements and confirm the plan before generating code.',
        );
        return;
      }
      if (!res.ok && !data?.code) {
        setErr(data?.message || data?.error || `Generate failed (${res.status})`);
        return;
      }
      setCode(String(data?.code || ''));
      onCodeGenerated?.(String(data?.code || ''));
      setValidationOk(Boolean(data?.validation_ok));
      setValidationErrors(Array.isArray(data?.validation_errors) ? data.validation_errors : []);
      setArtifactPath(typeof data?.artifact_rel_path === 'string' ? data.artifact_rel_path : null);
      setGeneratedBy(typeof data?.generated_by === 'string' ? data.generated_by : null);
      setIsDraft(Boolean(data?.is_draft ?? !data?.validation_ok));
      if (data?.gx_checkpoint) setGxCheckpoint(data.gx_checkpoint as Record<string, unknown>);
      if (!data?.ok) {
        setErr(data?.message || 'Code saved as draft — fix validation errors before deploy.');
      }
      if (typeof data?.latency_ms === 'number') {
        setGenerateStatus(
          `Done in ${(data.latency_ms / 1000).toFixed(1)}s via ${data.codegen_mode || data.generated_by || 'template'}`
        );
      }
      setStep('code');
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : 'Generate request failed');
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    setCopyStatus('idle');
  }, [code]);

  const copyCode = useCallback(async () => {
    const text = code.trim();
    if (!text) return;
    try {
      if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(code);
      } else {
        const ta = codeTextareaRef.current;
        if (!ta) throw new Error('Clipboard unavailable');
        ta.focus();
        ta.select();
        const ok = document.execCommand('copy');
        if (!ok) throw new Error('Copy not supported in this browser');
      }
      setCopyStatus('copied');
      window.setTimeout(() => setCopyStatus('idle'), 2000);
    } catch {
      setCopyStatus('failed');
      window.setTimeout(() => setCopyStatus('idle'), 3500);
      try {
        const ta = codeTextareaRef.current;
        if (ta) {
          ta.focus();
          ta.select();
        }
      } catch {
        /* ignore */
      }
    }
  }, [code]);

  const downloadCode = useCallback(async () => {
    const text = code.trim();
    if (!text) return;
    if (validationOk && !isDraft) {
      try {
        const res = await fetch(
          `/api/etl/download?session_id=${encodeURIComponent(sessionId)}`
        );
        if (res.ok) {
          const blob = await res.blob();
          const disposition = res.headers.get('content-disposition') ?? '';
          const match = disposition.match(/filename="?([^";]+)"?/i);
          const name = match?.[1] ?? `dhara_etl_${sessionId}.py`;
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url;
          a.download = name;
          a.rel = 'noopener';
          document.body.appendChild(a);
          a.click();
          a.remove();
          URL.revokeObjectURL(url);
          return;
        }
      } catch {
        /* fall through to client download */
      }
    }
    const ext = engine === 'sql' ? 'sql' : engine === 'adf' ? 'json' : 'py';
    const rawId = (plan as { plan_id?: string })?.plan_id ?? sessionId;
    const pid = String(rawId).replace(/[^\w.-]+/g, '_').slice(0, 48) || 'export';
    const name = `dhara_etl_${pid}${isDraft ? '_DRAFT' : ''}.${ext}`;
    const mime = ext === 'json' ? 'application/json;charset=utf-8' : 'text/plain;charset=utf-8';
    const blob = new Blob([code], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    a.rel = 'noopener';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }, [code, engine, plan, sessionId, validationOk, isDraft]);

  const selectAllCode = useCallback(() => {
    const ta = codeTextareaRef.current;
    if (!ta) return;
    ta.focus();
    ta.select();
  }, []);

  const genLabel =
    engine === 'python'
      ? 'Python (pandas)'
      : engine === 'sql'
        ? `SQL (${sqlDialect})`
        : engine === 'spark'
          ? 'PySpark'
          : 'ADF JSON';

  if (!assessment && pipelineMode !== 'etl') return null;

  if (pipelineMode === 'etl' && etlSessionLoading) {
    return (
      <div className={shell}>
        <div className="mb-4 flex items-center gap-3">
          <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-emerald-600 text-white shadow-md">
            <FaCode className="text-lg" />
          </div>
          <div>
            <h3 className={`text-lg font-black tracking-tight ${dm ? 'text-white' : 'text-zinc-900'}`}>ETL preview & code</h3>
            <p className={`text-[12.5px] font-medium ${sub}`}>Loading saved plan and preview…</p>
          </div>
        </div>
      </div>
    );
  }

  const heading =
    pipelineMode === 'requirements'
      ? 'ETL rules & plan'
      : pipelineMode === 'etl'
        ? 'ETL preview & code'
        : 'ETL code generation';

  const flowSubtitle =
    pipelineMode === 'requirements'
      ? 'Target engine, column rules, and notes → editable plan'
      : pipelineMode === 'etl'
        ? `Impact preview → generate ${genLabel}`
        : `Business rules → plan (edit) → impact preview → ${genLabel}`;

  const confirmPlanLabel =
    pipelineMode === 'requirements' ? 'Save plan & go to ETL code' : 'Agree & preview impact';

  const { engineRecommendation, narration: planNarration, relationships: planRelationships } =
    getPlanFromRecord(plan);

  const planApproveReady = useMemo(() => {
    if (!plan) return false;
    if (Array.isArray(plan.blocked) && (plan.blocked as unknown[]).length > 0) return false;
    if (pendingManualCount > 0) return false;
    const dsPlan = (plan.datasets || {}) as Record<
      string,
      { steps?: Array<{ classification?: string; bucket?: string; requires_user_choice?: boolean }> }
    >;
    for (const block of Object.values(dsPlan)) {
      for (const st of block.steps || []) {
        const cls = String(st.classification || st.bucket || 'auto').toLowerCase();
        if (cls === 'blocked') return false;
        if (cls === 'review' && st.requires_user_choice) return false;
      }
    }
    return true;
  }, [plan, pendingManualCount]);

  const useRecommendedEngine = () => {
    const applied = applyEngineRecommendation(engineRecommendation);
    if (applied) {
      setEngineUserOverride(false);
      setEngine(applied.engine);
      if (applied.sqlDialect) setSqlDialect(applied.sqlDialect);
    }
  };

  const getStepMeta = (dataset: string, order: number) => {
    const dsBlock = (plan?.datasets as Record<string, { steps?: Record<string, unknown>[] }> | undefined)?.[
      dataset
    ];
    return (dsBlock?.steps || []).find((s) => Number(s.order) === order);
  };

  return (
    <div className={shell}>
      <div className="mb-4 flex items-center gap-3">
        <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-emerald-600 text-white shadow-md">
          <FaCode className="text-lg" />
        </div>
        <div>
          <h3 className={`text-lg font-black tracking-tight ${dm ? 'text-white' : 'text-zinc-900'}`}>{heading}</h3>
          <p className={`text-[12.5px] font-medium ${sub}`}>{flowSubtitle}</p>
        </div>
      </div>

      <div className="mb-4 flex flex-wrap gap-2 text-[10px] font-black uppercase tracking-widest">
        {stepBadges.map((s) => (
          <span
            key={s}
            className={`rounded-full px-3 py-1 ${
              step === s
                ? 'bg-emerald-600 text-white'
                : dm
                  ? 'bg-white/10 text-white/50'
                  : 'bg-black/5 text-black/40'
            }`}
          >
            {badgeLabel(s)}
          </span>
        ))}
      </div>

      <AnimatePresence mode="wait">
        {(pipelineMode === 'full' || pipelineMode === 'requirements') && step === 'rules' && (
          <motion.div
            key="rules"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            className="space-y-4"
          >
            <div>
              <div className={`mb-1 text-[11px] font-black uppercase tracking-widest ${label}`}>Target engine</div>
              <div className="flex flex-wrap gap-2">
                {(
                  [
                    ['python', 'Python'],
                    ['sql', 'SQL'],
                    ['spark', 'PySpark'],
                    ['adf', 'ADF'],
                  ] as const
                ).map(([k, lab]) => (
                  <button
                    key={k}
                    type="button"
                    onClick={() => {
                      setEngineUserOverride(true);
                      setEngine(k);
                    }}
                    className={`rounded-lg px-3 py-1.5 text-xs font-bold ${
                      engine === k
                        ? 'bg-emerald-600 text-white'
                        : dm
                          ? 'bg-white/10 text-white/80'
                          : 'bg-black/5 text-zinc-700'
                    }`}
                  >
                    {lab}
                  </button>
                ))}
              </div>
            </div>
            {engine === 'sql' ? (
              <div>
                <div className={`mb-1 text-[11px] font-black uppercase tracking-widest ${label}`}>SQL dialect</div>
                <select
                  value={sqlDialect}
                  onChange={(e) => setSqlDialect(e.target.value as 'tsql' | 'ansi')}
                  className={field}
                >
                  <option value="tsql">T-SQL (Azure SQL / SQL Server)</option>
                  <option value="ansi">ANSI (portable comments / casts)</option>
                </select>
              </div>
            ) : null}

            <div>
              <div className={`mb-1 text-[11px] font-black uppercase tracking-widest ${label}`}>
                Rule set (tenant)
              </div>
              <select value={tenantId} onChange={(e) => setTenantId(e.target.value)} className={`mb-3 w-full ${field}`}>
                {tenantOptions.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <div className={`mb-1 text-[11px] font-black uppercase tracking-widest ${label}`}>
                Output destination
              </div>
              <select
                value={targetDestination}
                onChange={(e) =>
                  setTargetDestination(e.target.value as 'dataframe_only' | 'new_path' | 'overwrite')
                }
                className={field}
              >
                <option value="dataframe_only">Return DataFrame only (notebook / library use)</option>
                <option value="new_path">Write to new path</option>
                <option value="overwrite">Overwrite source (in-place)</option>
              </select>
              {targetDestination === 'new_path' ? (
                <input
                  type="text"
                  value={targetPath}
                  onChange={(e) => setTargetPath(e.target.value)}
                  placeholder="cleaned/"
                  className={`mt-2 w-full ${field}`}
                />
              ) : null}
            </div>

            <label className="flex items-start gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={neverDropRows}
                onChange={(e) => setNeverDropRows(e.target.checked)}
                className="mt-1 h-4 w-4 rounded border-black/20"
              />
              <span className={`text-sm ${dm ? 'text-zinc-200' : 'text-zinc-800'}`}>
                <span className="font-bold">Never drop rows</span> — prefer fills over row-dropping transforms when
                applicable.
              </span>
            </label>
            <div>
              <div className={`mb-1 text-[11px] font-black uppercase tracking-widest ${label}`}>Required columns</div>
              <textarea
                value={requiredColumns}
                onChange={(e) => setRequiredColumns(e.target.value)}
                placeholder="e.g. customer_id, email (comma or newline separated)"
                rows={2}
                className={`w-full ${field}`}
              />
            </div>
            <div>
              <div className={`mb-1 text-[11px] font-black uppercase tracking-widest ${label}`}>Exclude columns</div>
              <textarea
                value={excludeColumns}
                onChange={(e) => setExcludeColumns(e.target.value)}
                placeholder="Columns to leave untouched"
                rows={2}
                className={`w-full ${field}`}
              />
            </div>
            <div>
              <div className={`mb-1 text-[11px] font-black uppercase tracking-widest ${label}`}>Business notes</div>
              <textarea
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                placeholder="Constraints, definitions, or handoff notes for engineers reviewing this ETL."
                rows={3}
                className={`w-full ${field}`}
              />
            </div>
            <button
              type="button"
              disabled={busy}
              onClick={() => void runPlan()}
              className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-4 py-2.5 text-sm font-bold text-white shadow hover:bg-emerald-700 disabled:opacity-50"
            >
              Build ETL plan <FaChevronRight className="text-xs" />
            </button>
          </motion.div>
        )}

        {(pipelineMode === 'full' || pipelineMode === 'requirements') && step === 'plan' && plan && (
          <motion.div
            key="plan"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            className="space-y-3"
          >
            <p className={`text-sm ${dm ? 'text-zinc-300' : 'text-black/60'}`}>
              Edit steps in the table, or switch to JSON. Each step shows evidence from your assessment. Resolve any
              manual review items, then confirm to run impact preview before code generation.
            </p>
            {engineRecommendation ? (
              <EngineRecommendationCard
                rec={engineRecommendation}
                narration={planNarration?.engine_explanation}
                darkMode={dm}
                currentEngine={engine}
                onUseRecommendation={useRecommendedEngine}
              />
            ) : null}
            <OverallReadinessBanner narration={planNarration || null} plan={plan} darkMode={dm} />
            <ManualReviewPanel
              items={manualReviewItems}
              darkMode={dm}
              busy={busy}
              onApply={applyManualResolutions}
            />
            <RelationshipsCard relationships={planRelationships} darkMode={dm} />
            <ManyToManyCard
              relationships={planRelationships}
              narration={planNarration?.relationships_summary}
              darkMode={dm}
            />
            {planValidationErrors.length > 0 ? (
              <div
                className={`rounded-xl border px-3 py-2 text-[12px] ${
                  dm ? 'border-amber-400/40 bg-amber-500/10 text-amber-100' : 'border-amber-200 bg-amber-50 text-amber-950'
                }`}
              >
                <strong>Plan validation notes</strong>
                <ul className="mt-1 list-disc pl-4">
                  {planValidationErrors.map((e, i) => (
                    <li key={i}>{e}</li>
                  ))}
                </ul>
              </div>
            ) : null}
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => setPlanTab('table')}
                className={`rounded-lg px-3 py-1 text-xs font-bold ${planTab === 'table' ? 'bg-emerald-600 text-white' : dm ? 'bg-white/10' : 'bg-black/5'}`}
              >
                Table
              </button>
              <button
                type="button"
                onClick={() => setPlanTab('json')}
                className={`rounded-lg px-3 py-1 text-xs font-bold ${planTab === 'json' ? 'bg-emerald-600 text-white' : dm ? 'bg-white/10' : 'bg-black/5'}`}
              >
                JSON
              </button>
            </div>

            {planTab === 'table' ? (
              <div className="space-y-2">
                {planRows.length === 0 ? (
                  <div
                    className={`rounded-xl border px-3 py-2 text-[12px] leading-relaxed ${
                      dm ? 'border-amber-400/30 bg-amber-500/10 text-amber-100' : 'border-amber-200 bg-amber-50 text-amber-950'
                    }`}
                  >
                    <strong>No automatic steps yet</strong> — use the{' '}
                    <strong>Manual review</strong> panel above to pick how Dhara should handle each flagged issue,
                    then click <strong>Apply selections to plan</strong>. Steps will appear here after you apply.
                  </div>
                ) : null}
                <div className={`max-h-64 overflow-auto rounded-xl border ${dm ? 'border-white/10' : 'border-black/10'}`}>
                <table className="w-full text-left text-[11px]">
                  <thead className={dm ? 'bg-white/10' : 'bg-black/[0.04]'}>
                    <tr>
                      <th className="p-2 font-black uppercase tracking-tighter">Ds</th>
                      <th className="p-2 font-black uppercase tracking-tighter">#</th>
                      <th className="p-2 font-black uppercase tracking-tighter">Col</th>
                      <th className="p-2 font-black uppercase tracking-tighter">Action</th>
                      <th className="p-2 font-black uppercase tracking-tighter">Type</th>
                      <th className="p-2 font-black uppercase tracking-tighter">Why</th>
                      <th className="p-2 font-black uppercase tracking-tighter">Risk</th>
                      <th className="p-2 font-black uppercase tracking-tighter">Rows</th>
                      <th className="p-2" />
                    </tr>
                  </thead>
                  <tbody>
                    {planRows.map((r) => {
                      const stMeta = getStepMeta(r.dataset, r.order);
                      const evProfile = stMeta?.evidence_profile as StepEvidence | undefined;
                      const evText =
                        typeof stMeta?.evidence === 'string'
                          ? stMeta.evidence
                          : (stMeta?.reason as string | undefined);
                      const risk = String(stMeta?.risk || 'medium');
                      const rowImpact = String(stMeta?.row_impact || 'none');
                      return (
                      <tr key={r.id} className={dm ? 'border-t border-white/5' : 'border-t border-black/5'}>
                        <td className="p-2 font-mono">{r.dataset}</td>
                        <td className="p-2">{r.order}</td>
                        <td className="p-2 font-mono">{r.column ?? '—'}</td>
                        <td className="p-2 font-mono">{r.action}</td>
                        <td className="p-2">
                          <span
                            className={`rounded-full px-2 py-0.5 text-[9px] font-black uppercase ${bucketBadgeClass(r.bucket, dm)}`}
                          >
                            {r.bucket || 'auto'}
                          </span>
                        </td>
                        <td className="p-2 max-w-[140px]">
                          <span className={`line-clamp-2 text-[10px] ${dm ? 'text-zinc-300' : 'text-zinc-700'}`}>
                            {(stMeta?.reason as string) || evText || '—'}
                          </span>
                          {evProfile || evText ? (
                            <StepEvidenceTooltip
                              evidence={
                                evProfile ||
                                ({
                                  why_this_action: evText || String(stMeta?.reason || ''),
                                } as StepEvidence)
                              }
                              bucket={r.bucket || 'auto'}
                              darkMode={dm}
                              narration={getStepNarration(plan, r.dataset, r.order)}
                            />
                          ) : null}
                        </td>
                        <td className="p-2">
                          <span
                            className={`rounded-full px-2 py-0.5 text-[9px] font-black uppercase ${
                              risk === 'high'
                                ? dm
                                  ? 'bg-rose-500/30 text-rose-100'
                                  : 'bg-rose-100 text-rose-900'
                                : risk === 'low'
                                  ? dm
                                    ? 'bg-emerald-500/25 text-emerald-100'
                                    : 'bg-emerald-100 text-emerald-800'
                                  : dm
                                    ? 'bg-amber-500/30 text-amber-100'
                                    : 'bg-amber-100 text-amber-900'
                            }`}
                          >
                            {risk}
                          </span>
                        </td>
                        <td className="p-2 font-mono text-[10px]">{rowImpact}</td>
                        <td className="p-2 text-right">
                          <button
                            type="button"
                            title="Remove step"
                            onClick={() => removePlanRow(r.id)}
                            className="rounded p-1 text-rose-500 hover:bg-rose-500/10"
                          >
                            <FaTrash className="text-xs" />
                          </button>
                        </td>
                      </tr>
                    );
                    })}
                  </tbody>
                </table>
                </div>
              </div>
            ) : (
              <textarea
                value={planJson}
                onChange={(e) => setPlanJson(e.target.value)}
                rows={14}
                className={`w-full font-mono text-[11px] ${field}`}
              />
            )}

            <div className="flex flex-wrap gap-2">
              {planTab === 'json' ? (
                <button
                  type="button"
                  onClick={() => applyJsonPlan()}
                  className={`rounded-xl px-4 py-2 text-sm font-semibold ${dm ? 'bg-white/15 text-white' : 'border border-black/10 bg-white text-zinc-800'}`}
                >
                  Apply JSON to plan
                </button>
              ) : null}
              <button
                type="button"
                disabled={busy}
                onClick={() => setStep('rules')}
                className={`rounded-xl px-4 py-2 text-sm font-semibold ${dm ? 'bg-white/10 text-white' : 'border border-black/10 bg-white text-zinc-800'}`}
              >
                Back
              </button>
              <button
                type="button"
                disabled={busy || !planApproveReady}
                title={
                  !planApproveReady
                    ? pendingManualCount > 0
                      ? `Resolve ${pendingManualCount} manual review item(s) first`
                      : 'Resolve blocked or review steps before approving'
                    : undefined
                }
                onClick={() => void runConfirm()}
                className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-4 py-2 text-sm font-bold text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                {planApproveReady ? 'Approve & preview impact' : confirmPlanLabel}
                {pendingManualCount > 0 ? ` (${pendingManualCount} review pending)` : null}{' '}
                <FaChevronRight className="text-xs" />
              </button>
            </div>
          </motion.div>
        )}

        {(pipelineMode === 'full' || pipelineMode === 'etl') && step === 'preview' && preview && (
          <motion.div
            key="preview"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            className="space-y-3"
          >
            {engineRecommendation ? (
              <EngineRecommendationCard
                rec={engineRecommendation}
                narration={planNarration?.engine_explanation}
                darkMode={dm}
                currentEngine={engine}
                onUseRecommendation={useRecommendedEngine}
              />
            ) : null}
            <OverallReadinessBanner narration={planNarration || null} plan={plan} darkMode={dm} />
            <RelationshipsCard relationships={planRelationships} darkMode={dm} />
            <ManyToManyCard
              relationships={planRelationships}
              narration={planNarration?.relationships_summary}
              darkMode={dm}
            />
            <p className={`text-sm font-semibold ${dm ? 'text-white' : 'text-zinc-900'}`}>
              Expected impact (DQ counts + column profile heuristics)
            </p>
            <ul className={`list-disc space-y-1 pl-5 text-sm ${dm ? 'text-zinc-200' : 'text-zinc-800'}`}>
              {(Array.isArray(preview.summary_lines) ? preview.summary_lines : []).map((line: string, i: number) => (
                <li key={i}>{line}</li>
              ))}
            </ul>
            <EtlLineageVisualizer lineage={lineage as LineageMap} darkMode={dm} />
            <label
              className={`flex cursor-pointer items-start gap-3 rounded-xl border px-3 py-2 text-xs ${
                dm ? 'border-white/10 bg-black/20' : 'border-black/10 bg-white'
              }`}
            >
              <input
                type="checkbox"
                checked={useAiCodegen}
                onChange={(e) => setUseAiCodegen(e.target.checked)}
                disabled={busy}
                className="mt-0.5"
              />
              <span className={dm ? 'text-zinc-200' : 'text-zinc-800'}>
                <span className="font-semibold">Enhance with AI</span> (slower — calls Azure OpenAI, 30–90s).
                Leave unchecked for fast template code from your plan (recommended for PySpark).
              </span>
            </label>
            {busy && generateStatus ? (
              <p
                className={`text-xs font-medium ${dm ? 'text-emerald-200' : 'text-emerald-800'}`}
                role="status"
              >
                {generateStatus}
              </p>
            ) : null}
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                disabled={busy}
                onClick={() =>
                  pipelineMode === 'etl' && onEditPlanInRequirements
                    ? onEditPlanInRequirements()
                    : setStep('plan')
                }
                className={`rounded-xl px-4 py-2 text-sm font-semibold ${dm ? 'bg-white/10 text-white' : 'border border-black/10 bg-white text-zinc-800'}`}
              >
                {pipelineMode === 'etl' && onEditPlanInRequirements ? 'Edit plan in Requirements' : 'Back to plan'}
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => void runGenerate()}
                className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-4 py-2 text-sm font-bold text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                {busy
                  ? useAiCodegen
                    ? 'Generating with AI…'
                    : 'Generating…'
                  : `Generate ${genLabel}`}{' '}
                <FaChevronRight className="text-xs" />
              </button>
            </div>
          </motion.div>
        )}

        {(pipelineMode === 'full' || pipelineMode === 'etl') && step === 'code' && (
          <motion.div
            key="code"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            className="space-y-3"
          >
            <div className="flex flex-wrap items-center gap-2">
              {validationOk && !isDraft ? (
                <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2 py-0.5 text-xs font-bold text-emerald-800">
                  <FaCheck /> Validated{generatedBy ? ` (${generatedBy})` : ''}
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-xs font-bold text-amber-900">
                  <FaExclamationTriangle /> Draft — review before deploy
                </span>
              )}
              {artifactPath ? (
                <span className={`text-[11px] ${dm ? 'text-white/50' : 'text-black/50'}`}>
                  Saved: <code className={`rounded px-1 ${dm ? 'bg-white/10' : 'bg-black/5'}`}>{artifactPath}</code>
                </span>
              ) : null}
            </div>
            {validationErrors.length > 0 ? (
              <ul className="text-xs text-red-400">
                {validationErrors.map((v, i) => (
                  <li key={i}>{v}</li>
                ))}
              </ul>
            ) : null}
            <div className="relative overflow-hidden rounded-xl border border-black/10">
              <div
                className={`flex flex-wrap items-center justify-end gap-1 border-b px-2 py-1.5 ${
                  dm ? 'border-white/10 bg-black/30' : 'border-black/10 bg-zinc-100/90'
                }`}
              >
                <button
                  type="button"
                  onClick={() => void copyCode()}
                  disabled={!code.trim()}
                  className={`inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-bold transition-colors ${
                    copyStatus === 'copied'
                      ? 'bg-emerald-600 text-white'
                      : dm
                        ? 'bg-white/10 text-white hover:bg-white/15'
                        : 'bg-white text-zinc-800 shadow-sm hover:bg-zinc-50'
                  } disabled:cursor-not-allowed disabled:opacity-40`}
                >
                  <FaCopy className="text-[10px]" />
                  {copyStatus === 'copied'
                    ? 'Copied'
                    : copyStatus === 'failed'
                      ? 'Copy blocked — Select all'
                      : 'Copy'}
                </button>
                <button
                  type="button"
                  onClick={selectAllCode}
                  disabled={!code.trim()}
                  className={`rounded-md px-2 py-1 text-xs font-bold transition-colors ${
                    dm ? 'bg-white/10 text-white hover:bg-white/15' : 'bg-white text-zinc-800 shadow-sm hover:bg-zinc-50'
                  } disabled:cursor-not-allowed disabled:opacity-40`}
                >
                  Select all
                </button>
                <button
                  type="button"
                  onClick={() => void downloadCode()}
                  disabled={!code.trim()}
                  className={`inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-bold transition-colors ${
                    dm ? 'bg-white/10 text-white hover:bg-white/15' : 'bg-white text-zinc-800 shadow-sm hover:bg-zinc-50'
                  } disabled:cursor-not-allowed disabled:opacity-40`}
                >
                  <FaDownload className="text-[10px]" />
                  Download
                </button>
              </div>
              <textarea
                ref={codeTextareaRef}
                readOnly
                spellCheck={false}
                value={code}
                rows={22}
                aria-label="Generated ETL code"
                placeholder="Generate code to see output here."
                className={`w-full resize-y font-mono text-[11px] leading-relaxed outline-none ${
                  dm
                    ? 'min-h-[16rem] border-0 bg-black/40 p-3 text-emerald-100 placeholder:text-white/25'
                    : 'min-h-[16rem] border-0 bg-zinc-950 p-3 text-emerald-100 placeholder:text-emerald-100/30'
                }`}
              />
            </div>
            <p className={`text-[11px] leading-snug ${dm ? 'text-white/45' : 'text-black/50'}`}>
              Plain UTF-8. Deploy over HTTPS so one-click copy works. Code is AI-generated from your approved plan
              (template fallback if the model is unavailable)—wire your own sources, credentials, and schedules.
            </p>
            <motion.div
              className={`rounded-xl border p-3 ${dm ? 'border-white/10 bg-black/20' : 'border-black/10 bg-white/80'}`}
            >
              <p className={`mb-2 text-[11px] font-black uppercase tracking-widest ${label}`}>
                Post-ETL GX checkpoint
              </p>
              <p className={`mb-2 text-xs ${dm ? 'text-zinc-300' : 'text-zinc-700'}`}>
                Build expectation metadata from your approved plan and assessment (run after staging ETL output).
              </p>
              <button
                type="button"
                disabled={gxBusy || !code.trim()}
                onClick={() => void runGxCheckpoint()}
                className="inline-flex items-center gap-2 rounded-xl bg-violet-600 px-4 py-2 text-sm font-bold text-white hover:bg-violet-700 disabled:opacity-50"
              >
                <FaShieldAlt className="text-xs" />
                {gxBusy ? 'Running…' : 'Run GX checkpoint'}
              </button>
              {gxCheckpoint ? (
                <div className={`mt-3 space-y-1 text-xs font-mono ${dm ? 'text-zinc-200' : 'text-zinc-800'}`}>
                  {(() => {
                    const summary = gxCheckpoint.summary as Record<string, unknown> | undefined;
                    const overall = summary?.overall_ok;
                    return (
                      <p className={overall ? 'text-emerald-500' : 'text-amber-600'}>
                        Overall: {overall === true ? 'OK' : overall === false ? 'Review' : '—'}
                        {typeof summary?.expectation_count === 'number'
                          ? ` · ${summary.expectation_count} expectations`
                          : ''}
                      </p>
                    );
                  })()}
                  {Array.isArray(gxCheckpoint.expectations) ? (
                    <ul className="max-h-32 list-disc overflow-auto pl-4">
                      {(gxCheckpoint.expectations as Record<string, unknown>[]).slice(0, 12).map((ex, i) => (
                        <li key={i}>
                          {String(ex.type || 'expectation')}
                          {ex.column ? ` · ${String(ex.column)}` : ''}
                          {ex.passed === true ? ' ✓' : ex.passed === false ? ' ✗' : ''}
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              ) : null}
            </motion.div>
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => {
                  if (pipelineMode === 'etl' && onEditPlanInRequirements) {
                    onEditPlanInRequirements();
                    return;
                  }
                  setStep('rules');
                  setPlan(null);
                  setPreview(null);
                  setCode('');
                  setValidationOk(null);
                  setValidationErrors([]);
                  setArtifactPath(null);
                  setPlanJson('');
                  setPlanRows([]);
                }}
                className={`rounded-xl px-4 py-2 text-sm font-semibold ${dm ? 'bg-white/10 text-white' : 'border border-black/10 bg-white text-zinc-800'}`}
              >
                {pipelineMode === 'etl' && onEditPlanInRequirements ? 'Edit plan in Requirements' : 'Start over'}
              </button>
              {onContinueAfterCode && code.trim().length > 0 ? (
                <button
                  type="button"
                  onClick={() => onContinueAfterCode()}
                  className="inline-flex items-center gap-2 rounded-xl bg-[#0070AD] px-4 py-2 text-sm font-bold text-white shadow hover:bg-[#0070AD]/90"
                >
                  Continue to data cleaning <FaChevronRight className="text-xs" />
                </button>
              ) : null}
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {err ? (
        <div className="mt-4 flex items-start gap-2 rounded-xl border border-red-300/50 bg-red-950/40 px-3 py-2 text-sm text-red-100">
          <FaExclamationTriangle className="mt-0.5 shrink-0" />
          <span>{err}</span>
        </div>
      ) : null}
    </div>
  );
}
