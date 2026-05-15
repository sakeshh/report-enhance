'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { FaCode, FaChevronRight, FaExclamationTriangle, FaCheck, FaCopy, FaTrash } from 'react-icons/fa';

type Step = 'rules' | 'plan' | 'preview' | 'code';

export type EtlEngine = 'python' | 'sql' | 'spark' | 'adf';

type StepRow = { id: string; dataset: string; order: number; column: string | null; action: string };

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
  const [sqlDialect, setSqlDialect] = useState<'tsql' | 'ansi'>('tsql');
  const [neverDropRows, setNeverDropRows] = useState(false);
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

  useEffect(() => {
    if (plan) {
      setPlanJson(JSON.stringify(plan, null, 2));
      setPlanRows(planToRows(plan));
    }
  }, [plan]);

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
        if (!flow?.preview || !flow?.approved_plan) {
          setErr(
            'Complete ETL rules and plan on the Requirements step first (confirm the plan to save the preview).',
          );
          setEtlSessionLoading(false);
          return;
        }
        setPlan(flow.approved_plan as Record<string, unknown>);
        setPreview(flow.preview as Record<string, unknown>);
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
        }),
      });
      const data = await res.json().catch(() => null);
      if (!res.ok || !data?.ok) {
        setErr(data?.message || data?.error || `Plan failed (${res.status})`);
        return;
      }
      const blocked = Array.isArray(data.blocked) ? data.blocked : [];
      if (blocked.length > 0) {
        setErr(`Blocked: ${blocked.map((b: { message?: string }) => b.message || JSON.stringify(b)).join(' | ')}`);
        setPlan(data.plan || null);
        setStep('plan');
        return;
      }
      setPlan(data.plan || null);
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

  const runConfirm = async () => {
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
        setErr(data?.message || data?.error || `Confirm failed (${res.status})`);
        return;
      }
      setPreview((data.preview as Record<string, unknown>) || null);
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

  const runGenerate = async () => {
    setBusy(true);
    setErr(null);
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
        }),
      });
      const data = await res.json().catch(() => null);
      if (!res.ok || !data?.ok) {
        setErr(data?.message || data?.error || `Generate failed (${res.status})`);
        return;
      }
      setCode(String(data.code || ''));
      onCodeGenerated?.(String(data.code || ''));
      setValidationOk(Boolean(data.validation_ok));
      setValidationErrors(Array.isArray(data.validation_errors) ? data.validation_errors : []);
      setArtifactPath(typeof data.artifact_rel_path === 'string' ? data.artifact_rel_path : null);
      setStep('code');
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : 'Generate request failed');
    } finally {
      setBusy(false);
    }
  };

  const copyCode = () => {
    if (!code) return;
    void navigator.clipboard.writeText(code);
  };

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
                    onClick={() => setEngine(k)}
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
              Edit steps in the table, or switch to JSON. Confirm runs impact preview using assessment metrics.
            </p>
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
                    <strong>No automatic steps</strong> — the assessment issues may only map to{' '}
                    <code className="rounded bg-black/10 px-1">manual_review</code> in the plan, so the table is
                    empty. Open the <strong>JSON</strong> tab to inspect{' '}
                    <code className="rounded bg-black/10 px-1">manual_review</code>
                    {Array.isArray((plan as { manual_review?: unknown[] }).manual_review) &&
                    (plan as { manual_review: unknown[] }).manual_review.length > 0
                      ? ` (${(plan as { manual_review: unknown[] }).manual_review.length} items)`
                      : ''}
                    . After updating Agent Dhara, click <strong>Back</strong> and <strong>Build ETL plan</strong> again —
                    <code className="rounded bg-black/10 px-1">case_inconsistency</code> now maps to{' '}
                    <code className="rounded bg-black/10 px-1">lowercase</code> steps for <code>name</code> /{' '}
                    <code>department</code>.
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
                      <th className="p-2" />
                    </tr>
                  </thead>
                  <tbody>
                    {planRows.map((r) => (
                      <tr key={r.id} className={dm ? 'border-t border-white/5' : 'border-t border-black/5'}>
                        <td className="p-2 font-mono">{r.dataset}</td>
                        <td className="p-2">{r.order}</td>
                        <td className="p-2 font-mono">{r.column ?? '—'}</td>
                        <td className="p-2 font-mono">{r.action}</td>
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
                    ))}
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
                disabled={busy}
                onClick={() => void runConfirm()}
                className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-4 py-2 text-sm font-bold text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                {confirmPlanLabel} <FaChevronRight className="text-xs" />
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
            <p className={`text-sm font-semibold ${dm ? 'text-white' : 'text-zinc-900'}`}>
              Expected impact (DQ counts + column profile heuristics)
            </p>
            <ul className={`list-disc space-y-1 pl-5 text-sm ${dm ? 'text-zinc-200' : 'text-zinc-800'}`}>
              {(Array.isArray(preview.summary_lines) ? preview.summary_lines : []).map((line: string, i: number) => (
                <li key={i}>{line}</li>
              ))}
            </ul>
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
                Generate {genLabel} <FaChevronRight className="text-xs" />
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
              {validationOk ? (
                <span className="inline-flex items-center gap-1 rounded-full bg-emerald-100 px-2 py-0.5 text-xs font-bold text-emerald-800">
                  <FaCheck /> Validation OK
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-0.5 text-xs font-bold text-amber-900">
                  <FaExclamationTriangle /> Check validation
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
            <div className="relative">
              <button
                type="button"
                onClick={() => copyCode()}
                className={`absolute right-2 top-2 rounded-lg px-2 py-1 text-xs font-bold shadow ${
                  dm ? 'bg-[#001a2e]/90 text-white' : 'bg-white/90 text-zinc-700'
                }`}
              >
                <FaCopy className="inline mr-1" />
                Copy
              </button>
              <pre
                className={`max-h-80 overflow-auto rounded-xl border p-3 pr-16 pt-10 text-[11px] leading-relaxed ${
                  dm ? 'border-white/10 bg-black/40 text-emerald-100' : 'border-black/10 bg-zinc-950 text-emerald-100'
                }`}
              >
                {code || '—'}
              </pre>
            </div>
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
