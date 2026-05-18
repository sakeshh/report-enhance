'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { motion } from 'framer-motion';

export interface ResolutionOption {
  id: string;
  label: string;
  action: string;
  recommended?: boolean;
  description?: string;
}

export interface ManualReviewItem {
  id: string;
  dataset?: string | null;
  column?: string | null;
  issue_type?: string;
  severity?: string;
  message?: string;
  guidance?: string;
  status?: string;
  default_resolution?: string;
  resolution_options?: ResolutionOption[];
}

export interface ManualReviewPanelProps {
  items: ManualReviewItem[];
  darkMode?: boolean;
  busy?: boolean;
  onApply: (resolutions: Array<{ item_id: string; resolution_id: string }>) => Promise<void>;
}

export default function ManualReviewPanel({
  items,
  darkMode = false,
  busy = false,
  onApply,
}: ManualReviewPanelProps) {
  const pending = useMemo(
    () => items.filter((m) => (m.status || 'pending') === 'pending'),
    [items]
  );

  const [picks, setPicks] = useState<Record<string, string>>({});

  useEffect(() => {
    const next: Record<string, string> = {};
    for (const m of pending) {
      const def =
        m.default_resolution ||
        m.resolution_options?.find((o) => o.recommended)?.id ||
        m.resolution_options?.[0]?.id ||
        'keep_as_is';
      next[m.id] = def;
    }
    setPicks(next);
  }, [pending]);

  const setPick = useCallback((itemId: string, resolutionId: string) => {
    setPicks((prev) => ({ ...prev, [itemId]: resolutionId }));
  }, []);

  const applyAll = useCallback(async () => {
    const resolutions = pending.map((m) => ({
      item_id: m.id,
      resolution_id: picks[m.id] || m.default_resolution || 'keep_as_is',
    }));
    await onApply(resolutions);
  }, [pending, picks, onApply]);

  if (!pending.length) return null;

  const border = darkMode ? 'border-amber-400/35 bg-amber-500/10' : 'border-amber-300 bg-amber-50';
  const text = darkMode ? 'text-zinc-100' : 'text-zinc-900';
  const sub = darkMode ? 'text-zinc-300' : 'text-zinc-600';
  const card = darkMode ? 'border-white/10 bg-black/20' : 'border-black/10 bg-white';

  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      className={`rounded-xl border p-4 ${border}`}
    >
      <motion.div className="mb-3 flex flex-wrap items-center gap-2">
        <span className={`text-[11px] font-black uppercase tracking-widest ${sub}`}>
          Manual review — choose how to handle each issue
        </span>
        <span
          className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${
            darkMode ? 'bg-amber-500/30 text-amber-100' : 'bg-amber-200 text-amber-950'
          }`}
        >
          {pending.length} pending
        </span>
      </motion.div>
      <p className={`mb-4 text-xs leading-relaxed ${sub}`}>
        Dhara detected issues that need your decision. Pick one option per item, then apply to add
        transforms to the ETL plan. You must resolve all items before confirming the plan.
      </p>

      <ul className="space-y-4">
        {pending.map((m) => (
          <li key={m.id} className={`rounded-xl border p-3 ${card}`}>
            <div className={`mb-2 text-sm font-semibold ${text}`}>
              <span className="font-mono text-xs opacity-70">{m.dataset || '—'}</span>
              {m.column ? (
                <>
                  <span className="mx-1 opacity-40">·</span>
                  <span className="font-mono">{m.column}</span>
                </>
              ) : null}
              <span
                className={`ml-2 rounded px-1.5 py-0.5 text-[9px] font-black uppercase ${
                  darkMode ? 'bg-white/10' : 'bg-black/5'
                }`}
              >
                {(m.issue_type || 'issue').replace(/_/g, ' ')}
              </span>
            </div>
            {m.message ? <p className={`mb-1 text-xs ${sub}`}>{m.message}</p> : null}
            {m.guidance ? (
              <p className={`mb-3 text-xs italic ${sub}`}>{m.guidance}</p>
            ) : null}

            <div className="space-y-2">
              {(m.resolution_options || []).map((opt) => {
                const checked = picks[m.id] === opt.id;
                return (
                  <label
                    key={opt.id}
                    className={`flex cursor-pointer items-start gap-2 rounded-lg border px-3 py-2 text-xs transition ${
                      checked
                        ? darkMode
                          ? 'border-emerald-400/50 bg-emerald-500/15'
                          : 'border-emerald-400 bg-emerald-50'
                        : darkMode
                          ? 'border-white/10 hover:bg-white/5'
                          : 'border-black/5 hover:bg-black/[0.02]'
                    }`}
                  >
                    <input
                      type="radio"
                      name={`manual-${m.id}`}
                      checked={checked}
                      onChange={() => setPick(m.id, opt.id)}
                      className="mt-0.5"
                    />
                    <span className={text}>
                      <span className="font-semibold">{opt.label}</span>
                      {opt.recommended ? (
                        <span className="ml-1 text-[10px] font-bold text-emerald-600">recommended</span>
                      ) : null}
                      {opt.description ? (
                        <span className={`mt-0.5 block font-normal ${sub}`}>{opt.description}</span>
                      ) : null}
                    </span>
                  </label>
                );
              })}
            </div>
          </li>
        ))}
      </ul>

      <button
        type="button"
        disabled={busy || pending.length === 0}
        onClick={() => void applyAll()}
        className="mt-4 inline-flex items-center gap-2 rounded-xl bg-amber-600 px-4 py-2.5 text-sm font-bold text-white shadow hover:bg-amber-700 disabled:opacity-50"
      >
        Apply {pending.length} selection{pending.length === 1 ? '' : 's'} to plan
      </button>
    </motion.div>
  );
}
