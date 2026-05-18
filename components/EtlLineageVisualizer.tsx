'use client';

import { motion } from 'framer-motion';

/**
 * Visual column lineage: source dtype → transforms → target dtype per dataset.
 */
export interface LineageMap {
  [dataset: string]: {
    [column: string]: {
      source_dtype?: string;
      transforms?: string[];
      target_dtype?: string;
      nullable?: boolean;
    };
  };
}

interface EtlLineageVisualizerProps {
  lineage: LineageMap | null;
  darkMode?: boolean;
}

export default function EtlLineageVisualizer({ lineage, darkMode = false }: EtlLineageVisualizerProps) {
  if (!lineage || Object.keys(lineage).length === 0) {
    return null;
  }

  const border = darkMode ? 'border-white/10' : 'border-black/10';
  const card = darkMode ? 'bg-black/25' : 'bg-white/90';
  const label = darkMode ? 'text-emerald-200/70' : 'text-black/45';
  const text = darkMode ? 'text-zinc-200' : 'text-zinc-800';
  const pill = darkMode ? 'bg-emerald-500/20 text-emerald-100' : 'bg-emerald-100 text-emerald-900';
  const arrow = darkMode ? 'text-white/30' : 'text-black/25';

  return (
    <div className={`rounded-xl border p-4 ${border} ${card}`}>
      <p className={`mb-3 text-[11px] font-black uppercase tracking-widest ${label}`}>
        Schema lineage graph
      </p>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="space-y-6 max-h-[28rem] overflow-auto pr-1"
      >
        {Object.entries(lineage).map(([ds, cols]) => (
          <motion.div key={ds}>
            <div className={`mb-2 text-sm font-bold ${darkMode ? 'text-emerald-300' : 'text-emerald-800'}`}>
              {ds}
            </div>
            <div className="space-y-3">
              {Object.entries(cols).map(([col, meta]) => {
                const transforms = meta.transforms || [];
                return (
                  <div
                    key={col}
                    className={`flex flex-wrap items-center gap-2 rounded-lg border px-3 py-2 text-[11px] ${border}`}
                  >
                    <div className="min-w-[4.5rem] text-center">
                      <div className={`font-mono font-bold ${text}`}>{col}</div>
                      <div className={label}>{meta.source_dtype || '?'}</div>
                    </div>
                    <span className={`text-lg ${arrow}`}>→</span>
                    <div className="flex flex-wrap items-center gap-1">
                      {transforms.length === 0 ? (
                        <span className={`italic ${label}`}>no transforms</span>
                      ) : (
                        transforms.map((t, i) => (
                          <span
                            key={i}
                            className={`rounded-full px-2 py-0.5 font-mono text-[10px] font-bold ${pill}`}
                          >
                            {t}
                          </span>
                        ))
                      )}
                    </div>
                    <span className={`text-lg ${arrow}`}>→</span>
                    <div className="min-w-[4.5rem] text-center">
                      <div className={`font-mono font-bold ${text}`}>{col}</div>
                      <div className={label}>{meta.target_dtype || '?'}</div>
                    </div>
                  </div>
                );
              })}
            </div>
          </motion.div>
        ))}
      </motion.div>
    </div>
  );
}
