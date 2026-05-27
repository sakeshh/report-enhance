'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { FaTags, FaArrowLeft, FaCheck, FaInfoCircle, FaSpinner } from 'react-icons/fa';

interface SemanticReviewPanelProps {
  database: string;
  files: string[];
  onComplete: (approvedSemantics: Record<string, Record<string, string>>) => void;
  onBack: () => void;
}

type ColumnSemantics = {
  name: string;
  tag: string;
  samples: string[];
};

type TableSemanticsMap = Record<string, ColumnSemantics[]>;

const SEMANTIC_OPTIONS = [
  { value: 'id', label: 'ID / Identifier' },
  { value: 'metric', label: 'Metric (Measure)' },
  { value: 'categorical', label: 'Categorical' },
  { value: 'date', label: 'Date / Datetime' },
  { value: 'text', label: 'General Text' },
];

export default function SemanticReviewPanel({ database, files, onComplete, onBack }: SemanticReviewPanelProps) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [semanticsMap, setSemanticsMap] = useState<TableSemanticsMap>({});
  const [retryTrigger, setRetryTrigger] = useState(0);

  useEffect(() => {
    let alive = true;
    const fetchSemantics = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch('/api/etl/infer-semantics', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sources: files }),
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data.message || 'Failed to infer semantics');
        }

        if (!alive) return;

        const tableMap: TableSemanticsMap = {};
        const inferredSemantics = data.semantics || {};
        const sampleValues = data.samples || {};

        // Helper to normalize table keys (case-insensitive and ignores all non-alphanumeric characters)
        const normalizeKey = (key: string) => {
          return String(key || '').toLowerCase().replace(/[^a-z0-9]/g, '');
        };

        files.forEach((table) => {
          const normTable = normalizeKey(table);
          const matchedKey = Object.keys(inferredSemantics).find(
            (k) => normalizeKey(k) === normTable
          ) || table;

          const colTags = inferredSemantics[matchedKey] || {};
          const colSamples = sampleValues[matchedKey] || {};
          
          tableMap[table] = Object.keys(colTags).map((col) => ({
            name: col,
            tag: colTags[col] || 'text',
            samples: colSamples[col] || [],
          }));
        });

        setSemanticsMap(tableMap);
      } catch (err: any) {
        if (alive) {
          setError(err.message || 'An error occurred during semantic inference.');
        }
      } finally {
        if (alive) {
          setLoading(false);
        }
      }
    };

    fetchSemantics();
    return () => {
      alive = false;
    };
  }, [files, retryTrigger]);

  const handleTagChange = (tableName: string, colName: string, newTag: string) => {
    setSemanticsMap((prev) => {
      const updated = { ...prev };
      updated[tableName] = updated[tableName].map((col) =>
        col.name === colName ? { ...col, tag: newTag } : col
      );
      return updated;
    });
  };

  const handleConfirm = () => {
    const approved: Record<string, Record<string, string>> = {};
    Object.entries(semanticsMap).forEach(([table, columns]) => {
      approved[table] = {};
      columns.forEach((col) => {
        approved[table][col.name] = col.tag;
      });
    });
    onComplete(approved);
  };

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center space-y-8 py-16">
        <div className="relative">
          <div className="absolute inset-0 scale-150 blur-3xl bg-gradient-to-tr from-[#0070AD]/20 to-[#12ABDB]/20 animate-pulse" />
          <div className="relative flex h-24 w-24 items-center justify-center rounded-3xl bg-white shadow-2xl">
            <FaSpinner className="h-12 w-12 animate-spin text-[#0070AD]" />
          </div>
        </div>
        <div className="text-center space-y-2">
          <h3 className="text-2xl font-bold text-zinc-900">Inferring Column Semantics</h3>
          <p className="text-sm text-black/50">LLM is scanning schemas and samples to categorize data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6 py-8 text-center max-w-md mx-auto">
        <div className="w-16 h-16 bg-red-500/10 rounded-full flex items-center justify-center text-red-500 mx-auto">
          <FaInfoCircle className="text-3xl" />
        </div>
        <div className="space-y-2">
          <h3 className="text-xl font-bold text-zinc-900">Inference Failed</h3>
          <p className="text-sm text-black/60">{error}</p>
        </div>
        <div className="flex gap-4 justify-center">
          <button
            onClick={onBack}
            className="px-6 py-2.5 rounded-xl border border-black/10 hover:border-black/20 text-sm font-semibold transition-colors"
          >
            Go Back
          </button>
          <button
            onClick={() => setRetryTrigger((prev) => prev + 1)}
            className="px-6 py-2.5 rounded-xl bg-[#0070AD] text-white hover:bg-[#0070AD]/90 text-sm font-semibold transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-zinc-900 mb-2 flex items-center gap-2">
            <FaTags className="text-[#0070AD]" />
            Verify Column Semantics
          </h2>
          <p className="text-black/60">Review and approve LLM-classified column types to prevent DQ false positives.</p>
        </div>
        <button
          onClick={onBack}
          className="flex items-center gap-2 text-sm text-black/65 hover:text-black transition-colors"
        >
          <FaArrowLeft />
          <span>Back</span>
        </button>
      </div>

      <div className="space-y-8 max-h-[calc(100vh-340px)] overflow-y-auto pr-2">
        {Object.entries(semanticsMap).map(([tableName, columns]) => (
          <motion.div
            key={tableName}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="rounded-2xl border border-black/10 bg-white/60 p-6 shadow-sm"
          >
            <h3 className="text-lg font-bold text-zinc-900 mb-4 pb-2 border-b border-black/5">
              Table: <span className="text-[#0070AD]">{tableName}</span>
            </h3>

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-xs font-bold uppercase tracking-wider text-black/40 border-b border-black/5 font-sans">
                    <th className="py-3 text-left">Column Name</th>
                    <th className="py-3 text-left">Sample Values</th>
                    <th className="py-3 text-left w-64">Semantic Category</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-black/5">
                  {columns.map((col) => (
                    <tr key={col.name} className="hover:bg-black/[0.01]">
                      <td className="py-3 font-semibold text-zinc-800">{col.name}</td>
                      <td className="py-3">
                        <div className="flex flex-wrap gap-1">
                          {col.samples.length > 0 ? (
                            col.samples.map((val, idx) => (
                              <span
                                key={idx}
                                className="inline-block bg-black/5 px-2 py-0.5 rounded text-xs text-black/65 font-mono truncate max-w-xs"
                                title={val}
                              >
                                {val}
                              </span>
                            ))
                          ) : (
                            <span className="text-black/35 italic">No non-null samples</span>
                          )}
                        </div>
                      </td>
                      <td className="py-2">
                        <select
                          value={col.tag}
                          onChange={(e) => handleTagChange(tableName, col.name, e.target.value)}
                          className="w-full px-3 py-1.5 border border-black/10 rounded-lg outline-none focus:border-[#0070AD]/50 bg-white text-zinc-800 font-medium text-xs transition-colors"
                        >
                          {SEMANTIC_OPTIONS.map((opt) => (
                            <option key={opt.value} value={opt.value}>
                              {opt.label}
                            </option>
                          ))}
                        </select>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </motion.div>
        ))}
      </div>

      <motion.button
        onClick={handleConfirm}
        className="w-full py-4 rounded-xl border border-[#0070AD]/40 bg-[#0070AD]/10 text-[#0070AD] font-semibold hover:bg-[#0070AD]/15 hover:border-[#0070AD]/60 transition-all flex items-center justify-center gap-2"
        whileHover={{ scale: 1.01 }}
        whileTap={{ scale: 0.99 }}
      >
        <FaCheck />
        <span>Confirm Semantics & Run Assessment</span>
      </motion.button>
    </div>
  );
}
