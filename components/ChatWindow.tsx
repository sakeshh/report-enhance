'use client';

import { useState, useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  FaDatabase,
  FaCloud,
  FaStream,
  FaPlug,
  FaFolder,
  FaChevronDown,
  FaPaperPlane,
  FaArrowLeft,
  FaCopy,
  FaShareAlt,
  FaEdit,
  FaStop,
  FaPlus,
  FaFileUpload,
  FaExclamationTriangle,
  FaCode,
  FaCheck,
} from 'react-icons/fa';
import { Message } from '@/types';
import Image from 'next/image';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import ReportEnhancements from '@/components/ReportEnhancements';
import EtlGenerationPanel from '@/components/EtlGenerationPanel';
import {
  humanizeSeverityLabel,
  humanizeSnakeIdentifier,
  prettyTableHeaderKey,
  sourceRootLabel,
} from '@/lib/assessmentDisplay';

interface ChatOption {
  id: string;
  text: string;
  category: string;
  action?: string;
  icon?: React.ReactNode;
}

const DATA_SOURCE_OPTIONS: ChatOption[] = [
  { id: 'local', text: 'Local data', category: 'Data Source', action: 'local-data', icon: <FaFolder className="text-[#0070AD]/80" /> },
  { id: 'sql', text: 'SQL data', category: 'Data Source', action: 'sql-data', icon: <FaDatabase className="text-[#0070AD]/80" /> },
  { id: 'blob', text: 'Blob data', category: 'Data Source', action: 'blob-data', icon: <FaCloud className="text-[#12ABDB]/80" /> },
  { id: 'streams', text: 'Real-time streams', category: 'Data Source', action: 'realtime-streams', icon: <FaStream className="text-[#0070AD]/80" /> },
  { id: 'apis', text: 'APIs', category: 'Data Source', action: 'apis', icon: <FaPlug className="text-[#12ABDB]/80" /> },
];

/* Options shown after selecting a data source — vary by source type */
const CHAT_OPTIONS_BY_SOURCE: Record<string, ChatOption[]> = {
  sql: [
    { id: 'sql-view', text: '👁️ View data in files/tables', category: 'Explore', action: 'guided-view' },
    { id: 'sql-report', text: '📄 Extract report from files/tables', category: 'Reports', action: 'guided-report' },
  ],
  local: [
    { id: 'local-view', text: '👁️ View data in files', category: 'Explore', action: 'guided-view' },
    { id: 'local-report', text: '📄 Extract report from files', category: 'Reports', action: 'guided-report' },
  ],
  blob: [
    { id: 'blob-view', text: '👁️ View data in files', category: 'Explore', action: 'guided-view' },
    { id: 'blob-report', text: '📄 Extract report from files', category: 'Reports', action: 'guided-report' },
  ],
  streams: [
    { id: 'streams-view', text: '👁️ View data in stream files', category: 'Explore', action: 'guided-view' },
    { id: 'streams-report', text: '📄 Extract report from stream files', category: 'Reports', action: 'guided-report' },
  ],
  apis: [
    { id: 'apis-1', text: '📊 Start Data Pipeline Workflow', category: 'Pipeline', action: '/data-pipeline' },
    { id: 'apis-2', text: '🔗 Connect to REST / GraphQL API', category: 'API', action: 'connect' },
    { id: 'apis-2b', text: '📄 View Report', category: 'Reports', action: 'view-report' },
    { id: 'apis-3', text: '📥 Fetch API Data', category: 'Import', action: 'fetch' },
    { id: 'apis-4', text: '🔄 Transform Data', category: 'Transform', action: 'transform' },
    { id: 'apis-5', text: '📤 Export Data', category: 'Export', action: 'export' },
    { id: 'apis-6', text: '🔍 Analyze API Response', category: 'Analysis', action: 'analyze' },
  ],
};

function getChatOptionsForSource(sourceId: string | null): ChatOption[] {
  if (!sourceId) return [];
  return CHAT_OPTIONS_BY_SOURCE[sourceId] ?? CHAT_OPTIONS_BY_SOURCE.local;
}

/** Format time the same on server and client to avoid hydration mismatch (e.g. "pm" vs "PM"). */
function formatTime(d: Date): string {
  const h = d.getHours();
  const m = d.getMinutes();
  const ampm = h >= 12 ? 'PM' : 'AM';
  const h12 = h % 12 || 12;
  const min = m < 10 ? `0${m}` : String(m);
  return `${h12}:${min} ${ampm}`;
}

export default function ChatWindow({ gxEnabled = false }: { gxEnabled?: boolean }) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [hasSelectedDataSource, setHasSelectedDataSource] = useState(false);
  const [selectedDataSource, setSelectedDataSource] = useState<string | null>(null);
  const [agentError, setAgentError] = useState<string | null>(null);
  const [showOptions, setShowOptions] = useState(false);
  const [chatInput, setChatInput] = useState('');
  const [isLoadingAgent, setIsLoadingAgent] = useState(false);
  const [jobProgress, setJobProgress] = useState<string | null>(null);
  const [agentThreadId, setAgentThreadId] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string>('default');
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editingText, setEditingText] = useState<string>('');
  const [localFolderFiles, setLocalFolderFiles] = useState<File[]>([]);
  const [guidedMode, setGuidedMode] = useState<'none' | 'view' | 'report'>('none');
  // For SQL table selection in-chat: allow multi-select + OK.
  const [pendingTableSelections, setPendingTableSelections] = useState<Record<string, number[]>>({});
  // For Blob/Local file selection: allow multi-select + OK.
  const [pendingFileSelections, setPendingFileSelections] = useState<
    Record<string, { mode: 'blob' | 'local'; selected: number[] }>
  >({});
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesScrollRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const localFolderInputRef = useRef<HTMLInputElement | null>(null);

  const showDebugPanels =
    typeof window !== 'undefined' &&
    (window.location.search.includes('debug=1') || window.location.hash.includes('debug=1'));

  const getPersistedSelectedSource = (): string | null => {
    if (typeof window === 'undefined') return null;
    const v = window.localStorage.getItem('dharaSelectedDataSource');
    return v && typeof v === 'string' ? v : null;
  };

  const getEffectiveSessionId = (): string => {
    if (typeof window === 'undefined') return 'default';
    return window.localStorage.getItem('dharaSessionId') || 'default';
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const saved = window.localStorage.getItem('agentThreadId');
    if (saved && !agentThreadId) setAgentThreadId(saved);
    setSessionId(getEffectiveSessionId());
    const persistedSource = getPersistedSelectedSource();
    if (persistedSource && !selectedDataSource) {
      setSelectedDataSource(persistedSource);
      setHasSelectedDataSource(true);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // First message should be from the agent (UI greeting).
  // If the session has no prior messages, seed a single assistant greeting.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (!sessionId) return;
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}`);
        const data = await res.json().catch(() => null);
        const session = data?.session;
        const msgs = Array.isArray(session?.messages) ? session.messages : [];
        if (cancelled) return;
        if (msgs.length) return;
        if (messages.length) return;
        setMessages([
          {
            id: `greeting-${Date.now()}`,
            text: "Hi i'm Agent Dhara, Select a Data Source to get started..",
            sender: 'bot',
            timestamp: new Date(),
            options: [
              { id: 'sql', text: '1. SQL', send: 'sql' },
              { id: 'blob', text: '2. Blob', send: 'blob' },
              { id: 'fs', text: '3. File Stream', send: 'file stream' },
            ],
          },
        ]);
      } catch {
        // If session fetch fails, still show greeting for a usable first render.
        if (cancelled) return;
        if (messages.length) return;
        setMessages([
          {
            id: `greeting-${Date.now()}`,
            text: "Hi i'm Agent Dhara, Select a Data Source to get started..",
            sender: 'bot',
            timestamp: new Date(),
            options: [
              { id: 'sql', text: '1. SQL', send: 'sql' },
              { id: 'blob', text: '2. Blob', send: 'blob' },
              { id: 'fs', text: '3. File Stream', send: 'file stream' },
            ],
          },
        ]);
      }
    })();
    return () => {
      cancelled = true;
    };
    // Intentionally run once per session id.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (selectedDataSource) {
      window.localStorage.setItem('dharaSelectedDataSource', selectedDataSource);
    }
  }, [selectedDataSource]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (agentThreadId) window.localStorage.setItem('agentThreadId', agentThreadId);
  }, [agentThreadId]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const handler = async () => {
      const sid = getEffectiveSessionId();
      // Hard reset UI state on session change (e.g. "New chat")
      setMessages([]);
      setAgentError(null);
      setShowOptions(false);
      setGuidedMode('none');
      setPendingTableSelections({});
      setPendingFileSelections({});
      setLocalFolderFiles([]);
      setHasSelectedDataSource(false);
      setSelectedDataSource(getPersistedSelectedSource());
      setAgentThreadId(window.localStorage.getItem('agentThreadId'));
      setSessionId(sid);
      try {
        const res = await fetch(`/api/sessions/${encodeURIComponent(sid)}`);
        const data = await res.json();
        const session = data?.session;
        const msgs = Array.isArray(session?.messages) ? session.messages : [];
        if (!msgs.length) return; // greeting effect will seed the first bot message
        setMessages(
          msgs
            .filter((m: any) => m?.content && (m?.role === 'user' || m?.role === 'assistant'))
            .map((m: any, idx: number) => ({
              id: String(m?.id ?? `${sid}-${idx}-${Date.now()}`),
              text: String(m.content),
              sender: m.role === 'user' ? ('user' as const) : ('bot' as const),
              timestamp: m.ts ? new Date(Number(m.ts) * 1000) : new Date(),
            }))
        );
      } catch {
        // ignore
      }
    };
    window.addEventListener('dhara-session-change', handler as EventListener);
    return () => window.removeEventListener('dhara-session-change', handler as EventListener);
  }, []);

  /** Build request payload for Azure agent from current messages */
  const getMessagesForApi = (msgs: Message[]) =>
    msgs.map((m) => ({
      role: m.sender === 'user' ? ('user' as const) : ('assistant' as const),
      content: m.text,
    }));

  /** Poll for job status until completion */
  const pollJobStatus = async (jobId: string): Promise<any> => {
    const startTime = Date.now();
    const TIMEOUT = 1200000; // 20 minutes
    const INTERVAL = 2000; // 2 seconds

    while (Date.now() - startTime < TIMEOUT) {
      if (abortRef.current?.signal.aborted) throw new Error('ABORTED');

      // Fetch progress events
      try {
        const eventsRes = await fetch(`/api/jobs/${jobId}/events`);
        if (eventsRes.ok) {
          const { events } = await eventsRes.json();
          if (Array.isArray(events) && events.length > 0) {
            const last = events[events.length - 1];
            setJobProgress(last.message || 'Processing...');
          }
        }
      } catch (e) {
        // ignore event fetch errors
      }

      let res;
      let retryCount = 0;
      const MAX_RETRIES = 3;

      while (retryCount < MAX_RETRIES) {
        try {
          res = await fetch(`/api/jobs/${jobId}`);
          if (res.ok || (res.status !== 502 && res.status !== 504 && res.status !== 503)) {
            break;
          }
        } catch (e) {
          // fetch failed (network error)
        }
        retryCount++;
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      if (!res || !res.ok) {
        throw new Error(`Polling failed (${res?.status || 'network_error'})`);
      }
      
      const { job } = await res.json();
      if (job.status === 'succeeded') {
        setJobProgress(null);
        return job.result;
      }
      if (job.status === 'failed') {
        setJobProgress(null);
        throw new Error(job.error || 'Job failed');
      }
      
      await new Promise(resolve => setTimeout(resolve, INTERVAL));
    }
    throw new Error('Job timed out');
  };

  /** Call our API route which invokes your Azure AI Foundry agent (threads + runs). */
  const fetchAgentReply = async (
    msgs: Message[]
  ): Promise<{ content: string; threadId: string | null; payload: any }> => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    // Use Async Job Pattern to avoid timeouts
    const res = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: controller.signal,
      body: JSON.stringify({
        kind: 'chat',
        input: {
          messages: getMessagesForApi(msgs),
          threadId: agentThreadId,
          session_id: sessionId,
          message: msgs[msgs.length - 1].text, // The latest message
          gx_enabled: gxEnabled,
        },
      }),
    });

    const data = await res.json();
    if (!res.ok || !data.job_id) {
      throw new Error(data?.error || 'Failed to create job');
    }

    const result = await pollJobStatus(data.job_id);
    const content = typeof result?.reply === 'string' ? result.reply : '';
    const threadId = result?.threadId ?? null;
    const payload = result?.payload ?? null;

    if (!content.trim() && !payload) {
      throw new Error('EMPTY_REPLY');
    }
    return { content, threadId, payload };
  };

  const stopGeneration = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setIsLoadingAgent(false);
  };

  const uploadReport = async (file: File) => {
    // Upload via Next.js API route (proxies to backend /upload?format=md)
    const form = new FormData();
    form.append('file', file, file.name);
    const res = await fetch('/api/upload', { method: 'POST', body: form });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.detail || data?.error || `Upload failed (${res.status})`);
    const report = String(data?.report ?? '');
    if (!report) throw new Error('No report returned.');

    // Persist in backend session context for later Q&A/compare.
    await fetch('/api/session-context', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId,
        context: {
          uploaded_report_name: file.name,
          uploaded_report_markdown: report,
          uploaded_report_uploaded_at: Date.now(),
        },
      }),
    }).catch(() => null);
    // No canned bot response here—user can ask the agent about the uploaded report next.
  };

  const copyText = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // ignore
    }
  };

  const shareText = async (text: string) => {
    try {
      // @ts-ignore - web share optional
      if (navigator.share) {
        // @ts-ignore
        await navigator.share({ text });
        return;
      }
    } catch {
      // ignore
    }
    await copyText(text);
  };

  const tryParseJson = (text: string): any | null => {
    const t = String(text || '').trim();
    if (!t) return null;
    // Quick guard: only try parse if it looks like JSON.
    if (!(t.startsWith('{') || t.startsWith('['))) return null;
    try {
      return JSON.parse(t);
    } catch {
      return null;
    }
  };

  const JsonTable = ({ value, prettyHeaders, gx }: { value: any; prettyHeaders?: boolean; gx?: boolean }) => {
    const [showAll, setShowAll] = useState(false);
    const arr = Array.isArray(value) ? value : null;
    if (!arr || arr.length === 0) {
      return (
        <pre className={`max-w-full overflow-x-auto rounded-lg border p-3 text-xs shadow-sm ${gx ? 'border-emerald-500/30 bg-[#000B14] text-emerald-400' : 'border-black/10 bg-white/70 text-zinc-900'}`}>
          {JSON.stringify(value, null, 2)}
        </pre>
      );
    }
    const isRowObj = arr.every((x) => x && typeof x === 'object' && !Array.isArray(x));
    if (!isRowObj) {
      return (
        <pre className={`max-w-full overflow-x-auto rounded-lg border p-3 text-xs ${gx ? 'border-emerald-500/30 bg-[#000B14] text-emerald-400' : 'border-black/10 bg-white/70 text-zinc-900'}`}>
          {JSON.stringify(value, null, 2)}
        </pre>
      );
    }
    const cols = Array.from(new Set(arr.flatMap((r: any) => Object.keys(r || {}))));
    const rows = (showAll || prettyHeaders) ? arr : arr.slice(0, 25);
    const more = arr.length > rows.length ? arr.length - rows.length : 0;
    return (
      <div className={`group/table relative max-w-full overflow-hidden rounded-2xl border shadow-2xl transition-all duration-500 ${gx ? 'border-emerald-500/20 bg-[#000B14]' : 'border-[#0070AD]/20 bg-white/50'}`}>
        {gx && (
          <div className="absolute right-3 top-3 z-30 flex items-center gap-1.5 rounded-full bg-emerald-500/20 px-2.5 py-1 text-[9px] font-black uppercase tracking-tighter text-emerald-400 border border-emerald-500/30 backdrop-blur-md">
            <FaCheck className="text-[8px]" />
            GX VERIFIED
          </div>
        )}
        <div className="max-h-[600px] overflow-auto">
          <table className="w-full border-collapse text-[12px] leading-tight">
            <thead className="sticky top-0 z-20">
              <tr className={`backdrop-blur-xl ${gx ? 'bg-[#001D2E] border-b border-emerald-500/20' : 'bg-gradient-to-r from-[#0070AD]/10 to-[#12ABDB]/10 border-b border-[#0070AD]/10'}`}>
                {cols.map((c) => (
                  <th
                    key={c}
                    className={`px-4 py-4 text-left font-black uppercase tracking-widest last:border-r-0 ${gx ? 'text-emerald-400/90 border-r border-emerald-500/10' : 'text-[#0070AD] border-r border-[#0070AD]/10'}`}
                  >
                    {prettyHeaders ? prettyTableHeaderKey(c) : c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className={`divide-y ${gx ? 'divide-emerald-500/10' : 'divide-[#0070AD]/10'}`}>
              {rows.map((r: any, i: number) => (
                <tr 
                  key={i} 
                  className={`transition-colors group/row ${gx ? 'hover:bg-white/5' : 'hover:bg-[#12ABDB]/5'}`}
                >
                  {cols.map((c) => (
                    <td
                      key={c}
                      className={`px-4 py-3 align-top last:border-r-0 ${gx ? 'text-zinc-100 border-r border-emerald-500/5' : 'text-zinc-800 border-r border-[#0070AD]/5'}`}
                    >
                      <div className="max-w-[560px] whitespace-pre-wrap break-words leading-relaxed font-semibold">
                        {r?.[c] === null || r?.[c] === undefined
                          ? <span className="text-zinc-500 italic">null</span>
                          : typeof r?.[c] === 'object'
                            ? <code className={`text-[11px] px-1.5 py-0.5 rounded font-mono ${gx ? 'bg-emerald-500/10 text-emerald-300' : 'bg-black/5 text-zinc-900'}`}>{JSON.stringify(r?.[c])}</code>
                            : String(r?.[c])}
                      </div>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {more > 0 ? (
          <div className={`flex items-center justify-between gap-3 border-t px-5 py-3 text-[11px] font-bold ${gx ? 'border-emerald-500/10 bg-[#002B45] text-emerald-400/60' : 'border-[#0070AD]/10 bg-white/40 text-zinc-600'}`}>
            <div className="flex items-center gap-2">
              <span className={`h-2 w-2 rounded-full animate-pulse ${gx ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]' : 'bg-[#12ABDB]'}`} />
              Showing {rows.length} of {arr.length} total rows
            </div>
            <button
              type="button"
              onClick={() => setShowAll(true)}
              className={`group flex items-center gap-2 rounded-full border px-4 py-1.5 transition-all hover:scale-105 active:scale-95 ${
                gx ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20' : 'border-[#0070AD]/30 bg-white/80 text-[#0070AD] hover:bg-[#0070AD] hover:text-white'
              }`}
            >
              <span>View all</span>
              <FaPlus className={`text-[9px] group-hover:rotate-90 transition-transform ${gx ? 'text-emerald-400' : ''}`} />
            </button>
          </div>
        ) : showAll && arr.length > 25 ? (
          <div className={`flex items-center justify-end gap-3 border-t px-5 py-3 ${gx ? 'border-emerald-500/10 bg-[#001D2E]' : 'border-[#0070AD]/10 bg-white/40'}`}>
            <button
              type="button"
              onClick={() => setShowAll(false)}
              className={`rounded-full border px-4 py-1.5 text-[11px] font-bold transition-all ${
                gx ? 'border-white/10 bg-white/5 text-white/60 hover:text-white' : 'border-zinc-200 bg-white/80 text-zinc-600 hover:bg-zinc-100'
              }`}
            >
              Show less
            </button>
          </div>
        ) : null}
      </div>
    );
  };

  const LlmUsageFooter = ({ usage }: { usage: Record<string, unknown> }) => {
    const labels: Record<string, string> = {
      router: 'Router',
      nl_sql: 'NL→SQL',
      cleaning_recommendations: 'Cleaning suggestions',
    };
    const segments: string[] = [];
    let combined = 0;
    for (const [step, raw] of Object.entries(usage || {})) {
      if (!raw || typeof raw !== 'object') continue;
      const u = raw as Record<string, unknown>;
      const pt = typeof u.prompt_tokens === 'number' ? u.prompt_tokens : null;
      const ct = typeof u.completion_tokens === 'number' ? u.completion_tokens : null;
      const tt = typeof u.total_tokens === 'number' ? u.total_tokens : null;
      if (typeof tt === 'number') combined += tt;
      const detail =
        typeof tt === 'number'
          ? `${tt} total`
          : [typeof pt === 'number' ? `prompt ${pt}` : '', typeof ct === 'number' ? `completion ${ct}` : '']
              .filter(Boolean)
              .join(', ') || '—';
      segments.push(`${labels[step] ?? step}: ${detail}`);
    }
    if (!segments.length) return null;
    return (
      <p className="mt-2 border-t border-black/[0.06] pt-2 text-[11px] leading-snug text-black/50">
        <span className="font-medium text-black/55">LLM tokens (API usage, approximate): </span>
        {segments.join(' · ')}
        {combined > 0 ? (
          <span className="text-black/45"> · combined total {combined}</span>
        ) : null}
      </p>
    );
  };

  const renderBotContent = (text: string, payload?: any) => {
    const renderStructuredReportFromResult = (result: any) => {
      if (!result || typeof result !== 'object') return null;
      const ui = payload?.ui && typeof payload.ui === 'object' ? payload.ui : {};
      const showCleaning = ui?.show_cleaning === true;
      const showTransform = ui?.show_transform === true;
      const onlyPanel = ui?.only_panel === 'cleaning' ? 'cleaning' : ui?.only_panel === 'transform' ? 'transform' : null;

      if (onlyPanel) {
        return (
          <ReportEnhancements
            result={result}
            userIntent={typeof text === 'string' ? text : ''}
            enableDqRecommendations={onlyPanel === 'cleaning'}
            enableTransformSuggestions={onlyPanel === 'transform'}
            variant="chat"
            gxEnabled={gxEnabled}
          />
        );
      }
      const datasets = result?.datasets && typeof result.datasets === 'object' ? result.datasets : {};
      const dqDatasets =
        result?.data_quality_issues?.datasets && typeof result.data_quality_issues.datasets === 'object'
          ? result.data_quality_issues.datasets
          : {};
      const rels = Array.isArray(result?.relationships) ? result.relationships : [];

      const datasetRows = Object.entries(datasets || {}).map(([name, meta]: any) => {
        const summ = dqDatasets?.[name]?.summary || {};
        return {
          dataset: name,
          rows: meta?.row_count ?? '',
          cols: meta?.column_count ?? '',
          issues: summ?.issue_count ?? 0,
          high: summ?.high_severity ?? 0,
          medium: summ?.medium_severity ?? 0,
          low: summ?.low_severity ?? 0,
          source: sourceRootLabel(meta?.source_root ?? ''),
        };
      });

      const issuesRows: any[] = [];
      for (const [dsName, block] of Object.entries(dqDatasets || {})) {
        const issues = Array.isArray((block as any)?.issues) ? (block as any).issues : [];
        for (const it of issues) {
          issuesRows.push({
            dataset: dsName,
            severity: humanizeSeverityLabel(it?.severity ?? ''),
            type: humanizeSnakeIdentifier(it?.type ?? ''),
            column: it?.column ?? '',
            count: it?.count ?? '-',
            message: it?.message ?? '',
            recommendation: it?.recommendation ?? '',
          });
        }
      }

      const relRows = rels.map((r: any) => ({
        dataset_a: r?.dataset_a ?? r?.from ?? '',
        column_a: r?.column_a ?? '',
        dataset_b: r?.dataset_b ?? r?.to ?? '',
        column_b: r?.column_b ?? '',
        cardinality: r?.cardinality ?? '',
        overlap_count: r?.overlap_count ?? '',
      }));

      return (
        <div className="space-y-8 py-4">
          <div className="space-y-4">
            <div className="flex items-center gap-3 px-1">
              <div className={`flex h-8 w-8 items-center justify-center rounded-xl ${gxEnabled ? 'bg-emerald-500/20 text-emerald-400' : 'bg-[#0070AD]/10 text-[#0070AD]'}`}>
                <FaDatabase className="text-sm" />
              </div>
              <h4 className={`text-[14px] font-black uppercase tracking-widest ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>Registered Datasets</h4>
            </div>
            <JsonTable value={datasetRows} prettyHeaders gx={gxEnabled} />
          </div>

          {issuesRows.length > 0 ? (
            <div className="space-y-4">
              <div className="flex items-center gap-3 px-1">
                <div className={`flex h-8 w-8 items-center justify-center rounded-xl ${gxEnabled ? 'bg-rose-500/20 text-rose-400' : 'bg-rose-500/10 text-rose-600'}`}>
                  <FaExclamationTriangle className="text-sm" />
                </div>
                <h4 className={`text-[14px] font-black uppercase tracking-widest ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>Diagnostic Findings</h4>
              </div>
              <JsonTable value={issuesRows} prettyHeaders gx={gxEnabled} />
            </div>
          ) : null}

          {relRows.length > 0 ? (
            <div className="space-y-4">
              <div className="flex items-center gap-3 px-1">
                <div className={`flex h-8 w-8 items-center justify-center rounded-xl ${gxEnabled ? 'bg-emerald-500/20 text-emerald-400' : 'bg-[#12ABDB]/10 text-[#12ABDB]'}`}>
                  <FaDatabase className="text-sm" />
                </div>
                <h4 className={`text-[14px] font-black uppercase tracking-widest ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>Data Relationships</h4>
              </div>
              <JsonTable value={relRows} prettyHeaders gx={gxEnabled} />
            </div>
          ) : null}

          {result?.gx_results && typeof result.gx_results === 'object' && (
            <div className="space-y-6 pt-6 border-t border-[#0070AD]/10">
              <div className="flex flex-col gap-2">
                <div className="flex items-center gap-3 px-1">
                  <div className={`flex h-8 w-8 items-center justify-center rounded-xl ${gxEnabled ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' : 'bg-[#0070AD]/10 text-[#0070AD]'}`}>
                    <FaCheck className="text-sm" />
                  </div>
                  <h4 className={`text-[14px] font-black uppercase tracking-widest ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>
                    🛡️ Great Expectations Deep Audit
                  </h4>
                </div>
                <div className="mx-1 rounded-xl bg-[#0070AD]/5 p-3 border border-[#0070AD]/10">
                  <p className={`text-[12px] font-medium leading-relaxed ${gxEnabled ? 'text-emerald-300' : 'text-[#0070AD]/80'}`}>
                    <span className="font-bold">Deep Audit Enabled</span>: These insights are generated by the GX Validation Engine, providing high-fidelity verification of data integrity and business rules.
                  </p>
                </div>
              </div>
              
              {Object.entries(result.gx_results).map(([dsName, suite]: [string, any]) => (
                <div key={dsName} className="space-y-3">
                  <div className="flex items-center justify-between px-1">
                  <div className={`text-[13px] font-bold ${gxEnabled ? 'text-zinc-100' : 'text-zinc-900'}`}>{dsName}</div>
                    <div className={`text-[11px] font-black uppercase tracking-widest px-2 py-0.5 rounded-full ${suite.success ? 'bg-emerald-500/10 text-emerald-600' : 'bg-rose-500/10 text-rose-600'}`}>
                      {suite.success ? '✅ Pass' : '❌ Fail'}
                    </div>
                  </div>
                  
                  <JsonTable value={[{
                    "Evaluated": suite.statistics?.evaluated_expectations,
                    "Successful": suite.statistics?.successful_expectations,
                    "Failed": suite.statistics?.unsuccessful_expectations,
                    "Success Rate": `${suite.statistics?.success_percent}%`
                  }]} gx />
                  
                  {suite.results && suite.results.length > 0 && (
                    <JsonTable value={suite.results.map((r: any) => ({
                      "Status": r.success ? '✅' : '❌',
                      "Expectation": r.expectation,
                      "Column": r.column || '-',
                      "Details": r.details
                    }))} gx />
                  )}
                </div>
              ))}
            </div>
          )}

          {(showCleaning || showTransform) && (
            <div className={`pt-6 border-t ${gxEnabled ? 'border-white/10' : 'border-[#0070AD]/10'}`}>
              <ReportEnhancements
                result={result}
                userIntent={typeof text === 'string' ? text : ''}
                enableDqRecommendations={showCleaning}
                enableTransformSuggestions={showTransform}
                variant="chat"
                gxEnabled={gxEnabled}
              />
            </div>
          )}

          <div className={`pt-6 border-t ${gxEnabled ? 'border-emerald-400/20' : 'border-emerald-500/25'}`}>
            <EtlGenerationPanel
              sessionId={sessionId}
              assessment={result as Record<string, unknown>}
              variant="chat"
              darkMode={gxEnabled}
            />
          </div>
        </div>
      );
    };

    // Multi-file row preview: one table per file with the filename above each table.
    if (Array.isArray(payload?.preview_tables) && payload.preview_tables.length > 0) {
      return (
        <div className="space-y-5">
          {(payload.preview_tables as any[]).map((tbl: any, idx: number) => {
            const fname = typeof tbl?.file === 'string' ? tbl.file : `File ${idx + 1}`;
            const rws = Array.isArray(tbl?.rows) ? tbl.rows : [];
            return (
              <div key={`${fname}-${idx}`}>
                <div className={`mb-2 text-[13.5px] font-bold tracking-[0.01em] ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>{fname}</div>
                {rws.length > 0 ? (
                  <JsonTable value={rws} gx={gxEnabled} />
                ) : (
                  <p className="rounded-lg border border-black/15 bg-white/60 px-3 py-2 text-[12.5px] italic text-black/60">
                    End of file — no more rows to show.
                  </p>
                )}
              </div>
            );
          })}
        </div>
      );
    }

    // Prefer structured payload rendering when available (more reliable than parsing strings).
    if (Array.isArray(payload?.rows) && payload.rows.length > 0) {
      return <JsonTable value={payload.rows} gx={gxEnabled} />;
    }

    // Structured report tables are built from `payload.result`; cells are normalized for UI (sources, severity, issue types).
    // (Markdown is still available as a fallback.)
    if (payload?.result && typeof payload.result === 'object' && (payload?.step === 'report' || payload?.report_markdown)) {
      return renderStructuredReportFromResult(payload.result);
    }

    const isMetadataView =
      !!payload?.metadata ||
      (typeof text === 'string' && text.toLowerCase().includes('metadata —')) ||
      (typeof text === 'string' && text.toLowerCase().includes('metadata (selected'));

    // Do not render payload.ui_html as an iframe; prefer chat-native markdown/tables.

    const reportMd = typeof payload?.report_markdown === 'string' ? payload.report_markdown : null;
    if (reportMd && reportMd.trim()) {
      return (
        <ReactMarkdown
          remarkPlugins={[remarkGfm]}
          components={{
            h1: ({ children }) => (
              <h2 className={`text-[15px] font-bold tracking-[0.01em] ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>{children}</h2>
            ),
            h2: ({ children }) => (
              <h3 className={`text-[14px] font-bold tracking-[0.01em] ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>{children}</h3>
            ),
            h3: ({ children }) => <h4 className={`text-[13px] font-semibold ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>{children}</h4>,
            p: ({ children }) => (
              <p className={`text-[13.5px] leading-[1.55] whitespace-pre-wrap ${gxEnabled ? 'text-zinc-100' : 'text-zinc-900'}`}>{children}</p>
            ),
            ul: ({ children }) => <ul className={`list-disc pl-5 text-[13.5px] leading-[1.55] ${gxEnabled ? 'text-zinc-100' : ''}`}>{children}</ul>,
            ol: ({ children }) => <ol className={`list-decimal pl-5 text-[13.5px] leading-[1.55] ${gxEnabled ? 'text-zinc-100' : ''}`}>{children}</ol>,
            li: ({ children }) => <li className="my-1">{children}</li>,
            a: ({ children, href }) => (
              <a
                href={href}
                target="_blank"
                rel="noreferrer"
                className={`font-medium underline underline-offset-2 ${gxEnabled ? 'text-emerald-400 decoration-emerald-400/40 hover:text-emerald-300' : 'text-[#0070AD] decoration-[#0070AD]/40 hover:text-[#12ABDB]'}`}
              >
                {children}
              </a>
            ),
            blockquote: ({ children }) => (
              <blockquote className={`border-l-2 pl-3 text-[13.5px] italic ${gxEnabled ? 'border-emerald-500/40 text-zinc-300' : 'border-[#0070AD]/40 text-black/70'}`}>
                {children}
              </blockquote>
            ),
            code: ({ children }) => (
              <code className={`rounded-md border px-1.5 py-0.5 font-mono text-[12px] ${gxEnabled ? 'border-white/10 bg-white/10 text-emerald-300' : 'border-black/10 bg-white/75 text-zinc-900'}`}>
                {children}
              </code>
            ),
            pre: ({ children }) => (
              <pre className="max-w-full overflow-x-auto rounded-xl border border-black/10 bg-white/70 p-3 font-mono text-[12px] leading-relaxed text-zinc-900 shadow-[0_8px_22px_rgba(0,0,0,0.05)]">
                {children}
              </pre>
            ),
            table: ({ children }) => (
              <div className="my-4 group/table relative max-w-full overflow-hidden rounded-2xl border border-[#0070AD]/30 bg-white/50 shadow-[0_8px_32px_rgba(0,0,0,0.08)] backdrop-blur-md transition-all duration-300 hover:shadow-[0_12px_42px_rgba(0,0,0,0.12)]">
                <div className="max-h-[500px] overflow-auto">
                  <table
                    className={`w-full border-collapse text-[12.5px] leading-tight ${
                      isMetadataView ? '[&>tbody>tr:first-child]:bg-[#0070AD]/10 [&>tbody>tr:first-child]:font-bold' : ''
                    }`}
                  >
                    {children}
                  </table>
                </div>
              </div>
            ),
            thead: ({ children }) => (
              <thead className={`sticky top-0 z-20 backdrop-blur-xl ${gxEnabled ? 'bg-[#001D2E] border-b border-emerald-500/20' : 'bg-gradient-to-r from-[#0070AD]/20 to-[#12ABDB]/20'}`}>
                {children}
              </thead>
            ),
            th: ({ children }) => (
              <th className={`border-b border-[#0070AD]/20 border-r border-[#0070AD]/10 px-4 py-3 text-left font-bold uppercase tracking-wider last:border-r-0 ${gxEnabled ? 'text-emerald-400' : 'text-[#0070AD]'}`}>
                {children}
              </th>
            ),
            td: ({ children }) => (
              <td className="border-b border-[#0070AD]/10 border-r border-[#0070AD]/5 px-4 py-2.5 align-top last:border-r-0">
                <div className={`max-w-[560px] whitespace-pre-wrap break-words leading-relaxed font-medium ${gxEnabled ? 'text-zinc-100' : 'text-zinc-800'}`}>
                  {children}
                </div>
              </td>
            ),
          }}
        >
          {reportMd}
        </ReactMarkdown>
      );
    }

    const reportHtml = typeof payload?.report_html === 'string' ? payload.report_html : null;
    if (reportHtml && reportHtml.trim()) {
      return (
        <div className="h-[78vh] w-full overflow-hidden rounded-xl border border-black/10 bg-white shadow-[0_8px_22px_rgba(0,0,0,0.05)]">
          <iframe
            title="Assessment report"
            className="h-full w-full"
            srcDoc={reportHtml}
            sandbox="allow-scripts allow-same-origin"
          />
        </div>
      );
    }

    const parsed = tryParseJson(text);
    if (parsed !== null) return <JsonTable value={parsed} gx={gxEnabled} />;

    return (
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          h1: ({ children }) => (
            <h2 className={`text-[15px] font-bold tracking-[0.01em] ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>{children}</h2>
          ),
          h2: ({ children }) => (
            <h3 className={`text-[14px] font-bold tracking-[0.01em] ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>{children}</h3>
          ),
          h3: ({ children }) => <h4 className={`text-[13px] font-semibold ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>{children}</h4>,
          p: ({ children }) => (
            <p className={`text-[13.5px] leading-[1.55] whitespace-pre-wrap ${gxEnabled ? 'text-zinc-100' : 'text-zinc-900'}`}>{children}</p>
          ),
          ul: ({ children }) => <ul className={`list-disc pl-5 text-[13.5px] leading-[1.55] ${gxEnabled ? 'text-zinc-100' : ''}`}>{children}</ul>,
          ol: ({ children }) => <ol className={`list-decimal pl-5 text-[13.5px] leading-[1.55] ${gxEnabled ? 'text-zinc-100' : ''}`}>{children}</ol>,
          li: ({ children }) => <li className="my-1">{children}</li>,
          a: ({ children, href }) => (
            <a
              href={href}
              target="_blank"
              rel="noreferrer"
              className={`font-medium underline underline-offset-2 ${gxEnabled ? 'text-emerald-400 decoration-emerald-400/40 hover:text-emerald-300' : 'text-[#0070AD] decoration-[#0070AD]/40 hover:text-[#12ABDB]'}`}
            >
              {children}
            </a>
          ),
          blockquote: ({ children }) => (
            <blockquote className={`border-l-2 pl-3 text-[13.5px] italic ${gxEnabled ? 'border-emerald-500/40 text-zinc-300' : 'border-[#0070AD]/40 text-black/70'}`}>
              {children}
            </blockquote>
          ),
          code: ({ children }) => (
            <code className={`rounded-md border px-1.5 py-0.5 font-mono text-[12px] ${gxEnabled ? 'border-white/10 bg-white/10 text-emerald-300' : 'border-black/10 bg-white/75 text-zinc-900'}`}>
              {children}
            </code>
          ),
          pre: ({ children }) => (
            <pre className={`max-w-full overflow-x-auto rounded-xl border p-3 font-mono text-[12px] leading-relaxed shadow-[0_8px_22px_rgba(0,0,0,0.05)] ${gxEnabled ? 'border-white/10 bg-[#001D2E]/50 text-zinc-100' : 'border-black/10 bg-white/70 text-zinc-900'}`}>
              {children}
            </pre>
          ),
          table: ({ children }) => (
            <div className={`my-4 group/table relative max-w-full overflow-hidden rounded-2xl border bg-white/50 shadow-[0_8px_32px_rgba(0,0,0,0.08)] backdrop-blur-md transition-all duration-300 hover:shadow-[0_12px_42px_rgba(0,0,0,0.12)] ${gxEnabled ? 'border-emerald-500/50' : 'border-[#0070AD]/30'}`}>
              {gxEnabled && (
                <div className="absolute right-2 top-2 z-30 flex items-center gap-1 rounded-md bg-emerald-500/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-tighter text-emerald-600 border border-emerald-500/20">
                  <FaCheck className="text-[7px]" />
                  GX
                </div>
              )}
              <div className="max-h-[500px] overflow-auto">
                <table
                  className={`w-full border-collapse text-[12.5px] leading-tight ${
                    isMetadataView ? '[&>tbody>tr:first-child]:bg-[#0070AD]/10 [&>tbody>tr:first-child]:font-bold' : ''
                  }`}
                >
                  {children}
                </table>
              </div>
            </div>
          ),
          thead: ({ children }) => (
            <thead className={`sticky top-0 z-20 backdrop-blur-xl ${gxEnabled ? 'bg-[#001D2E] border-b border-emerald-500/20' : 'bg-gradient-to-r from-[#0070AD]/20 to-[#12ABDB]/20'}`}>
              {children}
            </thead>
          ),
          th: ({ children }) => (
            <th className={`border-b border-[#0070AD]/20 border-r border-[#0070AD]/10 px-4 py-3 text-left font-bold uppercase tracking-wider last:border-r-0 ${gxEnabled ? 'text-emerald-400' : 'text-[#0070AD]'}`}>
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td className="border-b border-[#0070AD]/10 border-r border-[#0070AD]/5 px-4 py-2.5 align-top last:border-r-0">
              <div className={`max-w-[560px] whitespace-pre-wrap break-words leading-relaxed font-medium ${gxEnabled ? 'text-zinc-100' : 'text-zinc-800'}`}>
                {children}
              </div>
            </td>
          ),
        }}
      >
        {text}
      </ReactMarkdown>
    );
  };

  const renderValidation = (payload?: any) => {
    const v = payload?.validation;
    if (!v || typeof v !== 'object') return null;
    const title = typeof v.title === 'string' ? v.title : 'Validation';
    const ok = Boolean(v.ok);
    const checks = Array.isArray(v.checks) ? v.checks : [];
    if (checks.length === 0) return null;
    return (
      <details className={`rounded-xl border px-3 py-2 text-[12px] shadow-[0_8px_22px_rgba(0,0,0,0.05)] ${gxEnabled ? 'border-white/10 bg-[#001D2E]/50 text-zinc-100' : 'border-black/10 bg-white/70 text-zinc-900'}`}>
        <summary className="cursor-pointer select-none font-semibold">
          {title}: <span className={ok ? 'text-emerald-700' : 'text-rose-700'}>{ok ? 'OK' : 'Needs review'}</span>
        </summary>
        <div className="mt-2 space-y-1">
          {checks.map((c: any, idx: number) => {
            const cid = typeof c?.id === 'string' ? c.id : `check_${idx}`;
            const cok = Boolean(c?.ok);
            const detail = typeof c?.detail === 'string' ? c.detail : '';
            return (
              <div key={cid} className="flex gap-2">
                <div className={`mt-[2px] h-2.5 w-2.5 rounded-full ${cok ? 'bg-emerald-500' : 'bg-rose-500'}`} />
                <div className="min-w-0">
                  <div className="font-mono text-[11px] text-black/60">{cid}</div>
                  {detail ? <div className="whitespace-pre-wrap break-words leading-relaxed">{detail}</div> : null}
                </div>
              </div>
            );
          })}
        </div>
      </details>
    );
  };

  const renderRawBackendPayload = (payload?: any) => {
    if (!payload || typeof payload !== 'object') return null;
    const hasAny =
      payload?.result !== undefined ||
      payload?.schemas !== undefined ||
      payload?.metadata !== undefined ||
      payload?.rows !== undefined ||
      payload?.preview_tables !== undefined;
    if (!hasAny) return null;
    return (
      <details className="group/details mt-4 rounded-2xl border border-[#0070AD]/20 bg-white/40 shadow-sm backdrop-blur-sm transition-all overflow-hidden">
        <summary className="flex cursor-pointer select-none items-center justify-between px-5 py-3 transition-colors hover:bg-[#0070AD]/5">
          <div className="flex items-center gap-3">
            <div className="flex h-6 w-6 items-center justify-center rounded-lg bg-[#0070AD]/10 text-[#0070AD]">
              <FaCode className="text-[10px]" />
            </div>
            <span className={`text-[11px] font-black uppercase tracking-widest ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>System Diagnostics</span>
          </div>
          <div className="text-[10px] text-black/30 group-open/details:rotate-180 transition-transform">▼</div>
        </summary>
        <div className="space-y-6 border-t border-[#0070AD]/10 p-5 bg-white/20">
          {payload?.schemas !== undefined ? (
            <div className="space-y-3">
              <div className="text-[10px] font-black uppercase tracking-widest text-black/40 px-1">Logical Schema Map</div>
              <JsonTable value={payload.schemas} gx={gxEnabled} />
            </div>
          ) : null}
          {payload?.metadata !== undefined ? (
            <div className="space-y-3">
              <div className="text-[10px] font-black uppercase tracking-widest text-black/40 px-1">Source Metadata</div>
              <JsonTable value={payload.metadata} gx={gxEnabled} />
            </div>
          ) : null}
          {Array.isArray(payload?.preview_tables) && payload.preview_tables.length > 0 ? (
            <div className="space-y-3">
              <div className="text-[10px] font-black uppercase tracking-widest text-black/40 px-1">Table Previews (RAW)</div>
              <pre className="max-h-[300px] overflow-auto whitespace-pre-wrap break-words rounded-xl border border-black/10 bg-black/5 p-4 font-mono text-[10px] leading-relaxed text-zinc-700">
                {JSON.stringify(payload.preview_tables, null, 2)}
              </pre>
            </div>
          ) : null}
          {payload?.rows !== undefined ? (
            <div className="space-y-3">
              <div className="text-[10px] font-black uppercase tracking-widest text-black/40 px-1">Dataset Sample</div>
              <JsonTable value={payload.rows} gx={gxEnabled} />
            </div>
          ) : null}
          {payload?.result !== undefined ? (
            <div className="space-y-3">
              <div className="text-[10px] font-black uppercase tracking-widest text-black/40 px-1">Pipeline Result</div>
              <JsonTable value={payload.result} gx={gxEnabled} />
            </div>
          ) : null}
        </div>
      </details>
    );
  };

  const beginEdit = (m: Message) => {
    setEditingId(m.id);
    setEditingText(m.text);
  };

  const cancelEdit = () => {
    setEditingId(null);
    setEditingText('');
  };

  const saveEditAndRegenerate = async () => {
    if (!editingId) return;
    const newText = editingText.trim();
    if (!newText) return;

    // Build a truncated conversation up to the edited message, replace that message, and regenerate.
    const idx = messages.findIndex((m) => m.id === editingId);
    if (idx < 0) return;
    const editedMsg = { ...messages[idx], text: newText };
    const base = [...messages.slice(0, idx), editedMsg];
    setMessages(base);
    cancelEdit();

    setIsLoadingAgent(true);
    setAgentError(null);
    try {
      const { content, threadId } = await fetchAgentReply(base);
      if (threadId) setAgentThreadId(threadId);
      setMessages((prev) => [
        ...prev,
        { id: (Date.now() + 1).toString(), text: content, sender: 'bot', timestamp: new Date() },
      ]);
    } catch (err: any) {
      const isAbort = err?.name === 'AbortError';
      if (!isAbort) {
        setAgentError("Couldn't reach the agent to regenerate. Check backend/agent configuration and try again.");
      }
    } finally {
      setIsLoadingAgent(false);
    }
  };

  /** Scroll only the messages panel — avoid scrollIntoView (it scrolls ancestor windows and can hide the chat header). */
  const scrollMessagesToBottom = () => {
    const el = messagesScrollRef.current;
    if (!el) return;
    requestAnimationFrame(() => {
      el.scrollTop = el.scrollHeight;
    });
  };

  useEffect(() => {
    scrollMessagesToBottom();
  }, [messages, isLoadingAgent]);

  const handleBack = () => {
    if (hasSelectedDataSource) {
      setHasSelectedDataSource(false);
      setSelectedDataSource(null);
      setGuidedMode('none');
      const userMessage: Message = {
        id: Date.now().toString(),
        text: '← Back',
        sender: 'user',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, userMessage]);
    }
    setShowOptions(false);
  };

  const sendUserText = async (text: string, opts?: { silent?: boolean }) => {
    const silent = Boolean(opts?.silent);
    const userMessage: Message = {
      id: Date.now().toString(),
      text,
      sender: 'user',
      timestamp: new Date(),
    };
    if (!silent) {
      setMessages((prev) => [...prev, userMessage]);
    }
    setIsLoadingAgent(true);
    setAgentError(null);
    try {
      const messagesWithUser = silent ? [...messages, userMessage] : [...messages, userMessage];
      const { content, threadId, payload } = await fetchAgentReply(messagesWithUser);
      if (threadId) setAgentThreadId(threadId);

      // If the user typed "blob"/"sql"/etc. directly (instead of selecting via UI),
      // the backend may return files/tables without the UI having a selectedDataSource yet.
      // Infer source from payload shape to ensure our interactive buttons send correct commands.
      let inferredSource: string | null = null;
      if (payload && typeof payload === 'object') {
        const hasFiles = Array.isArray((payload as any)?.files);
        const hasTables = Array.isArray((payload as any)?.tables);
        const hasRoot = typeof (payload as any)?.root === 'string' && String((payload as any).root).length > 0;
        if (hasTables) inferredSource = 'sql';
        else if (hasFiles) inferredSource = hasRoot ? 'streams' : 'blob';
      }

      const effectiveSource = selectedDataSource || inferredSource;
      if (!selectedDataSource && inferredSource) {
        setSelectedDataSource(inferredSource);
        setHasSelectedDataSource(true);
      }

      const interactiveOptions: Message['options'] = Array.isArray(payload?.options)
        ? payload.options.map((o: any, i: number) => ({
            id: String(o?.id ?? `opt-${i}`),
            text: String(o?.text ?? ''),
            send: String(o?.send ?? ''),
          })).filter((o: any) => o.text && o.send)
        : Array.isArray(payload?.tables)
        ? (() => {
            const items = payload.tables.slice(0, 50).map((t: any, i: number) => {
              const oneBased = Number(t?.index ?? i) + 1;
              const name = String(t?.name ?? t ?? `Table ${oneBased}`);
              return {
                id: `table-${oneBased}`,
                text: `${oneBased}. ${name}`,
                send: `__toggle_table__:${oneBased}`,
              };
            });
            const nums = items
              .map((x: { send: string }) => Number(String(x.send).split(':')[1]))
              .filter((n: number) => Number.isFinite(n) && n > 0);
            if (nums.length > 1) {
              items.push({
                id: 'select-all-tables',
                text: 'Select all',
                send: `__select_all_tables__:${nums.join(',')}`,
              });
            }
            return items;
          })()
        : Array.isArray(payload?.files)
          ? (() => {
              const mode: 'blob' | 'local' = effectiveSource === 'blob' ? 'blob' : 'local';
              const items = payload.files.slice(0, 30).map((f: any, i: number) => ({
                id: `file-${i + 1}`,
                text: `${i + 1}. ${String(f)}`,
                send: `__toggle_file__:${mode}:${i + 1}`,
              }));
              const nums = items
                .map((x: { send: string }) => Number(String(x.send).split(':')[2]))
                .filter((n: number) => Number.isFinite(n) && n > 0);
              if (nums.length > 1) {
                items.push({
                  id: 'select-all-files',
                  text: 'Select all',
                  send: `__select_all_files__:${mode}:${nums.join(',')}`,
                });
              }
              return items;
            })()
          : undefined;

      const botResponse: Message = {
        id: (Date.now() + 1).toString(),
        text: content,
        sender: 'bot',
        timestamp: new Date(),
        options: interactiveOptions,
        payload,
      };
      if (silent) {
        // Replace the last bot message (the one that had the refresh button),
        // so the UI updates without echoing a "list tables" user bubble.
        setMessages((prev) => {
          if (!prev.length) return [botResponse];
          const lastBotIdx = [...prev].reverse().findIndex((m) => m.sender === 'bot');
          if (lastBotIdx < 0) return [...prev, botResponse];
          const idxFromStart = prev.length - 1 - lastBotIdx;
          return prev.map((m, i) => (i === idxFromStart ? botResponse : m));
        });
      } else {
        setMessages((prev) => [...prev, botResponse]);
      }
    } catch (err: any) {
      const msg = err?.message || "Couldn't reach the agent.";
      setAgentError(`${msg} Check your backend/agent configuration and try again.`);
    } finally {
      setIsLoadingAgent(false);
    }
  };

  const isToggleTableOption = (send: string) => String(send || '').startsWith('__toggle_table__:');

  const isSelectAllTablesOption = (send: string) => String(send || '').startsWith('__select_all_tables__:');

  const isToggleFileOption = (send: string) => String(send || '').startsWith('__toggle_file__:');
  const isSelectAllFilesOption = (send: string) => String(send || '').startsWith('__select_all_files__:');

  const toggleTableForMessage = (messageId: string, tableNumber: number) => {
    setPendingTableSelections((prev) => {
      const existing = Array.isArray(prev[messageId]) ? prev[messageId] : [];
      const has = existing.includes(tableNumber);
      const next = has ? existing.filter((x) => x !== tableNumber) : [...existing, tableNumber];
      next.sort((a, b) => a - b);
      return { ...prev, [messageId]: next };
    });
  };

  const toggleFileForMessage = (messageId: string, mode: 'blob' | 'local', fileNumber: number) => {
    setPendingFileSelections((prev) => {
      const cur = prev[messageId];
      const existing = cur?.mode === mode && Array.isArray(cur.selected) ? cur.selected : [];
      const has = existing.includes(fileNumber);
      const next = has ? existing.filter((x) => x !== fileNumber) : [...existing, fileNumber];
      next.sort((a, b) => a - b);
      return { ...prev, [messageId]: { mode, selected: next } };
    });
  };

  const setAllFilesForMessage = (messageId: string, mode: 'blob' | 'local', all: number[]) => {
    setPendingFileSelections((prev) => {
      const cur = prev[messageId];
      const existing = cur?.mode === mode && Array.isArray(cur.selected) ? cur.selected : [];
      const isAllSelected = all.length > 0 && existing.length === all.length && all.every((n) => existing.includes(n));
      return { ...prev, [messageId]: { mode, selected: isAllSelected ? [] : [...all] } };
    });
  };

  const setAllTablesForMessage = (messageId: string, all: number[]) => {
    setPendingTableSelections((prev) => {
      const existing = Array.isArray(prev[messageId]) ? prev[messageId] : [];
      const isAllSelected = all.length > 0 && existing.length === all.length && all.every((n) => existing.includes(n));
      return { ...prev, [messageId]: isAllSelected ? [] : [...all] };
    });
  };

  const confirmTablesForMessage = async (messageId: string) => {
    const selected = pendingTableSelections[messageId] || [];
    if (!selected.length) return;
    // Use multi-select command even for a single selection.
    await sendUserText(`select tables ${selected.join(',')}`);
    setPendingTableSelections((prev) => {
      const copy = { ...prev };
      delete copy[messageId];
      return copy;
    });
  };

  const confirmFilesForMessage = async (messageId: string) => {
    const cur = pendingFileSelections[messageId];
    const selected = cur?.selected || [];
    const mode = cur?.mode || 'blob';
    if (!selected.length) return;
    const cmd =
      mode === 'blob' ? `select files ${selected.join(',')}` : `select local files ${selected.join(',')}`;
    await sendUserText(cmd);
    setPendingFileSelections((prev) => {
      const copy = { ...prev };
      delete copy[messageId];
      return copy;
    });
  };

  const handleOptionSelect = async (option: ChatOption) => {
    const userMessage: Message = {
      id: Date.now().toString(),
      text: option.text,
      sender: 'user',
      timestamp: new Date(),
    };

    const isDataSourceSelection = DATA_SOURCE_OPTIONS.some(ds => ds.id === option.id);
    if (isDataSourceSelection) {
      setHasSelectedDataSource(true);
      setSelectedDataSource(option.id);
      setGuidedMode('none');
    }

    setMessages((prev) => [...prev, userMessage]);

    // Local data: open folder picker (dynamic path selection)
    if (option.id === 'local') {
      // Ensure user sees the local panel options, but open explorer immediately.
      setHasSelectedDataSource(true);
      setSelectedDataSource('local');
      setShowOptions(false);
      setTimeout(() => localFolderInputRef.current?.click(), 0);
      return;
    }

    // Redirect to data pipeline without calling agent
    if (option.action === '/data-pipeline') {
      setTimeout(() => {
        window.location.href = '/data-pipeline';
      }, 1500);
      return;
    }

    // Guided UX: for SQL/Blob/Streams/Local, turn "View" and "Report" into deterministic steps.
    if (option.action === 'guided-view') {
      setGuidedMode('view');
      // Ask backend to list selectable entities
      const cmd =
        option.id.startsWith('sql') ? 'list tables' : option.id.startsWith('blob') ? 'list files' : 'list local files';
      await sendUserText(cmd);
      return;
    }
    if (option.action === 'guided-report') {
      setGuidedMode('report');
      const cmd =
        option.id.startsWith('sql') ? 'list tables' : option.id.startsWith('blob') ? 'list files' : 'list local files';
      await sendUserText(cmd);
      return;
    }

    await sendUserText(option.text);
  };

  return (
    <div className="flex h-full min-h-0 min-w-0 w-full flex-1 flex-col bg-transparent">
      {/* Top chrome only (borders / bar) — no title text; keeps alignment with Sign out on large screens */}
      <div
        className="shrink-0 w-full border-y border-[#0070AD]/20 bg-gradient-to-r from-[#0070AD]/10 via-white/60 to-[#12ABDB]/10 pt-4 pb-6 backdrop-blur-xl lg:pr-24"
        aria-hidden
      />

      {/* Chat Messages Area — basis-0 + flex-1 fills space above composer */}
      <div
        ref={messagesScrollRef}
        className="min-h-0 flex-1 basis-0 space-y-4 overflow-y-auto overflow-x-hidden p-6 bg-gradient-to-b from-white/40 via-white/10 to-[#0070AD]/10"
      >
        <AnimatePresence initial={false}>
          {messages.map((message, idx) => {
            const isUser = message.sender === 'user';
            const isHtmlReportMessage = false;
            const staggerDelay = Math.min(idx * 0.04, 0.2);
            const payload = (message as any)?.payload;
            const baseOpts = Array.isArray(message.options) ? message.options : [];
            const opts = (() => {
              const out = [...baseOpts];
              // Screen 1: data source selection — add Refresh Sources (restarts flow).
              if (message.sender === 'bot' && payload?.step === 'data_source') {
                if (!out.some((o) => String(o?.id) === 'refresh-sources')) {
                  out.push({ id: 'refresh-sources', text: 'Refresh sources', send: '__refresh_sources__' });
                }
              }
              return out;
            })();
            const hasOptions = message.sender === 'bot' && opts.length > 0;
            const hasTableToggles = hasOptions && opts.some((o) => isToggleTableOption(o.send));
            const hasFileToggles = hasOptions && opts.some((o) => isToggleFileOption(o.send));
            // Screen 2: table selection — add Refresh Tables.
            const optsWithRefresh = (() => {
              if (!hasOptions) return opts;
              if (!hasTableToggles) return opts;
              if (opts.some((o) => String(o?.id) === 'refresh-tables')) return opts;
              return [...opts, { id: 'refresh-tables', text: 'Refresh tables', send: '__refresh_tables__' }];
            })();
            const selectAllOpt = hasOptions ? opts.find((o) => isSelectAllTablesOption(o.send)) : undefined;
            const allTableNumbers =
              selectAllOpt && String(selectAllOpt.send).includes(':')
                ? String(selectAllOpt.send)
                    .split(':')[1]
                    .split(',')
                    .map((x) => Number(x))
                    .filter((x) => Number.isFinite(x) && x > 0)
                : [];
            const selectAllFilesOpt = hasOptions ? opts.find((o) => isSelectAllFilesOption(o.send)) : undefined;
            const parsedFiles =
              selectAllFilesOpt && String(selectAllFilesOpt.send).split(':').length >= 3
                ? (() => {
                    const parts = String(selectAllFilesOpt.send).split(':');
                    const mode = (parts[1] as any) === 'blob' ? ('blob' as const) : ('local' as const);
                    const nums = (parts[2] || '')
                      .split(',')
                      .map((x) => Number(x))
                      .filter((x) => Number.isFinite(x) && x > 0);
                    return { mode, nums };
                  })()
                : null;
            const selectedTables = pendingTableSelections[message.id] || [];
            const selectedFiles = pendingFileSelections[message.id]?.selected || [];
            const fileMode = pendingFileSelections[message.id]?.mode || parsedFiles?.mode || 'blob';
            const hasBackOpt = hasOptions && opts.some((o) => String(o?.send || '').trim().toLowerCase() === 'back');
            return (
            <motion.div
              key={message.id}
              initial={{ opacity: 0, x: isUser ? 40 : -40, scale: 0.95 }}
              animate={{ opacity: 1, x: 0, scale: 1 }}
              exit={{ opacity: 0, x: isUser ? 20 : -20, scale: 0.98 }}
              transition={{ duration: 0.4, ease: [0.25, 0.46, 0.45, 0.94], delay: staggerDelay }}
              className={`group flex ${isUser ? 'justify-end' : 'justify-start'}`}
            >
            <div className={isHtmlReportMessage ? 'w-full max-w-full' : 'max-w-[78%]'}>
              {message.sender === 'bot' && (
                <div className="flex items-center gap-2 mb-1.5 ml-1">
                  <span className={`text-[10px] font-black uppercase tracking-widest ${gxEnabled ? 'text-emerald-400' : 'text-zinc-500 opacity-60'}`}>
                    {gxEnabled ? '✨ GX AUDIT AGENT' : 'AGENT DHARA'}
                  </span>
                  {gxEnabled && (
                    <div className="flex items-center gap-1 rounded-full bg-emerald-500/10 px-2 py-0.5 text-[8px] font-black uppercase tracking-tighter text-emerald-500 border border-emerald-500/20">
                      <FaCheck className="text-[7px]" />
                      Verified
                    </div>
                  )}
                </div>
              )}
              <motion.div
                  initial={{ scale: 0.92, opacity: 0 }}
                  animate={{ scale: 1, opacity: 1 }}
                  transition={{ duration: 0.35, delay: 0.05 + staggerDelay, ease: 'easeOut' }}
                  className={`rounded-lg border px-4 py-3 ${
                    message.sender === 'user'
                      ? 'border-[#0070AD]/40 bg-gradient-to-r from-[#0070AD] to-[#12ABDB] text-white shadow-[0_10px_30px_rgba(0,112,173,0.18)]'
                      : isHtmlReportMessage
                        ? 'border-[#0070AD]/25 bg-transparent text-zinc-900 p-0'
                        : 'border-[#0070AD]/25 bg-gradient-to-r from-[#0070AD]/10 to-[#12ABDB]/10 text-zinc-900'
                  } ${gxEnabled && message.sender === 'bot' ? 'border-emerald-500/30 bg-[#001D2E]/95 text-white shadow-[0_4px_20px_rgba(16,185,129,0.1)]' : ''}`}
                >
                  {editingId === message.id ? (
                    <div className="space-y-2">
                      <textarea
                        value={editingText}
                        onChange={(e) => setEditingText(e.target.value)}
                        className="w-full resize-none rounded-lg border border-black/10 bg-white/85 px-3 py-2 text-sm text-zinc-900 outline-none focus:border-[#0070AD]/40 focus:ring-2 focus:ring-[#0070AD]/20"
                        rows={3}
                      />
                      <div className="flex justify-end gap-2">
                        <button
                          type="button"
                          onClick={cancelEdit}
                          className="rounded-lg border border-black/10 bg-white/85 px-3 py-1.5 text-xs font-semibold text-black/70 hover:bg-white"
                        >
                          Cancel
                        </button>
                        <button
                          type="button"
                          onClick={saveEditAndRegenerate}
                          className="rounded-lg border border-[#0070AD]/40 bg-[#0070AD] px-3 py-1.5 text-xs font-semibold text-white hover:bg-[#12ABDB]"
                        >
                          Save & regenerate
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {message.sender === 'bot' ? (
                        <div className="space-y-2">
                          {renderBotContent(message.text, (message as any).payload)}
                          {(message as any).payload?.llm_usage &&
                          typeof (message as any).payload.llm_usage === 'object' ? (
                            <LlmUsageFooter usage={(message as any).payload.llm_usage} />
                          ) : null}
                          {showDebugPanels ? renderValidation((message as any).payload) : null}
                          {showDebugPanels ? renderRawBackendPayload((message as any).payload) : null}
                        </div>
                      ) : (
                        <p className="text-sm leading-relaxed whitespace-pre-wrap">{message.text}</p>
                      )}
                      {hasOptions && (
                        <div className="flex flex-wrap gap-2.5">
                          {optsWithRefresh.map((opt) => (
                            <button
                              key={opt.id}
                              type="button"
                              onClick={() => {
                                if (isToggleTableOption(opt.send)) {
                                  const n = Number(String(opt.send).split(':')[1]);
                                  if (Number.isFinite(n) && n > 0) toggleTableForMessage(message.id, n);
                                  return;
                                }
                                if (isSelectAllTablesOption(opt.send)) {
                                  setAllTablesForMessage(message.id, allTableNumbers);
                                  return;
                                }
                                if (isToggleFileOption(opt.send)) {
                                  const parts = String(opt.send).split(':');
                                  const mode = (parts[1] as any) === 'blob' ? ('blob' as const) : ('local' as const);
                                  const n = Number(parts[2]);
                                  if (Number.isFinite(n) && n > 0) toggleFileForMessage(message.id, mode, n);
                                  return;
                                }
                                if (isSelectAllFilesOption(opt.send) && parsedFiles) {
                                  setAllFilesForMessage(message.id, parsedFiles.mode, parsedFiles.nums);
                                  return;
                                }
                                // Silent refresh actions (do not echo commands into chat).
                                if (String(opt.send) === '__refresh_tables__') {
                                  sendUserText('list tables', { silent: true });
                                  return;
                                }
                                if (String(opt.send) === '__refresh_sources__') {
                                  sendUserText('restart', { silent: true });
                                  return;
                                }
                                // Remove options from the previous message once a choice is made.
                                setMessages((prev) =>
                                  prev.map((m) => (m.id === message.id ? { ...m, options: undefined } : m))
                                );
                                sendUserText(opt.send);
                              }}
                              className={`group relative flex items-center gap-2 overflow-hidden rounded-xl border px-4 py-2.5 text-left text-[12px] font-bold tracking-tight transition-all duration-300 active:scale-95 ${
                                hasTableToggles && isToggleTableOption(opt.send)
                                  ? selectedTables.includes(Number(String(opt.send).split(':')[1]))
                                    ? gxEnabled ? 'border-emerald-500 bg-emerald-500 text-black shadow-lg shadow-emerald-500/20' : 'border-[#0070AD] bg-gradient-to-r from-[#0070AD] to-[#12ABDB] text-white shadow-lg shadow-[#0070AD]/20'
                                    : gxEnabled ? 'border-white/10 bg-[#001D2E]/80 text-white hover:border-emerald-500/50 hover:bg-emerald-500/10' : 'border-[#0070AD]/20 bg-white/80 text-zinc-800 hover:border-[#0070AD]/50 hover:bg-white shadow-sm'
                                  : hasFileToggles && isToggleFileOption(opt.send)
                                    ? selectedFiles.includes(Number(String(opt.send).split(':')[2]))
                                      ? gxEnabled ? 'border-emerald-500 bg-emerald-500 text-black shadow-lg shadow-emerald-500/20' : 'border-[#0070AD] bg-gradient-to-r from-[#0070AD] to-[#12ABDB] text-white shadow-lg shadow-[#0070AD]/20'
                                      : gxEnabled ? 'border-white/10 bg-[#001D2E]/80 text-white hover:border-emerald-500/50 hover:bg-emerald-500/10' : 'border-[#0070AD]/20 bg-white/80 text-zinc-800 hover:border-[#0070AD]/50 hover:bg-white shadow-sm'
                                    : gxEnabled ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20 shadow-lg' : 'border-[#0070AD]/30 bg-white shadow-md hover:shadow-lg hover:border-[#0070AD] hover:-translate-y-0.5 text-[#0070AD]'
                              }`}
                              title={opt.send}
                            >
                              <div className="absolute inset-0 bg-white/0 transition-colors group-hover:bg-white/5" />
                              <span className="relative z-10">{opt.text}</span>
                              {opt.id.includes('report') && <FaPaperPlane className="relative z-10 text-[10px] opacity-70" />}
                              {opt.id.includes('view') && <FaDatabase className="relative z-10 text-[10px] opacity-70" />}
                            </button>
                          ))}
                          {(hasTableToggles || hasFileToggles) && (
                            <button
                              type="button"
                              disabled={(hasTableToggles ? selectedTables.length === 0 : selectedFiles.length === 0)}
                              onClick={() =>
                                hasTableToggles
                                  ? confirmTablesForMessage(message.id)
                                  : confirmFilesForMessage(message.id)
                              }
                              className={`group flex items-center gap-2 rounded-xl border-2 px-5 py-2.5 text-center text-[12px] font-black uppercase tracking-widest transition-all disabled:cursor-not-allowed disabled:opacity-30 disabled:grayscale ${
                                gxEnabled ? 'border-emerald-500 bg-[#001D2E] text-emerald-400 hover:bg-emerald-500 hover:text-black' : 'border-[#0070AD] bg-white text-[#0070AD] hover:bg-[#0070AD] hover:text-white'
                              }`}
                            >
                              <span>Confirm Selection</span>
                              <div className={`flex h-5 w-5 items-center justify-center rounded-full text-[10px] transition-colors ${gxEnabled ? 'bg-emerald-500 text-black group-hover:bg-black group-hover:text-emerald-400' : 'bg-[#0070AD] text-white group-hover:bg-white group-hover:text-[#0070AD]'}`}>
                                {hasTableToggles ? selectedTables.length : selectedFiles.length}
                              </div>
                            </button>
                          )}
                          {!hasBackOpt && (
                             <button
                              type="button"
                              onClick={() => {
                                setMessages((prev) =>
                                  prev.map((m) => (m.id === message.id ? { ...m, options: undefined } : m))
                                );
                                sendUserText('back');
                              }}
                              className={`flex items-center gap-2 rounded-xl border px-4 py-2.5 text-[12px] font-bold transition-all ${
                                gxEnabled ? 'border-white/10 bg-[#001D2E]/80 text-white/60 hover:text-white hover:bg-white/5' : 'border-zinc-200 bg-white/90 text-zinc-500 hover:bg-zinc-50 hover:text-zinc-800'
                              }`}
                            >
                              <FaArrowLeft className="text-[10px]" />
                              <span>Back</span>
                            </button>
                          )}
                        </div>
                      )}
                    </div>
                  )}
                  <span className={`mt-2 block text-xs ${message.sender === 'user' ? 'text-white/80' : 'text-black/45'}`}>
                    {formatTime(message.timestamp)}
                  </span>
                </motion.div>

                {/* Message actions */}
                {editingId !== message.id && (
                  <div className={`mt-1 flex items-center gap-2 text-[11px] opacity-0 transition-opacity group-hover:opacity-100 ${isUser ? 'justify-end' : 'justify-start'}`}>
                    {message.sender === 'bot' ? (
                      <>
                         <button
                          type="button"
                          onClick={() => copyText(message.text)}
                          className={`inline-flex items-center gap-1 rounded-md border px-2 py-1 transition-colors ${
                            gxEnabled ? 'border-white/10 bg-[#001D2E]/80 text-white/60 hover:text-emerald-400 hover:bg-[#001D2E]' : 'border-black/10 bg-white/85 text-black/70 hover:bg-white'
                          }`}
                          title="Copy"
                        >
                          <FaCopy />
                          Copy
                        </button>
                        <button
                          type="button"
                          onClick={() => shareText(message.text)}
                          className={`inline-flex items-center gap-1 rounded-md border px-2 py-1 transition-colors ${
                            gxEnabled ? 'border-white/10 bg-[#001D2E]/80 text-white/60 hover:text-emerald-400 hover:bg-[#001D2E]' : 'border-black/10 bg-white/85 text-black/70 hover:bg-white'
                          }`}
                          title="Share"
                        >
                          <FaShareAlt />
                          Share
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        onClick={() => beginEdit(message)}
                        className="inline-flex items-center gap-1 rounded-md border border-black/10 bg-white/85 px-2 py-1 text-black/70 hover:bg-white"
                        title="Edit"
                      >
                        <FaEdit />
                        Edit
                      </button>
                    )}
                  </div>
                )}
              </div>
            </motion.div>
            );
          })}
        </AnimatePresence>
        {isLoadingAgent && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="flex flex-col items-start gap-2"
          >
            <div className="flex max-w-[70%] items-center rounded-lg border border-black/10 bg-white/85 px-4 py-3">
              <span className="inline-flex gap-1.5">
                {[0, 1, 2].map((i) => (
                  <motion.span
                    key={i}
                    className="h-2 w-2 rounded-full bg-[#0070AD]/80"
                    animate={{ y: [0, -6, 0] }}
                    transition={{ duration: 0.5, repeat: Infinity, delay: i * 0.12, ease: 'easeInOut' }}
                  />
                ))}
              </span>
            </div>
            {jobProgress && (
              <div className="ml-2 flex items-center gap-2">
                <div className="h-1 w-24 overflow-hidden rounded-full bg-[#0070AD]/10">
                  <motion.div 
                    className="h-full bg-gradient-to-r from-[#0070AD] to-[#12ABDB]"
                    animate={{ x: ["-100%", "100%"] }}
                    transition={{ duration: 1.5, repeat: Infinity, ease: "linear" }}
                  />
                </div>
                <span className="text-[10px] font-bold uppercase tracking-widest text-[#0070AD]/60">
                  {jobProgress}
                </span>
              </div>
            )}
          </motion.div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Chat Box at Bottom */}
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3, delay: 0.2 }}
        className="shrink-0 border-t border-[#0070AD]/20 bg-gradient-to-r from-[#0070AD]/10 via-white/60 to-[#12ABDB]/10 px-6 py-4 backdrop-blur-xl"
      >
        {agentError && (
          <div className="mb-3 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
            {agentError}
          </div>
        )}
        {isLoadingAgent && (
          <div className="mb-3 flex justify-end">
            <button
              type="button"
              onClick={stopGeneration}
              className="inline-flex items-center gap-2 rounded-xl border border-black/10 bg-white/85 px-3 py-2 text-xs font-semibold text-black/70 hover:bg-white"
              title="Stop generating"
              aria-label="Stop generating"
            >
              <FaStop />
              Stop
            </button>
          </div>
        )}

        {/* Hidden file input for report upload */}
        <input
          ref={fileInputRef}
          type="file"
          accept=".md,.txt,.csv,.json,.jsonl,.xlsx,.xls,.parquet"
          className="hidden"
          onChange={async (e) => {
            const f = e.target.files?.[0];
            e.target.value = '';
            if (!f) return;
            try {
              await uploadReport(f);
            } catch (err: any) {
              setMessages((prev) => [
                ...prev,
                { id: String(Date.now()), text: `Upload failed: ${err?.message ?? String(err)}`, sender: 'bot', timestamp: new Date() },
              ]);
            }
          }}
        />

        {/* Hidden folder input for Local data (directory picker) */}
        <input
          ref={localFolderInputRef}
          type="file"
          multiple
          className="hidden"
          // @ts-ignore - webkitdirectory is supported by Chromium-based browsers
          webkitdirectory="true"
          // @ts-ignore - directory is non-standard but commonly supported
          directory="true"
          onChange={async (e) => {
            const files = Array.from(e.target.files || []);
            e.target.value = '';
            if (!files.length) return;

            setLocalFolderFiles(files);

            const rel = (files[0] as any)?.webkitRelativePath as string | undefined;
            const folder = rel ? rel.split('/')[0] : 'Selected folder';
            const names = files
              .slice(0, 12)
              .map((f) => (f as any)?.webkitRelativePath || f.name)
              .join('\n');
            const more = files.length > 12 ? `\n…(+${files.length - 12} more)` : '';

            // Persist file list metadata into session context (names only).
            await fetch('/api/session-context', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                session_id: sessionId,
                context: {
                  local_folder_name: folder,
                  local_folder_file_count: files.length,
                  local_folder_files: files.map((f) => (f as any)?.webkitRelativePath || f.name),
                  local_folder_selected_at: Date.now(),
                },
              }),
            }).catch(() => null);

            setMessages((prev) => [
              ...prev,
              {
                id: String(Date.now()),
                text:
                  `Local folder selected: ${folder}\n` +
                  `Files found: ${files.length}\n\n` +
                  `Top files:\n${names}${more}\n\n` +
                  `Next: you can ask me to assess a specific file, or use “+ Upload” to upload one report/file for analysis.`,
                sender: 'bot',
                timestamp: new Date(),
              },
            ]);
          }}
        />
        {/* Options dropdown - shown when "Choose option" is clicked */}
        <AnimatePresence>
          {showOptions && (
            <>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                onClick={() => setShowOptions(false)}
                className="fixed inset-0 z-[5] bg-black/50"
                style={{ margin: 0, top: 0, left: 0, right: 0, bottom: 0 }}
              />
              <motion.div
                initial={{ opacity: 0, height: 0, scale: 0.96, y: -8 }}
                animate={{ opacity: 1, height: 'auto', scale: 1, y: 0 }}
                exit={{ opacity: 0, height: 0, scale: 0.96, y: -8 }}
                transition={{ duration: 0.28, ease: [0.25, 0.46, 0.45, 0.94] }}
                style={{ transformOrigin: 'top left' }}
                className="mb-4 relative z-[6] rounded-xl border border-black/10 bg-white/85 backdrop-blur-xl"
              >
                {!hasSelectedDataSource ? (
                  <>
                    <div className="p-3 flex flex-wrap items-center gap-3 border-b border-black/10">
                      <motion.button
                        whileHover={{ scale: 1.05, x: -2 }}
                        whileTap={{ scale: 0.95 }}
                        onClick={() => setShowOptions(false)}
                        className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium text-black/55 transition-colors hover:text-black"
                      >
                        <FaArrowLeft className="w-3 h-3" />
                        Back
                      </motion.button>
                      <p className={`text-sm font-medium ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>Choose one:</p>
                    </div>
                    <div className="options-scroll p-3 max-h-48 scroll-smooth">
                      <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
                        {DATA_SOURCE_OPTIONS.map((option, idx) => (
                          <motion.button
                            key={option.id}
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: idx * 0.03 }}
                            whileHover={{ scale: 1.02 }}
                            whileTap={{ scale: 0.98 }}
                            onClick={() => {
                              handleOptionSelect(option);
                              setShowOptions(false);
                            }}
                            className={`flex items-center gap-2 rounded-lg border p-3 text-left text-sm font-medium transition-all ${gxEnabled ? 'border-emerald-500/30 bg-[#001D2E]/80 text-white hover:border-emerald-500/60 hover:bg-emerald-500/10' : 'border-black/10 bg-white/90 text-zinc-900 hover:border-[#0070AD]/30 hover:bg-white'}`}
                          >
                            {option.icon && <span className="flex-shrink-0">{option.icon}</span>}
                            <span>{option.text}</span>
                          </motion.button>
                        ))}
                      </div>
                    </div>
                  </>
                ) : (
                  <>
                    <div className="p-3 flex flex-wrap items-center gap-3 border-b border-black/10">
                      <motion.button
                        whileHover={{ scale: 1.05, x: -2 }}
                        whileTap={{ scale: 0.95 }}
                        onClick={handleBack}
                        className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium text-black/55 transition-colors hover:text-black"
                      >
                        <FaArrowLeft className="w-3 h-3" />
                        Back
                      </motion.button>
                      <p className={`text-sm font-medium ${gxEnabled ? 'text-white' : 'text-zinc-900'}`}>What would you like to do next?</p>
                    </div>
                    <div className="options-scroll p-3 max-h-52 scroll-smooth">
                      <div className="space-y-2">
                        {getChatOptionsForSource(selectedDataSource).map((option, idx) => (
                          <motion.button
                            key={option.id}
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: Math.min(idx * 0.02, 0.2) }}
                            whileHover={{ scale: 1.01, x: 4 }}
                            whileTap={{ scale: 0.99 }}
                            onClick={() => {
                              handleOptionSelect(option);
                              setShowOptions(false);
                            }}
                            className={`w-full flex items-center gap-3 rounded-lg border px-4 py-3 text-left text-sm font-medium transition-all ${gxEnabled ? 'border-emerald-500/30 bg-[#001D2E]/80 text-white hover:border-emerald-500/60 hover:bg-emerald-500/10' : 'border-black/10 bg-white/90 text-zinc-900 hover:border-[#0070AD]/30 hover:bg-white'}`}
                          >
                            {option.icon && <span className="flex-shrink-0">{option.icon}</span>}
                            <span>{option.text}</span>
                          </motion.button>
                        ))}
                      </div>
                    </div>
                  </>
                )}
              </motion.div>
            </>
          )}
        </AnimatePresence>

        {/* Chat input with options trigger */}
        <div className="flex gap-2 items-center">
          {/* Upload only */}
          <motion.button
            whileHover={{ scale: 1.02, y: -1 }}
            whileTap={{ scale: 0.98 }}
            onClick={() => fileInputRef.current?.click()}
            className={`flex shrink-0 items-center justify-center gap-2 rounded-lg border px-3 py-2.5 text-sm font-medium transition-all ${gxEnabled ? 'border-emerald-500/30 bg-[#001D2E]/80 text-white hover:border-emerald-500/60 hover:bg-emerald-500/10' : 'border-black/10 bg-white/90 text-zinc-900 hover:border-[#0070AD]/30 hover:bg-white'}`}
            title="Upload report"
            aria-label="Upload report"
          >
            <FaPlus className="w-4 h-4" />
            <span className="hidden sm:inline">Upload</span>
          </motion.button>

          <motion.button
            whileHover={{ scale: 1.02, y: -1 }}
            whileTap={{ scale: 0.98 }}
            onClick={() => setShowOptions(!showOptions)}
            className={`flex shrink-0 items-center gap-2 rounded-lg border px-4 py-2.5 text-sm font-medium transition-all ${gxEnabled ? 'border-emerald-500/30 bg-[#001D2E]/80 text-white hover:border-emerald-500/60 hover:bg-emerald-500/10' : 'border-black/10 bg-white/90 text-zinc-900 hover:border-[#0070AD]/30 hover:bg-white'}`}
          >
            <motion.div
              animate={{ rotate: showOptions ? 180 : 0 }}
              transition={{ duration: 0.25 }}
            >
              <FaChevronDown className="w-4 h-4" />
            </motion.div>
            Choose option
          </motion.button>
          <div className="flex-1 flex gap-2">
            <input
              type="text"
              value={chatInput}
              onChange={(e) => setChatInput(e.target.value)}
              onKeyDown={async (e) => {
                if (e.key === 'Enter' && chatInput.trim()) {
                  const text = chatInput.trim();
                  setChatInput('');
                  await sendUserText(text);
                }
              }}
              placeholder="Type a message or choose an option..."
              className={`flex-1 rounded-lg border px-4 py-2.5 text-sm outline-none transition-all ${gxEnabled ? 'border-emerald-500/20 bg-[#001D2E]/80 text-white placeholder-white/30 focus:border-emerald-500/50 focus:ring-emerald-500/10' : 'border-black/10 bg-white/90 text-zinc-900 placeholder-black/40 focus:border-[#0070AD]/40 focus:ring-[#0070AD]/20'}`}
            />
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: !chatInput.trim() || isLoadingAgent ? 1 : 0.88 }}
              transition={{ type: 'spring', stiffness: 400, damping: 17 }}
              disabled={!chatInput.trim() || isLoadingAgent}
              onClick={async () => {
                if (!chatInput.trim() || isLoadingAgent) return;
                const text = chatInput.trim();
                setChatInput('');
                await sendUserText(text);
              }}
              className="flex shrink-0 items-center justify-center rounded-lg border border-[#0070AD]/50 bg-[#0070AD] px-4 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-[#12ABDB] disabled:cursor-not-allowed disabled:opacity-40"
            >
              <motion.span
                className="relative z-10"
                animate={{ x: 0, y: 0 }}
                whileHover={{ x: 2, y: -2 }}
                transition={{ type: 'spring', stiffness: 500 }}
              >
                <FaPaperPlane />
              </motion.span>
            </motion.button>
          </div>
        </div>
      </motion.div>
    </div>
  );
}
