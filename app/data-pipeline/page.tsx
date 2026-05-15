'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { FaDatabase, FaFileAlt, FaChartBar, FaCode, FaCheck, FaArrowLeft, FaDownload, FaEye, FaArrowRight, FaClipboardList } from 'react-icons/fa';
import { useRouter } from 'next/navigation';
import AnimatedBackground from '@/components/AnimatedBackground';
import DatabaseSelector from '@/components/DatabaseSelector';
import FileSelector from '@/components/FileSelector';
import DataAssessmentReport from '@/components/DataAssessmentReport';
import EtlGenerationPanel from '@/components/EtlGenerationPanel';
import DataCleaner from '@/components/DataCleaner';
import Confetti from '@/components/Confetti';
type Step = 'database' | 'files' | 'requirements' | 'assessment' | 'report' | 'etl' | 'cleaning' | 'complete';

function generateHtmlReportFromBackend(html: string): string {
  return html || '<!doctype html><html><head><meta charset="utf-8" /></head><body>No HTML report available.</body></html>';
}

function openHtmlReportInNewTab(html: string) {
  if (typeof window === 'undefined') return;
  const safeHtml = generateHtmlReportFromBackend(html);
  // Use a blob URL so large reports don't hit URL length limits.
  const blob = new Blob([safeHtml], { type: 'text/html' });
  const url = URL.createObjectURL(blob);
  window.open(url, '_blank', 'noopener,noreferrer');
  // Best-effort cleanup after the new tab has had time to load.
  window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

export default function DataPipelinePage() {
  const router = useRouter();
  const [currentStep, setCurrentStep] = useState<Step>('database');
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);
  const [selectedFiles, setSelectedFiles] = useState<string[]>([]);
  const [availableFiles, setAvailableFiles] = useState<string[]>([]);
  const [assessmentData, setAssessmentData] = useState<any>(null);
  const [reportFormat, setReportFormat] = useState<string | null>(null);
  const [showReportView, setShowReportView] = useState(false);
  const [etlCode, setEtlCode] = useState<string | null>(null);
  const [includeTransformSuggestions, setIncludeTransformSuggestions] = useState<boolean>(true);
  const [includeDqRecommendations, setIncludeDqRecommendations] = useState<boolean>(true);
  const [etlSessionId, setEtlSessionId] = useState('default');

  useEffect(() => {
    if (typeof window === 'undefined') return;
    setEtlSessionId(window.localStorage.getItem('dharaSessionId') || 'default');
  }, [assessmentData]);
  const [userFeedback, setUserFeedback] = useState<Array<{
    step: string;
    liked: boolean;
    comment?: string;
  }>>([]);

  const steps = [
    { id: 'database', label: 'Database', icon: FaDatabase },
    { id: 'files', label: 'Files', icon: FaFileAlt },
    { id: 'assessment', label: 'Assessment', icon: FaChartBar },
    { id: 'report', label: 'Report', icon: FaChartBar },
    { id: 'requirements', label: 'Requirements', icon: FaClipboardList },
    { id: 'etl', label: 'ETL Code', icon: FaCode },
    { id: 'cleaning', label: 'Data Cleaning', icon: FaCheck },
  ];

  const handleDatabaseSelect = (database: string) => {
    setSelectedDatabase(database);
    setDirection('forward');
    setCurrentStep('files');
  };

  const handleFilesSelect = (files: string[], available: string[]) => {
    setSelectedFiles(files);
    setAvailableFiles(available);
  };

  const handleStartAssessment = () => {
    setDirection('forward');
    setCurrentStep('assessment');
  };

  const handleAssessmentComplete = (data: any) => {
    setAssessmentData(data);
    // Stay on assessment step so user can review/toggle options,
    // then explicitly continue to the report step.
  };

  const handleReportFormatSelect = (format: string) => {
    setReportFormat(format);
  };

  const handleDownloadReport = () => {
    if (!assessmentData || !reportFormat) return;
    const backendResult = assessmentData?.result ?? assessmentData;
    const md = typeof assessmentData?.report_markdown === 'string' ? assessmentData.report_markdown : null;
    const html = typeof assessmentData?.report_html === 'string' ? assessmentData.report_html : null;

    const blob =
      reportFormat === 'JSON'
        ? new Blob([JSON.stringify(backendResult, null, 2)], { type: 'application/json' })
        : reportFormat === 'MD'
          ? new Blob([md ?? 'No markdown report available.'], { type: 'text/markdown' })
          : new Blob([generateHtmlReportFromBackend(html ?? '')], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `assessment-report.${reportFormat === 'JSON' ? 'json' : reportFormat === 'MD' ? 'md' : 'html'}`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleProceedFromReport = () => {
    setShowReportView(false);
    setDirection('forward');
    setCurrentStep('requirements');
  };

  const handleETLGenerated = (code: string) => {
    setEtlCode(code);
  };

  const handleStartCleaning = () => {
    setDirection('forward');
    setCurrentStep('cleaning');
  };

  const handleFeedback = (step: string, liked: boolean, comment?: string) => {
    setUserFeedback([...userFeedback, { step, liked, comment }]);
    
    if (!liked) {
      console.log(`User disliked ${step}. Feedback:`, comment);
    }
  };

  const [direction, setDirection] = useState<'forward' | 'back'>('forward');
  const getCurrentStepIndex = () => steps.findIndex(s => s.id === currentStep);

  const goToStep = (stepId: Step) => {
    const currIdx = getCurrentStepIndex();
    const targetIdx = steps.findIndex(s => s.id === stepId);
    setDirection(targetIdx >= currIdx ? 'forward' : 'back');
    setCurrentStep(stepId);
  };

  return (
    <div className="relative min-h-screen overflow-hidden bg-transparent">
      <AnimatedBackground />

      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        className="relative z-10 border-b border-black/10 bg-white/75 backdrop-blur-sm"
      >
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <motion.button
            whileHover={{ x: -4 }}
            onClick={() => router.push('/chat')}
            className="flex items-center gap-2 text-black/65 hover:text-black transition-colors"
          >
            <FaArrowLeft />
            <span>Back to Chat</span>
          </motion.button>
          <h1 className="text-2xl font-bold text-zinc-900">
            Data Pipeline Workflow
          </h1>
          <div className="w-20" />
        </div>
      </motion.div>

      {/* Progress Steps */}
      <div className="relative z-10 max-w-7xl mx-auto px-6 py-8">
        <div className="flex items-center justify-between mb-8">
          {steps.map((step, index) => {
            const Icon = step.icon;
            const isActive = currentStep === step.id;
            const isCompleted = index < getCurrentStepIndex();
            const isClickable = isActive || isCompleted;
            
            return (
              <div key={step.id} className="flex items-center flex-1">
                <div className="flex flex-col items-center">
                  <motion.button
                    type="button"
                    onClick={() => isClickable && goToStep(step.id as Step)}
                    disabled={!isClickable}
                    className={`flex flex-col items-center focus:outline-none focus:ring-2 focus:ring-[#0070AD]/25 focus:ring-offset-2 focus:ring-offset-white rounded-full ${
                      !isClickable ? 'cursor-default' : 'cursor-pointer'
                    }`}
                  >
                    <motion.div
                      className={`w-12 h-12 rounded-full flex items-center justify-center ${
                        isActive
                          ? 'bg-[#0070AD] text-white'
                          : isCompleted
                          ? 'bg-[#0070AD]/80 text-white hover:bg-[#0070AD]'
                          : 'bg-black/5 text-black/45'
                      }`}
                      animate={isActive ? { scale: [1, 1.1, 1] } : {}}
                      transition={{ duration: 0.5, repeat: isActive ? Infinity : 0, repeatDelay: 2 }}
                      whileHover={isClickable ? { scale: 1.1 } : {}}
                      whileTap={isClickable ? { scale: 0.95 } : {}}
                    >
                      <Icon className="text-xl" />
                    </motion.div>
                    <span className={`text-xs mt-2 font-medium ${
                      isActive ? 'text-zinc-900' : isCompleted ? 'text-[#0070AD]' : 'text-black/45'
                    } ${isClickable ? 'cursor-pointer' : ''}`}>
                      {step.label}
                    </span>
                  </motion.button>
                </div>
                {index < steps.length - 1 && (
                  <div className="flex-1 h-1 mx-4 bg-black/10 rounded-full overflow-hidden">
                    <motion.div
                      className="h-full bg-[#0070AD] rounded-full"
                      initial={{ width: 0 }}
                      animate={{ width: isCompleted ? '100%' : 0 }}
                      transition={{ duration: 0.5, ease: [0.25, 0.46, 0.45, 0.94] }}
                    />
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Content Area */}
        <AnimatePresence mode="wait">
          <motion.div
            key={currentStep}
            initial={{ opacity: 0, x: direction === 'forward' ? 60 : -60 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: direction === 'forward' ? -60 : 60 }}
            transition={{ duration: 0.35, ease: [0.25, 0.46, 0.45, 0.94] }}
            className="options-scroll rounded-2xl border border-black/10 bg-white/75 p-8 shadow-[0_30px_120px_rgba(0,0,0,0.12)] backdrop-blur-xl max-h-[calc(100vh-280px)]"
          >
            {currentStep === 'database' && (
              <DatabaseSelector onSelect={handleDatabaseSelect} onBack={() => router.push('/chat')} />
            )}

            {currentStep === 'files' && selectedDatabase && (
              <FileSelector
                database={selectedDatabase}
                onSelect={handleFilesSelect}
                onNext={handleStartAssessment}
                selectedFiles={selectedFiles}
              />
            )}

            {currentStep === 'assessment' && (
              <div className="space-y-4">
                <DataAssessmentReport
                  files={selectedFiles}
                  database={selectedDatabase!}
                  includeTransformSuggestions={includeTransformSuggestions}
                  onIncludeTransformSuggestionsChange={setIncludeTransformSuggestions}
                  includeDqRecommendations={includeDqRecommendations}
                  onIncludeDqRecommendationsChange={setIncludeDqRecommendations}
                  onComplete={handleAssessmentComplete}
                  onFeedback={(liked, comment) => handleFeedback('assessment', liked, comment)}
                />
                {assessmentData && (
                  <motion.button
                    type="button"
                    onClick={() => {
                      setDirection('forward');
                      setCurrentStep('report');
                    }}
                    className="w-full flex items-center justify-center gap-2 px-6 py-3 rounded-xl border border-[#0070AD]/40 bg-[#0070AD]/10 text-[#0070AD] font-semibold hover:bg-[#0070AD]/15 hover:border-[#0070AD]/60 transition-colors"
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                  >
                    Continue to Report
                  </motion.button>
                )}
              </div>
            )}

            {currentStep === 'report' && assessmentData && (
              <div className="space-y-6">
                {!showReportView ? (
                  <>
                    <h2 className="text-2xl font-bold text-zinc-900">Select Report Format</h2>
                    <p className="text-black/60">Choose a format for your data assessment report</p>
                    <div className="grid grid-cols-2 gap-4">
                      {['JSON', 'HTML', 'MD'].map((format) => (
                        <motion.button
                          key={format}
                          onClick={() => handleReportFormatSelect(format)}
                          className={`p-6 rounded-xl border transition-all duration-300 ${
                            reportFormat === format
                              ? 'border-[#0070AD]/60 bg-[#0070AD]/10 shadow-[0_0_30px_rgba(0,112,173,0.12)]'
                              : 'border-black/10 bg-white/85 hover:border-[#0070AD]/30 hover:bg-white'
                          }`}
                          whileHover={{ y: -2 }}
                          whileTap={{ scale: 0.98 }}
                        >
                          <div className="text-lg font-semibold text-zinc-900">{format}</div>
                        </motion.button>
                      ))}
                    </div>
                    {reportFormat && (
                      <div className="flex flex-wrap items-center gap-3">
                        <motion.button
                          initial={{ opacity: 0, y: 10 }}
                          animate={{ opacity: 1, y: 0 }}
                          onClick={() => {
                            if (reportFormat === 'HTML') {
                              const html = typeof assessmentData?.report_html === 'string' ? assessmentData.report_html : '';
                              openHtmlReportInNewTab(html);
                              return;
                            }
                            setShowReportView(true);
                          }}
                          className="flex items-center gap-3 px-6 py-3 rounded-xl border border-[#0070AD]/40 bg-[#0070AD]/10 text-[#0070AD] font-semibold hover:bg-[#0070AD]/15 hover:border-[#0070AD]/60 transition-colors"
                        >
                          <FaEye className="w-5 h-5" />
                          {reportFormat === 'HTML' ? 'Open HTML in new tab' : 'View Report'}
                        </motion.button>

                        {reportFormat === 'HTML' && (
                          <motion.button
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            onClick={handleDownloadReport}
                            className="flex items-center gap-3 px-6 py-3 rounded-xl border border-black/10 bg-white/85 text-zinc-900 font-semibold hover:bg-white hover:border-[#0070AD]/30 transition-colors"
                            whileHover={{ scale: 1.02 }}
                            whileTap={{ scale: 0.98 }}
                          >
                            <FaDownload className="w-5 h-5 text-[#0070AD]" />
                            Download HTML
                          </motion.button>
                        )}
                      </div>
                    )}

                    {reportFormat && (
                      <motion.button
                        initial={{ opacity: 0, y: 10 }}
                        animate={{ opacity: 1, y: 0 }}
                        onClick={handleProceedFromReport}
                        className="mt-2 flex w-full items-center justify-center gap-2 px-6 py-3 rounded-xl border border-[#0070AD]/40 bg-[#0070AD]/10 text-[#0070AD] font-semibold hover:bg-[#0070AD]/15 hover:border-[#0070AD]/60 transition-colors"
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                      >
                        Continue to ETL rules & plan
                        <FaArrowRight className="w-4 h-4" />
                      </motion.button>
                    )}
                  </>
                ) : (
                  <>
                    <h2 className="text-2xl font-bold text-zinc-900">Data Assessment Report</h2>
                    <div className="options-scroll max-h-[70vh] rounded-xl border border-black/10 bg-white/90 p-4 overflow-y-auto">
                      {reportFormat === 'JSON' ? (
                        <pre className="text-sm text-zinc-900 whitespace-pre-wrap font-mono">
                          {JSON.stringify(assessmentData?.result ?? assessmentData, null, 2)}
                        </pre>
                      ) : reportFormat === 'MD' ? (
                        <pre className="text-sm text-zinc-900 whitespace-pre-wrap font-mono">
                          {typeof assessmentData?.report_markdown === 'string' ? assessmentData.report_markdown : 'No markdown report available.'}
                        </pre>
                      ) : (
                        <div className="text-sm text-black/70">
                          HTML report opens in a new tab. Use Download to save as HTML.
                        </div>
                      )}
                    </div>
                    <div className="flex gap-4">
                      <motion.button
                        onClick={handleDownloadReport}
                        className="flex items-center gap-2 px-6 py-3 rounded-xl border border-black/10 bg-white/85 text-zinc-900 font-medium hover:bg-white hover:border-[#0070AD]/30 transition-colors"
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                      >
                        <FaDownload className="w-4 h-4" />
                        Download
                      </motion.button>
                      <motion.button
                        onClick={handleProceedFromReport}
                        className="flex items-center gap-2 px-6 py-3 rounded-xl border border-[#0070AD]/40 bg-[#0070AD]/10 text-[#0070AD] font-semibold hover:bg-[#0070AD]/15 hover:border-[#0070AD]/60 transition-colors"
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                      >
                        Continue to ETL rules & plan
                        <FaArrowRight className="w-4 h-4" />
                      </motion.button>
                    </div>
                  </>
                )}
              </div>
            )}

            {currentStep === 'requirements' && (
              <div className="space-y-6">
                {!assessmentData ? (
                  <p className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950">
                    Complete assessment and report before defining ETL rules and plan.
                  </p>
                ) : (
                  <EtlGenerationPanel
                    sessionId={etlSessionId}
                    assessment={(assessmentData?.result ?? assessmentData) as Record<string, unknown>}
                    variant="pipeline"
                    pipelineMode="requirements"
                    onContinueToEtlStep={() => {
                      setDirection('forward');
                      setCurrentStep('etl');
                    }}
                  />
                )}
              </div>
            )}

            {currentStep === 'etl' && (
              <div className="space-y-4">
                {!assessmentData ? (
                  <p className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950">
                    Complete earlier steps before generating ETL code.
                  </p>
                ) : (
                  <EtlGenerationPanel
                    sessionId={etlSessionId}
                    assessment={(assessmentData?.result ?? assessmentData) as Record<string, unknown>}
                    variant="pipeline"
                    pipelineMode="etl"
                    onEditPlanInRequirements={() => {
                      setDirection('back');
                      setCurrentStep('requirements');
                    }}
                    onCodeGenerated={handleETLGenerated}
                    onContinueAfterCode={handleStartCleaning}
                  />
                )}
              </div>
            )}

            {currentStep === 'cleaning' && (
              <DataCleaner
                files={selectedFiles}
                etlCode={etlCode}
                assessmentData={assessmentData}
                userFeedback={userFeedback}
                onComplete={() => { setDirection('forward'); setCurrentStep('complete'); }}
                onFeedback={(liked, comment) => handleFeedback('cleaning', liked, comment)}
              />
            )}

            {currentStep === 'complete' && (
              <div className="relative text-center py-12 overflow-visible">
                <Confetti trigger />
                <motion.div
                  initial={{ scale: 0 }}
                  animate={{ scale: 1 }}
                  transition={{ type: 'spring', stiffness: 200 }}
                  className="w-24 h-24 bg-[#0070AD] rounded-full flex items-center justify-center mx-auto mb-6 shadow-[0_0_40px_rgba(0,112,173,0.25)]"
                >
                  <FaCheck className="text-4xl text-white" />
                </motion.div>
                <h2 className="text-3xl font-bold text-zinc-900 mb-4">Pipeline Complete!</h2>
                <p className="text-black/60 mb-8">
                  Your data has been processed successfully.
                </p>
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={() => {
                    setDirection('forward');
                    setCurrentStep('database');
                    setSelectedDatabase(null);
                    setSelectedFiles([]);
                    setAssessmentData(null);
                    setReportFormat(null);
                    setShowReportView(false);
                    setEtlCode(null);
                  }}
                  className="px-6 py-3 rounded-xl border border-[#0070AD]/40 bg-[#0070AD]/10 text-[#0070AD] font-semibold hover:bg-[#0070AD]/15 hover:border-[#0070AD]/60 transition-colors"
                >
                  Start New Pipeline
                </motion.button>
              </div>
            )}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
