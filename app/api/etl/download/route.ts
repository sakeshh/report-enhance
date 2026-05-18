import { NextRequest, NextResponse } from 'next/server';
import { getBackendBaseUrl, proxyToBackend } from '@/lib/backend-bridge';

export async function GET(req: NextRequest) {
  if (!getBackendBaseUrl()) {
    return NextResponse.json(
      { ok: false, error: 'BACKEND_NOT_CONFIGURED', message: 'Set BACKEND_BASE_URL' },
      { status: 503 }
    );
  }
  const sessionId = req.nextUrl.searchParams.get('session_id') || 'default';
  try {
    const res = await proxyToBackend(
      `/etl/download?session_id=${encodeURIComponent(sessionId)}`,
      { method: 'GET', timeoutMs: 60_000 }
    );
    if (!res.ok) {
      const text = await res.text();
      return new NextResponse(text, {
        status: res.status,
        headers: { 'Content-Type': res.headers.get('content-type') ?? 'application/json' },
      });
    }
    const blob = await res.blob();
    const disposition = res.headers.get('content-disposition') ?? 'attachment';
    return new NextResponse(blob, {
      status: 200,
      headers: {
        'Content-Type': res.headers.get('content-type') ?? 'application/octet-stream',
        'Content-Disposition': disposition,
      },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: 'PROXY_FAILED', message }, { status: 500 });
  }
}
