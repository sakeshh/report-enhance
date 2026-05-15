import { NextRequest, NextResponse } from 'next/server';
import { getBackendBaseUrl, proxyToBackend } from '@/lib/backend-bridge';

export async function POST(req: NextRequest) {
  if (!getBackendBaseUrl()) {
    return NextResponse.json(
      { ok: false, error: 'BACKEND_NOT_CONFIGURED', message: 'Set BACKEND_BASE_URL' },
      { status: 503 }
    );
  }
  try {
    const body = await req.json();
    const res = await proxyToBackend('/etl/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      timeoutMs: 120_000,
    });
    const text = await res.text();
    return new NextResponse(text, {
      status: res.status,
      headers: { 'Content-Type': res.headers.get('content-type') ?? 'application/json' },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: 'PROXY_FAILED', message }, { status: 500 });
  }
}
