import { useEffect, useState, type FormEvent } from 'react';

interface Hop {
  service: string;
  hostname: string;
  at: string;
}

interface HelloView {
  id: string;
  name?: string;
  status: 'QUEUED' | 'PAID' | 'DONE';
  hops: Hop[];
}

const serviceColors: Record<string, string> = {
  api: '#2563eb',
  orchestrator: '#7c3aed',
  payment: '#059669',
  shipping: '#d97706',
};

export function App() {
  const [name, setName] = useState('');
  const [id, setId] = useState<string | null>(null);
  const [view, setView] = useState<HelloView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function sayHello(event: FormEvent) {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || submitting) return;

    setSubmitting(true);
    setError(null);
    setView(null);
    setId(null);

    try {
      const response = await fetch('/api/hello', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: trimmed }),
      });
      if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
      }
      const data = (await response.json()) as { id: string };
      setId(data.id);
      setView({ id: data.id, status: 'QUEUED', hops: [] });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Request failed');
    } finally {
      setSubmitting(false);
    }
  }

  useEffect(() => {
    if (!id) return;

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const poll = async () => {
      try {
        const response = await fetch(`/api/hello/${id}`);
        if (response.status === 404) {
          // Not persisted by the orchestrator yet; keep the QUEUED placeholder.
          if (cancelled) return;
          setError(null);
          timer = setTimeout(poll, 500);
          return;
        }
        if (!response.ok) {
          throw new Error(`API returned ${response.status}`);
        }
        const data = (await response.json()) as HelloView;
        if (cancelled) return;
        setError(null);
        setView(data);
        if (data.status !== 'DONE') {
          timer = setTimeout(poll, 500);
        }
      } catch (caught) {
        if (cancelled) return;
        setError(caught instanceof Error ? caught.message : 'Polling failed');
        timer = setTimeout(poll, 1_000);
      }
    };

    void poll();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [id]);

  return (
    <main className="page">
      <header>
        <p className="eyebrow">Phase 0 · walking skeleton</p>
        <h1>Say hello through the whole stack</h1>
        <p className="lede">
          The message travels API → Redis stream → orchestrator → Postgres → SQS → payment → SQS →
          shipping → back again.
        </p>
      </header>

      <form className="hello-form" onSubmit={sayHello}>
        <label htmlFor="name">Your name</label>
        <div className="row">
          <input
            id="name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Ada Lovelace"
            maxLength={100}
            autoComplete="off"
          />
          <button type="submit" disabled={!name.trim() || submitting}>
            {submitting ? 'Sending…' : 'Say hello'}
          </button>
        </div>
      </form>

      {error && <p className="error">{error}</p>}

      {view && (
        <section className="timeline-card">
          <div className="timeline-header">
            <div>
              <p className="eyebrow">Message</p>
              <code>{view.id}</code>
            </div>
            <span className={`badge ${view.status.toLowerCase()}`}>{view.status}</span>
          </div>

          {view.hops.length === 0 && <p className="muted">Waiting for the orchestrator…</p>}

          <ol className="timeline">
            {view.hops.map((hop, index) => (
              <li key={`${hop.service}-${hop.at}-${index}`}>
                <span
                  className="dot"
                  style={{ background: serviceColors[hop.service] ?? '#475569' }}
                />
                <div>
                  <p className="hop-service">{hop.service}</p>
                  <p className="hop-meta">
                    {hop.hostname} · {new Date(hop.at).toLocaleTimeString()}
                  </p>
                </div>
              </li>
            ))}
          </ol>

          {view.status === 'DONE' && (
            <p className="done">
              Hello, {view.name ?? name}! The full round trip took {view.hops.length} hops.
            </p>
          )}
        </section>
      )}
    </main>
  );
}
