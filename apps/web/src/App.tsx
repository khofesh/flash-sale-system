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

type SaleStatus = 'UPCOMING' | 'ACTIVE' | 'SOLD_OUT' | 'ENDED';

interface SaleView {
  saleId: string;
  status: SaleStatus;
  startsAt: string;
  endsAt: string;
  remainingStock: number;
  totalStock: number;
  serverTime: string;
}

const serviceColors: Record<string, string> = {
  api: '#2563eb',
  orchestrator: '#7c3aed',
  payment: '#059669',
  shipping: '#d97706',
};

const saleLabels: Record<SaleStatus, string> = {
  UPCOMING: 'Upcoming',
  ACTIVE: 'Live now',
  SOLD_OUT: 'Sold out',
  ENDED: 'Ended',
};

function formatCountdown(milliseconds: number): string {
  const total = Math.max(0, Math.ceil(milliseconds / 1000));
  const d = Math.floor(total / 86_400);
  const h = Math.floor((total % 86_400) / 3_600);
  const m = Math.floor((total % 3_600) / 60);
  const s = total % 60;
  const pad = (n: number) => String(n).padStart(2, '0');
  if (d > 0) return `${d}d ${h}h ${pad(m)}m`;
  if (h > 0) return `${h}h ${pad(m)}m ${pad(s)}s`;
  if (m > 0) return `${m}m ${pad(s)}s`;
  return `${s}s`;
}

export function App() {
  const [name, setName] = useState('');
  const [id, setId] = useState<string | null>(null);
  const [view, setView] = useState<HelloView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const [sale, setSale] = useState<SaleView | null>(null);
  const [saleError, setSaleError] = useState<string | null>(null);
  const [clockOffset, setClockOffset] = useState(0);
  const [now, setNow] = useState(() => Date.now());

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
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // Lowest round trip seen so far; its offset sample is the most accurate.
    let bestRtt = Infinity;

    const pollSale = async () => {
      try {
        const sentAt = Date.now();
        const response = await fetch('/api/sale');
        const receivedAt = Date.now();
        if (cancelled) return;
        if (response.status === 404) {
          setSale(null);
          setSaleError('No sale configured yet');
        } else {
          if (!response.ok) {
            throw new Error(`API returned ${response.status}`);
          }
          const data = (await response.json()) as SaleView;
          if (cancelled) return;
          setSale(data);
          // Correct the local clock against the server's, so the countdown does
          // not depend on the browser clock (SPEC §8). Assume serverTime was
          // stamped mid-flight, and keep the sample with the smallest round trip
          // so latency jitter does not make the countdown jump.
          const rtt = receivedAt - sentAt;
          if (rtt <= bestRtt) {
            bestRtt = rtt;
            setClockOffset(Date.parse(data.serverTime) - (sentAt + rtt / 2));
          }
          setSaleError(null);
        }
      } catch (caught) {
        if (!cancelled) {
          setSaleError(caught instanceof Error ? caught.message : 'Could not load sale');
        }
      }
      if (!cancelled) {
        timer = setTimeout(pollSale, 1_000);
      }
    };

    void pollSale();
    const ticker = setInterval(() => setNow(Date.now()), 250);

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      clearInterval(ticker);
    };
  }, []);

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

  const serverNow = now + clockOffset;
  const countdown = sale
    ? sale.status === 'UPCOMING'
      ? formatCountdown(Date.parse(sale.startsAt) - serverNow)
      : sale.status === 'ACTIVE'
        ? formatCountdown(Date.parse(sale.endsAt) - serverNow)
        : null
    : null;

  return (
    <main className="page">
      <header>
        <p className="eyebrow">Phase 1 · sale configuration</p>
        <h1>Flash sale status</h1>
        <p className="lede">
          The banner follows the server clock and the stock kept in Redis. The hello walkthrough
          underneath still exercises the whole stack.
        </p>
      </header>

      {sale && (
        <section className={`sale-banner ${sale.status.toLowerCase()}`}>
          <div className="sale-heading">
            <div>
              <p className="eyebrow">Sale · {sale.saleId}</p>
              <h2>{saleLabels[sale.status]}</h2>
            </div>
            <span className="sale-status">{sale.status.replace('_', ' ')}</span>
          </div>

          <div className="sale-metrics">
            <div>
              <span>Remaining stock</span>
              <strong>
                {sale.remainingStock}
                <small> / {sale.totalStock}</small>
              </strong>
            </div>
            <div>
              <span>
                {sale.status === 'UPCOMING'
                  ? 'Starts in'
                  : sale.status === 'ACTIVE'
                    ? 'Ends in'
                    : 'Window'}
              </span>
              <strong>
                {countdown ??
                  `${new Date(sale.startsAt).toLocaleTimeString()} – ${new Date(
                    sale.endsAt,
                  ).toLocaleTimeString()}`}
              </strong>
            </div>
          </div>
        </section>
      )}

      {saleError && <p className="error">{saleError}</p>}

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
