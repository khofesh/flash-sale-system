import { useEffect, useState, type FormEvent } from 'react';

type SaleStatus = 'UPCOMING' | 'ACTIVE' | 'SOLD_OUT' | 'ENDED';

type OrderStatus =
  | 'RESERVED'
  | 'PAYMENT_PENDING'
  | 'PAID'
  | 'SHIPPING_PENDING'
  | 'COMPLETED'
  | 'SHIPPING_FAILED'
  | 'CANCELLED'
  | 'REFUNDED';

type PurchaseCode =
  | 'RESERVED'
  | 'ALREADY_PURCHASED'
  | 'SOLD_OUT'
  | 'NOT_STARTED'
  | 'ENDED'
  | 'VALIDATION_ERROR'
  | 'RATE_LIMITED'
  | 'UNAVAILABLE'
  | 'SALE_NOT_FOUND';

interface SaleView {
  saleId: string;
  status: SaleStatus;
  startsAt: string;
  endsAt: string;
  remainingStock: number;
  totalStock: number;
  serverTime: string;
}

interface PurchaseView {
  userId: string;
  secured: boolean;
  orderId?: string;
  status?: OrderStatus;
  updatedAt?: string;
  failureReason?: string;
  final?: boolean;
}

interface Feedback {
  tone: 'success' | 'info' | 'error' | 'warning';
  text: string;
}

const saleLabels: Record<SaleStatus, string> = {
  UPCOMING: 'Upcoming',
  ACTIVE: 'Live now',
  SOLD_OUT: 'Sold out',
  ENDED: 'Ended',
};

const orderLabels: Record<OrderStatus, string> = {
  RESERVED: 'Reserved',
  PAYMENT_PENDING: 'Processing payment',
  PAID: 'Paid',
  SHIPPING_PENDING: 'Preparing shipment',
  COMPLETED: 'Shipped',
  SHIPPING_FAILED: 'Shipping failed',
  CANCELLED: 'Cancelled',
  REFUNDED: 'Refunded',
};

const finalStatuses = new Set<OrderStatus>([
  'COMPLETED',
  'SHIPPING_FAILED',
  'CANCELLED',
  'REFUNDED',
]);

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

function feedbackFor(code: PurchaseCode): Feedback {
  switch (code) {
    case 'RESERVED':
      return { tone: 'success', text: '🎉 Reserved! You secured a unit.' };
    case 'ALREADY_PURCHASED':
      return { tone: 'info', text: '✅ You already purchased this item.' };
    case 'SOLD_OUT':
      return { tone: 'error', text: '❌ Sold out.' };
    case 'NOT_STARTED':
      return { tone: 'warning', text: '⏰ The sale has not started yet.' };
    case 'ENDED':
      return { tone: 'error', text: '🔒 The sale has ended.' };
    case 'VALIDATION_ERROR':
      return { tone: 'warning', text: '⚠️ Enter a valid user id.' };
    case 'RATE_LIMITED':
      return { tone: 'warning', text: '⚠️ Too many requests, please try again.' };
    case 'SALE_NOT_FOUND':
      return { tone: 'error', text: '⚠️ No sale is configured.' };
    case 'UNAVAILABLE':
      return { tone: 'warning', text: '⚠️ Service unavailable. Try again.' };
  }
}

const purchaseCodes = new Set<string>([
  'RESERVED',
  'ALREADY_PURCHASED',
  'SOLD_OUT',
  'NOT_STARTED',
  'ENDED',
  'VALIDATION_ERROR',
  'RATE_LIMITED',
  'UNAVAILABLE',
  'SALE_NOT_FOUND',
] satisfies PurchaseCode[]);

/**
 * Parses a purchase response. Errors produced in front of the API (nginx 429 or
 * 502 pages) are not JSON, so they are mapped from the status code instead.
 */
async function readPurchaseBody(
  response: Response,
): Promise<{ code: PurchaseCode; orderId?: string }> {
  try {
    const data = (await response.json()) as { code?: PurchaseCode; orderId?: string };
    if (data.code && purchaseCodes.has(data.code)) {
      return data as { code: PurchaseCode; orderId?: string };
    }
  } catch {
    // Fall through to the status code.
  }
  if (response.status === 429) return { code: 'RATE_LIMITED' };
  if (response.status >= 400 && response.status < 500) return { code: 'VALIDATION_ERROR' };
  return { code: 'UNAVAILABLE' };
}

function stepIndex(status: OrderStatus | undefined): number {
  switch (status) {
    case 'RESERVED':
    case 'PAYMENT_PENDING':
      return 0;
    case 'PAID':
    case 'SHIPPING_PENDING':
      return 1;
    case 'COMPLETED':
      // Past the last step, so every step renders as done.
      return 3;
    default:
      return -1;
  }
}

export function App() {
  const [userId, setUserId] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState<Feedback | null>(null);
  const [purchase, setPurchase] = useState<PurchaseView | null>(null);
  const [pollUserId, setPollUserId] = useState<string | null>(null);

  const [sale, setSale] = useState<SaleView | null>(null);
  const [saleError, setSaleError] = useState<string | null>(null);
  const [clockOffset, setClockOffset] = useState(0);
  const [now, setNow] = useState(() => Date.now());

  async function buy(event: FormEvent) {
    event.preventDefault();
    const id = userId.trim();
    if (!id || submitting || sale?.status !== 'ACTIVE') return;

    setSubmitting(true);
    setFeedback(null);
    setPurchase(null);
    setPollUserId(null);

    try {
      const response = await fetch('/api/purchases', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          // A per-click key makes a network retry return the original response
          // instead of reserving a second unit (SPEC §8).
          'idempotency-key': crypto.randomUUID(),
        },
        body: JSON.stringify({ userId: id }),
      });
      const data = await readPurchaseBody(response);

      setFeedback(feedbackFor(data.code));

      if (data.code === 'RESERVED' || data.code === 'ALREADY_PURCHASED') {
        const normalized = id.toLowerCase();
        setPurchase({
          userId: normalized,
          secured: true,
          orderId: data.orderId,
          status: 'RESERVED',
        });
        setPollUserId(normalized);
      }
    } catch (caught) {
      setFeedback({
        tone: 'warning',
        text: caught instanceof Error ? `⚠️ ${caught.message}` : '⚠️ Try again.',
      });
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
          // not depend on the browser clock (SPEC §8).
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
    if (!pollUserId) return;

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const poll = async () => {
      try {
        const response = await fetch(`/api/purchases/${encodeURIComponent(pollUserId)}`);
        if (cancelled) return;

        if (response.status === 404) {
          setPurchase((current) =>
            current ? { ...current, secured: false, status: 'CANCELLED' } : current,
          );
          return;
        }
        if (!response.ok) {
          throw new Error(`API returned ${response.status}`);
        }

        const data = (await response.json()) as PurchaseView;
        if (cancelled) return;
        setPurchase(data);

        if (!data.status || !finalStatuses.has(data.status)) {
          timer = setTimeout(poll, 2_000);
        }
      } catch {
        if (!cancelled) {
          timer = setTimeout(poll, 2_000);
        }
      }
    };

    void poll();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [pollUserId]);

  const serverNow = now + clockOffset;
  const countdown = sale
    ? sale.status === 'UPCOMING'
      ? formatCountdown(Date.parse(sale.startsAt) - serverNow)
      : sale.status === 'ACTIVE'
        ? formatCountdown(Date.parse(sale.endsAt) - serverNow)
        : null
    : null;

  const status = purchase?.status;
  const progress = stepIndex(status);
  const cancelled = status === 'CANCELLED' || status === 'REFUNDED' || status === 'SHIPPING_FAILED';

  return (
    <main className="page">
      <header>
        <p className="eyebrow">Phase 2 · atomic reservation</p>
        <h1>Flash sale</h1>
        <p className="lede">
          One unit per user, decided atomically in Redis. The sale window and stock below come from
          the same clock and keys the purchase path uses.
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

      <form className="buy-form" onSubmit={buy}>
        <label htmlFor="userId">User id (email or username)</label>
        <div className="row">
          <input
            id="userId"
            value={userId}
            onChange={(event) => setUserId(event.target.value)}
            placeholder="alice@example.com"
            maxLength={254}
            autoComplete="off"
          />
          <button
            type="submit"
            disabled={!userId.trim() || submitting || sale?.status !== 'ACTIVE'}
          >
            {submitting ? 'Buying…' : 'Buy Now'}
          </button>
        </div>
        {sale && sale.status !== 'ACTIVE' && (
          <p className="muted">Buy Now is enabled while the sale is live.</p>
        )}
      </form>

      {feedback && <p className={`feedback ${feedback.tone}`}>{feedback.text}</p>}

      {purchase && (
        <section className="purchase-card">
          <div className="purchase-header">
            <div>
              <p className="eyebrow">Your purchase</p>
              <strong>{purchase.userId}</strong>
            </div>
            <span className={`badge ${status ? status.toLowerCase() : 'queued'}`}>
              {status ? orderLabels[status] : 'Reserved'}
            </span>
          </div>

          {purchase.orderId && (
            <p className="muted">
              Order <code>{purchase.orderId}</code>
            </p>
          )}

          {cancelled ? (
            <p className="cancelled">
              {status === 'SHIPPING_FAILED' ? 'Shipping failed' : 'Cancelled'}
              {purchase.failureReason ? `: ${purchase.failureReason}` : '.'} You can try again while
              the sale is live.
            </p>
          ) : (
            <ol className="progress">
              {['Reserved', 'Paid', 'Shipped'].map((label, index) => (
                <li
                  key={label}
                  className={index < progress ? 'done' : index === progress ? 'current' : ''}
                >
                  <span>{index + 1}</span>
                  {label}
                </li>
              ))}
            </ol>
          )}
        </section>
      )}
    </main>
  );
}
