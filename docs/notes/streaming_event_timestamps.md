# Streaming Event Timestamps: Semantics and Known Behavior

This document defines the timestamps recorded by Tickrake for streaming market data (Level One quotes/trades and Level Two order book depth) and details their operational semantics, relationship, clock drift considerations, and analysis caveats.

---

## 1. Overview of Timestamps

### Level One Events (`level_one`)

Level One events (such as `LEVELONE_EQUITIES`, `LEVELONE_OPTIONS`, `LEVELONE_FUTURES`, `LEVELONE_FUTURES_OPTIONS`, and `LEVELONE_FOREX`) capture quote updates (bid/ask prices and sizes) and trade updates. In the resulting Parquet schema and raw event payloads, three distinct timestamps are tracked:

| Field | Source / Origin | Description | Nullable? |
|---|---|---|---|
| `received_at_ms` (or `received_at`) | Local collector system clock | Milliseconds since Unix epoch (UTC) when the Tickrake collector received the WebSocket message packet. | **No** (always present) |
| `quote_time_ms` | Broker / Exchange feed | Milliseconds since Unix epoch (UTC) attached to the quote information (bid and ask). | Yes (e.g. trade-only updates) |
| `trade_time_ms` | Broker / Exchange feed | Milliseconds since Unix epoch (UTC) attached to the last-trade information. | Yes (often absent when only quotes update) |

### Level Two Order Book Events (`order_book`)

Level Two order books (`NYSE_BOOK`, `NASDAQ_BOOK`, `OPTIONS_BOOK`) capture market depth across multiple price levels:

| Field | Source / Origin | Description | Nullable? |
|---|---|---|---|
| `received_at_ms` (or `received_at`) | Local collector system clock | Milliseconds since Unix epoch (UTC) when the Tickrake collector received the order book message. | **No** (always present) |
| `book_time_ms` | Broker / Exchange feed | Milliseconds since Unix epoch (UTC) corresponding to the market snapshot time of the order book. | Yes |

---

## 2. Semantics and Key Takeaways

### `received_at_ms`: The Point-in-Time Anchor

- `received_at_ms` reflects local collector receipt time.
- **Safest Cutoff**: For point-in-time backtesting and asking *"what data did my model or system actually possess by time $T$?"*, `received_at_ms` is the authoritative reference. Upstream exchange timestamps cannot represent local availability due to transit delays.

### `quote_time_ms` vs. `trade_time_ms`: Independent Clocks & Timings

Quotes and trades frequently occur and update independently:
- A quote update can occur without a trade.
- A trade can execute at an existing quote without changing top-of-book prices immediately.
- Each event row carries the timestamp pertinent to the data inside that message.

**Concrete Example:**
Consider two consecutive `/ES` Level One messages:

1. **Row 1:**
   - Quote time: `20:57:26.487 UTC` (`quote_time_ms = 1695761846487`)
   - Trade time: `20:57:26.346 UTC` (`trade_time_ms = 1695761846346`)
   - Receive time: `20:57:26.763 UTC` (`received_at_ms = 1695761846763`)
   *Analysis*: The trade occurred ~141 ms before the quote update. The collector received the packet containing both updates at `20:57:26.763 UTC`, which was 276 ms after the quote timestamp.

2. **Row 2:**
   - Quote time: `20:57:27.128 UTC`
   - Receive time: `20:57:27.100 UTC`
   *Analysis*: Here, the quote timestamp appears **28 ms later than the local receive timestamp**.

---

## 3. Important Caveats and Pitfalls

### Do Not Use `received_at_ms - quote_time_ms` as a Precise Latency Metric
As shown in Row 2 above, small timing reversals (where `quote_time_ms > received_at_ms`) can and do occur. This happens because:
1. Upstream exchange matching engines, Schwab feeder gateways, and local collector hosts run on independent hardware clocks that may experience slight clock skew (often single-digit to tens of milliseconds).
2. Upstream feed conflation and aggregation layers can timestamp packets at different stages of pipeline delivery.
Therefore, `received_at_ms - quote_time_ms` should **not** be assumed to be a pure or precise network latency measurement.

### Absence of `trade_time_ms` (`nil` or `NaT`)
When `trade_time_ms` is missing or null:
- It **only** means that this specific Level One message did not include a trade update.
- It does **not** mean that the instrument has never traded or did not trade during the day. Most Level One messages are quote updates (bid/ask adjustments) where trade fields are omitted.

### Presence of `trade_time_ms` Does Not Guarantee a New Trade
Do not assume that every row containing a non-null `trade_time_ms` represents a new trade execution. Feeds frequently repeat the latest trade price and last-trade timestamp alongside new quote updates. Before recording a new trade event in downstream analytics, verify whether:
- The `trade_time_ms` or trade sequence has advanced relative to the previous row.
- The volume or trade size has increased.
- The broker specifically marked the trade field as modified.
