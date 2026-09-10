# Plan: Order Book Streaming Jobs

## Context

The schwab_rb gem added WebSocket streaming via `SchwabRb::Stream::Client` (commit 64cd4c7, 2026-09-05). This plan adds a first-class `order_book` job type to the tickrake DSL that streams Level 2 order book data, buffers it in SQLite for durability, and periodically flushes to per-symbol Parquet files archived to S3.

**Schwab streaming services — full catalog (14 services):**
- **Level 1 Quotes** (real-time bid/ask, last, volume): `LEVELONE_EQUITIES`, `LEVELONE_OPTIONS`, `LEVELONE_FUTURES`, `LEVELONE_FUTURES_OPTIONS`, `LEVELONE_FOREX`
- **Level 2 Order Book** (bid/ask depth): `NYSE_BOOK`, `NASDAQ_BOOK`, `OPTIONS_BOOK`
- **Chart Bars** (OHLCV candles): `CHART_EQUITY`, `CHART_FUTURES`
- **Screeners**: `SCREENER_EQUITY`, `SCREENER_OPTION`
- **Account Activity**: `ACCT_ACTIVITY`

**Scope of this plan:** Level 2 Order Book only (`NYSE_BOOK`, `NASDAQ_BOOK`, `OPTIONS_BOOK`). Future PRs handle Level 1 Quotes, Chart Bars, etc. using the same infrastructure pattern.

---

## DSL Shape

**Equity order books** — symbols are static tickers, no runtime resolution:
```ruby
Tickrake.job("equity_order_book") do
  provider :schwab
  type :order_book
  symbols ["SPY", "QQQ"]
  schedule { daily; windows [["08:30", "15:00"]] }
  order_book do
    services [:nyse_book, :nasdaq_book]
    flush_interval 60     # seconds between SQLite→Parquet flushes
    retention_days 30     # how long to keep flushed rows in SQLite
  end
end
```

**Options order book** — contracts resolved dynamically and refreshed throughout the session:
```ruby
Tickrake.job("spx_options_book") do
  provider :schwab
  type :order_book
  schedule { daily; windows [["08:30", "15:00"]] }  # We're working with CST/CDT
  order_book do
    services [:options_book]
    flush_interval 60
    retention_days 30

    contracts do
      underlying "SPXW" # forget how we request these from schwab, but we need the weeklys here which have the root SPXW rather than SPX
      atm_strikes 5           # ATM ± 5 strikes
      expirations front: 2    # front 2 expirations by date
      re_resolve_interval 30  # minutes; re-fetches chain and ADDs new contracts mid-session
    end
  end
end
```

**DSL validation rules (enforced at build time):**
- `services` includes `options_book` → `contracts` block required; top-level `symbols` ignored.
- `services` includes equity books only → top-level `symbols` required, non-empty.
- Mixing equity book services with `options_book` in the same job is not allowed.

**Symbol conflict detection:** Jobs run in isolated containers sharing a SQLite volume. Conflict detection is handled via the `job_sessions` table (see below) — a shared session registry written by all job types for cross-container coordination. At `order_book` session start, the job checks for live sessions from other jobs with overlapping symbols before subscribing to the stream.

---

## Architecture

```
WebSocket thread (schwab_rb):
  event → handle_event() → INSERT INTO order_book_events (job_name, symbol, flushed=0)
                            [Monitor lock, ~microseconds]

Flush thread (inside OrderBookJob#run_session):
  every flush_interval seconds:
    - if re_resolve_interval elapsed: ContractResolver#resolve → stream.on() ADD new symbols
    - SELECT WHERE job_name=? AND flushed=0
    - group by symbol → write one Parquet file per symbol → S3 upload
    - UPDATE SET flushed=1

OrderBookRunner main thread:
  every 10s: check in_window?
  on window exit: job.stop → stream_thread.join → final flush
```

---

## Contract Resolution (options_book only)

**Class:** `Tickrake::OrderBook::ContractResolver`

Uses `client.get_option_chain(underlying, strike_count: atm_strikes, include_underlying_quote: true)` — `strike_count` is "strikes above and below ATM" in the Schwab API. Selects the front N expirations by date, returns all call + put OCC symbols.

```ruby
def resolve
  chain = @client.get_option_chain(
    @underlying,
    strike_count: @atm_strikes,
    include_underlying_quote: true
  )
  expirations = chain.option_expiration_list
                     .sort_by(&:expiration_date)
                     .first(@front_n)
  expirations.flat_map { |exp| exp.calls.map(&:symbol) + exp.puts.map(&:symbol) }
end
```

**Re-resolution in the flush loop:**
- `@last_resolved_at` starts nil, triggering resolution before `stream.start_async`.
- Each flush iteration checks: `(Time.now - @last_resolved_at) >= re_resolve_interval_seconds`.
- New symbols: diff against `@subscribed_symbols` → `stream.on(:options_book, symbols: new_only, ...)` sends `ADD` to existing connection.
- `@subscribed_symbols |= new_symbols` — never unsubscribe within a session.

---

## SQLite Tables

### `job_sessions` — migration version 10 (shared across all job types)

A cross-container session registry. All job runners write here at start/stop and heartbeat during execution. Used for coordination (e.g., symbol conflict detection for streaming jobs) and operational visibility.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER PK | autoincrement |
| `job_name` | TEXT NOT NULL | unique per running instance |
| `job_type` | TEXT NOT NULL | e.g. `"order_book"`, `"options"`, `"candles"` |
| `provider` | TEXT | |
| `parameters_json` | TEXT | job-type-specific params (symbols, services, etc.) as JSON |
| `started_at` | INTEGER NOT NULL | Unix milliseconds |
| `heartbeat_at` | INTEGER NOT NULL | Unix milliseconds; updated every ~30s |

**Index:** `(heartbeat_at)` — fast staleness check.

**Session lifecycle:**
1. **Session start:** Delete any stale record for this `job_name` (leftover from crash), then INSERT a new record.
2. **During session:** Heartbeat updates `heartbeat_at` every 30 seconds (folded into the flush loop or a dedicated thread).
3. **Session end (`ensure`):** `DELETE WHERE job_name = ?`.

**Staleness threshold:** Sessions with `heartbeat_at < (now - 2 * heartbeat_interval)` are considered dead. A crashed container's session expires automatically within ~60 seconds.

**Conflict check (order_book jobs):** At session start, after inserting the session record, query for other live sessions with `job_type = "order_book"` whose `parameters_json` symbols overlap. Raise `Tickrake::Error` if found.

**Future use:** All job types (`options`, `candles`, `maintenance`) should also write session records. This gives operational visibility into what's running across containers without needing external service discovery.

---

### `order_book_events` — migration version 11

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER PK | autoincrement |
| `job_name` | TEXT NOT NULL | partitions rows by job |
| `received_at` | INTEGER NOT NULL | Unix milliseconds |
| `symbol` | TEXT NOT NULL | OCC symbol for options; equity ticker otherwise |
| `service` | TEXT NOT NULL | e.g. `"NYSE_BOOK"`, `"OPTIONS_BOOK"` |
| `book_time_ms` | INTEGER | Schwab book timestamp; may be nil on partial updates |
| `bids_json` | TEXT | JSON array of bid levels |
| `asks_json` | TEXT | JSON array of ask levels |
| `flushed` | INTEGER NOT NULL DEFAULT 0 | 0=pending, 1=written to Parquet |

**Index:** `(job_name, flushed, received_at)` — fast range queries per job.

**Retention:**
- `flushed=1` rows pruned at session start: `DELETE WHERE job_name=? AND flushed=1 AND received_at < cutoff`.
- `flushed=0` rows never pruned — survive crashes, flushed on next session start.
- Default `retention_days`: 30.

---

## Parquet File Organization

Per-symbol, per-flush-interval. Job name stays in the buffer only — not in file paths:

```
~/.tickrake/data/order_book/schwab/2026/09/07/
  SPY_093000Z.parquet
  QQQ_093000Z.parquet
  SPXW__261219C04500000_093000Z.parquet
  SPXW__261219P04500000_093000Z.parquet
```

**Path:** `order_book/{provider}/{YYYY}/{MM}/{DD}/{sanitized_symbol}_{flush_start_utc}Z.parquet`

**Query example:** `SELECT * FROM read_parquet('~/.tickrake/data/order_book/schwab/2026/09/07/SPY_*.parquet')`

**Parquet schema:**

| Column | Parquet type |
|--------|-------------|
| `received_at_ms` | int64 |
| `symbol` | string |
| `service` | string |
| `book_time_ms` | int64 |
| `bids_json` | string |
| `asks_json` | string |

---

## Files to Create

| File | Purpose |
|------|---------|
| `lib/tickrake/db/migrations/010_create_job_sessions.rb` | Migration v10: creates `job_sessions` table (shared, all job types) |
| `lib/tickrake/db/migrations/011_create_order_book_events.rb` | Migration v11: creates `order_book_events` table + index |
| `lib/tickrake/dsl/order_book_contracts_builder.rb` | DSL sub-builder for `contracts do ... end` block |
| `lib/tickrake/dsl/order_book_builder.rb` | DSL sub-builder for `order_book do ... end` block |
| `lib/tickrake/order_book/contract_resolver.rb` | Fetches option chain, resolves ATM contract symbols |
| `lib/tickrake/storage/order_book_parquet_writer.rb` | Parquet writer with order book schema |
| `lib/tickrake/order_book_job.rb` | Streaming + flush logic; owns SQLite connection + Monitor |
| `lib/tickrake/order_book_runner.rb` | Window-aware scheduler runner; mirrors `CandlesSchedulerRunner` |

## Files to Modify

| File | Change |
|------|--------|
| `lib/tickrake/dsl/job_builder.rb` | Add `order_book` DSL method + `build_order_book_job!` |
| `lib/tickrake/job_runner.rb` | Add `when "order_book"` dispatch case |
| `lib/tickrake/storage/paths.rb` | Add `order_book_dir` and `order_book_path` methods |
| `lib/tickrake/tracker.rb` | Append migrations v10 and v11 to `self.migrations` |
| `lib/tickrake.rb` / `lib/tickrake/dsl.rb` | Add requires for all new files (v10 and v11 migrations first) |

---

## `OrderBookJob` Key Methods

**`run_session(window_start:)`:**
1. Prune old flushed rows from `order_book_events`.
2. **Recovery flush:** `SELECT WHERE job_name=? AND flushed=0`. If any stranded rows exist (from a previous crash or force-stop), call `flush_pending_events` with `flush_start = Time.at(min_received_at / 1000.0)` and `flush_end = Time.now`. Log a warning: `"Recovering N unflushed rows from previous session"`. Rows stay `flushed=0` if this flush fails — next session recovers again.
3. Register session in `job_sessions` (upsert own record; heartbeat thread starts).
4. Conflict check: query `job_sessions` for other live `order_book` sessions with overlapping symbols → raise `Tickrake::Error` if found.
5. Resolve initial contracts (options_book) or use static symbols (equity books).
6. Register stream callbacks via `stream.on(service, symbols:, fields:)`.
7. `stream.start_async` — stream runs in background thread.
8. Flush loop on calling thread: re-resolution check → `flush_pending_events` → `interruptible_sleep` → heartbeat update.
9. On `@stop_requested`: `stream.stop` → final flush → delete own `job_sessions` record.

**`handle_event(event, service:)`** — stream callback thread:
- Inserts each content entry as a row; Monitor lock held only for SQL execute.

**`flush_pending_events(flush_start:, flush_end:)`:**
1. SELECT unflushed rows for this job up to `flush_end`.
2. Group by symbol.
3. Per symbol: write Parquet (atomic tmp+rename) → S3 upload.
4. UPDATE `flushed=1` for all flushed row IDs.

---

## `OrderBookRunner` Structure

Mirrors `CandlesSchedulerRunner` closely:
- `run`: Lockfile → signal handlers → `with_timezone` → 10s polling loop.
- On window entry: start `@job.run_session` in `@stream_thread`.
- On window exit / shutdown: `@job.stop` → `@stream_thread.join(60)`.
- `in_window?` / `install_signal_handlers`: copied verbatim from `CandlesSchedulerRunner`.
- `ensure`: `@job.close`, `JobRegistry.new.delete(...)`.

---

## Shutdown Sequence

1. SIGTERM → `@shutdown_requested = true`
2. Runner calls `@job.stop` (sets `@stop_requested`)
3. Flush loop exits sleep → `stream.stop` (blocks until WebSocket thread joins)
4. Final `flush_pending_events` drains remaining rows per-symbol
5. `@stream_thread.join(60)` returns
6. `ensure`: SQLite connection closed, JobRegistry entry deleted

**Invariant:** No flushed=0 rows remain after clean shutdown.

**Crash/force-stop invariant:** Stranded `flushed=0` rows are recovered at the start of the next session. Recovery flush uses `min(received_at)` of stranded rows as `flush_start` — files are named after recovery time, not original capture time, which is acceptable. Stranded rows accumulate only between sessions and are bounded by `retention_days`.

---

## Verification

1. Load DSL file → migration applies, no load errors.
2. Equity book: events appear in `order_book_events`; per-symbol `.parquet` files written after one flush interval.
3. Options book: contract symbols logged at session start; new contracts added after `re_resolve_interval`.
4. Conflict check: second job with overlapping symbols raises `Tickrake::Error` at startup.
5. SIGTERM → clean shutdown, no `flushed=0` rows remain for the job.
6. Force-stop (SIGKILL / `docker stop --time=0`): restart job → recovery flush warning logged → stranded rows written to Parquet before stream starts.
7. S3: files appear at expected keys if archives configured.
