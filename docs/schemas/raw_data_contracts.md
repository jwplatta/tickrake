# Tickrake Raw Data Contracts & Staging Specifications

This document defines the authoritative raw data schemas, directory hierarchies, and MinIO staging contracts produced by Tickrake for downstream consumption and normalization by **Tractatus**.

---

## 1. Directory Layout & Staging Bucket Structure

Tickrake organizes raw scraped and streamed data on disk under `~/.tickrake/data/`. The MinIO staging bucket (`tickrake-staging`) mirrors this identical folder structure:

```text
s3://tickrake-staging/
├── options/
│   └── <provider>/                          # e.g. schwab, massive
│       └── <YYYY>/<MM>/<DD>/
│           └── <root>_exp<expiration>_<date>_<time>.csv
│               # e.g. SPXW_exp2026-10-18_2026-10-18_14-30-00.csv
│
├── level_one/
│   └── <provider>/                          # e.g. schwab
│       └── <YYYY>/<MM>/<DD>/
│           └── <symbol>_<timestamp>.ndjson  # Direct streaming raw event logs
│               # e.g. SPY_20261018_143000Z.ndjson
│
├── order_book/
│   └── <provider>/                          # e.g. schwab
│       └── <YYYY>/<MM>/<DD>/
│           └── <symbol>_<timestamp>.ndjson  # Direct streaming L2 depth event logs
│               # e.g. SPY_20261018_143000Z.ndjson
│
└── candles/
    └── <provider>/                          # e.g. schwab, ibkr
        └── <frequency>/                     # e.g. 1min, 5min, 1d
            └── <symbol>.csv
                # e.g. SPY.csv, QQQ.csv
```

### Staging Lifecycle & Retention Policy
1. **Upload & Prune Local Filetree**:
   Once Tickrake successfully pushes a sealed raw file to `s3://tickrake-staging/...`, it purges the local file from `~/.tickrake/data/` to prevent unbounded local disk growth.
2. **Staging Retention & Eviction**:
   To prevent object bloat in MinIO while giving Tractatus ample time to ingest, the staging bucket implements an automatic retention lifecycle:
   - **`options`**: 7-day retention (raw snapshot CSVs accumulate fast; Tractatus compacts daily).
   - **`level_one`**: 7-day retention.
   - **`order_book`**: 5-day retention (L2 depth files are large).
   - **`candles`**: 30-day retention.
3. **Direct-to-Folder Streaming (Eliminating `pending_events/`)**:
   Streaming jobs write rotated `.ndjson` files directly to `data/level_one/...` and `data/order_book/...`. Staging directories (`pending_events/`) and intermediate Tickrake Parquet writers are removed.

---

## 2. Options Chains Raw Contract (`options/*.csv`)

Option chain samples are emitted as point-in-time CSV snapshot files per expiration:
- **Filename**: `<root>_exp<YYYY-MM-DD>_<sample_date>_<sample_time>.csv`
- **CSV Encoding**: UTF-8 with header row.

### CSV Columns (40 Headers in Exact Order):
| Index | Column Name | Type | Description |
| :--- | :--- | :--- | :--- |
| 1 | `contract_type` | `VARCHAR` | `"CALL"` or `"PUT"` |
| 2 | `symbol` | `VARCHAR` | Full OSI symbol (e.g. `"SPXW  261018C05000000"`) |
| 3 | `description` | `VARCHAR` | Human-readable contract description |
| 4 | `strike` | `DOUBLE` | Strike price (e.g. `5000.0`) |
| 5 | `expiration_date` | `DATE` | ISO 8601 expiration date (`YYYY-MM-DD`) |
| 6 | `open` | `DOUBLE` | Day open price |
| 7 | `high` | `DOUBLE` | Day high price |
| 8 | `low` | `DOUBLE` | Day low price |
| 9 | `close` | `DOUBLE` | Prior close price |
| 10 | `mark` | `DOUBLE` | Current mark price (midpoint or close) |
| 11 | `bid` | `DOUBLE` | Current best bid price |
| 12 | `bid_size` | `BIGINT` | Current best bid size |
| 13 | `ask` | `DOUBLE` | Current best ask price |
| 14 | `ask_size` | `BIGINT` | Current best ask size |
| 15 | `last` | `DOUBLE` | Last trade price |
| 16 | `last_size` | `DOUBLE` | Last trade size |
| 17 | `open_interest` | `DOUBLE` | Total open contracts |
| 18 | `total_volume` | `DOUBLE` | Day trading volume |
| 19 | `transactions` | `BIGINT` | Total trade transaction count |
| 20 | `delta` | `DOUBLE` | Option Greek: Delta |
| 21 | `gamma` | `DOUBLE` | Option Greek: Gamma |
| 22 | `theta` | `DOUBLE` | Option Greek: Theta |
| 23 | `vega` | `DOUBLE` | Option Greek: Vega |
| 24 | `rho` | `DOUBLE` | Option Greek: Rho |
| 25 | `volatility` | `DOUBLE` | Implied volatility |
| 26 | `theoretical_volatility` | `DOUBLE` | Theoretical / model volatility |
| 27 | `theoretical_option_value` | `DOUBLE` | Model theoretical value |
| 28 | `intrinsic_value` | `DOUBLE` | Intrinsic value |
| 29 | `extrinsic_value` | `DOUBLE` | Extrinsic / time value |
| 30 | `underlying_price` | `DOUBLE` | Underlying asset spot price |
| 31 | `quote_at` | `TIMESTAMPTZ` | Upstream feed quote time (`YYYY-MM-DDTHH:MM:SSZ`) |
| 32 | `trade_at` | `TIMESTAMPTZ` | Upstream feed trade time (nullable, `YYYY-MM-DDTHH:MM:SSZ`) |
| 33 | `time_value` | `DOUBLE` | Time value component |
| 34 | `net_change` | `DOUBLE` | Net dollar change from previous close |
| 35 | `percent_change` | `DOUBLE` | Percent change from previous close |
| 36 | `settlement_type` | `VARCHAR` | Settlement type (`"AM"`, `"PM"`) |
| 37 | `exercise_type` | `VARCHAR` | Exercise style (`"AMERICAN"`, `"EUROPEAN"`) |
| 38 | `expiration_type` | `VARCHAR` | Expiration frequency (`"REGULAR"`, `"WEEKLY"`) |
| 39 | `multiplier` | `BIGINT` | Contract unit multiplier (typically `100`) |
| 40 | `non_standard` | `BOOLEAN` | Boolean flag (`true`/`false`) for non-standard deliverables |

---

## 3. Level One Stream Contract (`level_one/*.ndjson`)

Emitted as newline-delimited JSON (`.ndjson`). Each line is a single JSON object representing a quote or trade event.

### JSON Fields:
```json
{
  "job_type": "level_one",
  "provider": "schwab",
  "symbol": "SPY",
  "service": "LEVELONE_EQUITIES",
  "received_at": 1729261800123,
  "quote_time_ms": 1729261800115,
  "trade_time_ms": 1729261799890,
  "bid": 585.12,
  "ask": 585.14,
  "last": 585.13,
  "bid_size": 20,
  "ask_size": 15,
  "volume": 42150120,
  "mark": 585.13,
  "extra_json": "{\"1\":\"585.12\",\"2\":\"585.14\",\"103\":586.2,\"104\":583.1}"
}
```

### Field Definitions:
- `job_type` (`string`): Always `"level_one"`.
- `provider` (`string`): Adapter name (e.g. `"schwab"`).
- `symbol` (`string`): Ticker symbol (`"SPY"`, `"^ES"`).
- `service` (`string`): Upstream service name (e.g. `"LEVELONE_EQUITIES"`, `"LEVELONE_FUTURES"`).
- `received_at` (`int64`): Local collector receipt timestamp in **UTC epoch milliseconds**. This is the authoritative point-in-time timestamp.
- `quote_time_ms` (`int64 | null`): Exchange/broker quote timestamp in UTC epoch milliseconds.
- `trade_time_ms` (`int64 | null`): Exchange/broker last trade timestamp in UTC epoch milliseconds. Null if event is a quote update without a trade.
- `bid`, `ask`, `last`, `mark` (`float64 | null`): Market prices.
- `bid_size`, `ask_size`, `volume` (`int64 | null`): Market quantities and cumulative day volume.
- `extra_json` (`string | null`): Serialized JSON string containing raw upstream service attributes (e.g. Schwab field numbers 6–51: high/low, close, 52-week, P/E, exchange IDs). Tractatus unpacks this during normalization.

---

## 4. Order Book L2 Stream Contract (`order_book/*.ndjson`)

Emitted as newline-delimited JSON (`.ndjson`). Each line represents a market depth (Level 2) book update.

### JSON Fields:
```json
{
  "job_type": "order_book",
  "provider": "schwab",
  "symbol": "SPY",
  "service": "TOTAL_VIEW",
  "received_at": 1729261800123,
  "book_time_ms": 1729261800110,
  "bids_json": "[{\"0\":585.12,\"1\":100,\"2\":5,\"3\":[{\"0\":\"ARCA\",\"1\":50,\"2\":1}]}]",
  "asks_json": "[{\"0\":585.14,\"1\":150,\"2\":8,\"3\":[{\"0\":\"NSDQ\",\"1\":100,\"2\":2}]}]"
}
```

### Field Definitions:
- `received_at` (`int64`): Local collector receipt timestamp in **UTC epoch milliseconds**.
- `book_time_ms` (`int64`): Exchange market snapshot timestamp in UTC epoch milliseconds.
- `bids_json` / `asks_json` (`string`): Serialized JSON array of price level objects with Schwab/exchange depth encoding:
  - `"0"`: Price (`float64`)
  - `"1"`: Aggregate aggregate volume/size at price level (`int64`)
  - `"2"`: Number of orders at price level (`int64`)
  - `"3"`: List of venue/exchange components:
    - `"0"`: Market center / venue code (`string`, e.g. `"ARCA"`, `"NSDQ"`)
    - `"1"`: Size at venue (`int64`)
    - `"2"`: Venue sequence ID (`int64`)

*Tractatus normalizes `bids_json` and `asks_json` into nested Arrow structs: `list<struct<price, size, num_orders, venues: list<struct<venue, size, sequence_id>>>>`.*

---

## 5. Candles Raw Contract (`candles/*.csv`)

Underlying bar candles emitted as standard CSV files:
- **Filename**: `<symbol>.csv` under `data/candles/<provider>/<frequency>/`.
- **Headers**:
  ```csv
  datetime,open,high,low,close,volume
  2026-10-18T14:30:00Z,585.10,585.50,585.05,585.45,125000
  ```
- **Datetime Format**: ISO 8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`).
- **Prices**: `float64`.
- **Volume**: `int64`.

---

## 6. Structured Logging Specification for Ingestion & Dashboarding

Every raw file sync or upload to the staging bucket emits structured JSON logs at `INFO` level to simplify observability, alerting, and log aggregators (e.g. Vector, Loki, Datadog):

```json
{
  "event": "staging_upload",
  "data_type": "options",
  "provider": "schwab",
  "symbol": "SPXW",
  "date": "2026-10-18",
  "filename": "SPXW_exp2026-10-18_2026-10-18_14-30-00.csv",
  "remote_uri": "s3://tickrake-staging/options/schwab/2026/10/18/SPXW_exp2026-10-18_2026-10-18_14-30-00.csv",
  "row_count": 52,
  "byte_count": 14205,
  "duration_ms": 42,
  "purged_local_file": true
}
```
