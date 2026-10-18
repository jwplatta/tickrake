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
│           └── <symbol>.ndjson              # Consolidated streaming raw event logs per day
│               # e.g. SPY.ndjson
│
├── order_book/
│   └── <provider>/                          # e.g. schwab
│       └── <YYYY>/<MM>/<DD>/
│           └── <symbol>.ndjson              # Consolidated streaming L2 depth event logs per day
│               # e.g. SPY.ndjson
│
├── candles/
│   └── <provider>/                          # e.g. schwab, ibkr
│       └── <frequency>/                     # e.g. 1min, 5min, 1d
│           └── <symbol>.csv
│               # e.g. SPY.csv, QQQ.csv
│
├── fundamentals/
│   └── <provider>/                          # e.g. schwab
│       └── <YYYY>/<MM>/
│           └── <DD>.ndjson                  # Daily corporate fundamentals & metrics
│               # e.g. 20.ndjson
│
└── economic_events/
    └── <source>/                            # e.g. bls, fred, alpha_vantage
        └── <category>/                      # e.g. economic, fomc, earnings
            └── <YYYY>/<MM>/
                └── <DD>.ndjson              # Scheduled macro & earnings events
                    # e.g. 14.ndjson
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
   - **`fundamentals`**: 30-day retention.
   - **`economic_events`**: 30-day retention.
3. **Direct-to-Folder Streaming (Eliminating `pending_events/`)**:
   Streaming jobs write consolidated `<symbol>.ndjson` files directly into `data/level_one/...` and `data/order_book/...` per symbol/day. Staging directories (`pending_events/`) and intermediate Tickrake Parquet writers are removed.

---

## 2. Options Chains Raw Contract (`options/*.csv`)

Option chain samples are emitted as point-in-time CSV snapshot files per expiration:
- **Filename**: `<root>_exp<YYYY-MM-DD>_<sample_date>_<sample_time>.csv`
- **CSV Encoding**: UTF-8 with header row.

### CSV Columns (41 Headers in Exact Order):
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
| 41 | `sampled_at` | `TIMESTAMPTZ` | Ingestion sample collection timestamp (`YYYY-MM-DDTHH:MM:SSZ`) |

---

## 3. Level One Stream Contract (`level_one/*.ndjson`)

Emitted as newline-delimited JSON (`.ndjson`). Each line is a single JSON object representing a quote or trade event.
- **Filename**: `<symbol>.ndjson` under `data/level_one/<provider>/<YYYY>/<MM>/<DD>/`.

### JSON Fields:
```json
{
  "job_type": "level_one",
  "provider": "schwab",
  "symbol": "SPY",
  "service": "LEVELONE_EQUITIES",
  "received_at": "2026-10-18T14:30:00Z",
  "quote_time": "2026-10-18T14:30:00Z",
  "trade_time": "2026-10-18T14:29:59Z",
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
- `received_at` (`TIMESTAMPTZ` / `string`): Local collector receipt timestamp in **UTC ISO 8601** (`YYYY-MM-DDTHH:MM:SSZ`).
- `quote_time` (`TIMESTAMPTZ` / `string | null`): Exchange/broker quote timestamp in **UTC ISO 8601** (`YYYY-MM-DDTHH:MM:SSZ`).
- `trade_time` (`TIMESTAMPTZ` / `string | null`): Exchange/broker last trade timestamp in **UTC ISO 8601** (`YYYY-MM-DDTHH:MM:SSZ`). Null if event is a quote update without a trade.
- `bid`, `ask`, `last`, `mark` (`float64 | null`): Market prices.
- `bid_size`, `ask_size`, `volume` (`int64 | null`): Market quantities and cumulative day volume.
- `extra_json` (`string | null`): Serialized JSON string containing raw upstream service attributes (e.g. Schwab field numbers 6–51: high/low, close, 52-week, P/E, exchange IDs). Tractatus unpacks this during normalization.

---

## 4. Order Book L2 Stream Contract (`order_book/*.ndjson`)

Emitted as newline-delimited JSON (`.ndjson`). Each line represents a market depth (Level 2) book update.
- **Filename**: `<symbol>.ndjson` under `data/order_book/<provider>/<YYYY>/<MM>/<DD>/`.

### JSON Fields:
```json
{
  "job_type": "order_book",
  "provider": "schwab",
  "symbol": "SPY",
  "service": "TOTAL_VIEW",
  "received_at": "2026-10-18T14:30:00Z",
  "book_time": "2026-10-18T14:30:00Z",
  "bids_json": "[{\"0\":585.12,\"1\":100,\"2\":5,\"3\":[{\"0\":\"ARCA\",\"1\":50,\"2\":1}]}]",
  "asks_json": "[{\"0\":585.14,\"1\":150,\"2\":8,\"3\":[{\"0\":\"NSDQ\",\"1\":100,\"2\":2}]}]"
}
```

### Field Definitions:
- `job_type` (`string`): Always `"order_book"`.
- `provider` (`string`): Adapter name (e.g. `"schwab"`).
- `symbol` (`string`): Ticker symbol (`"SPY"`).
- `service` (`string`): Upstream service name (e.g. `"TOTAL_VIEW"`).
- `received_at` (`TIMESTAMPTZ` / `string`): Local collector receipt timestamp in **UTC ISO 8601** (`YYYY-MM-DDTHH:MM:SSZ`).
- `book_time` (`TIMESTAMPTZ` / `string | null`): Exchange market snapshot timestamp in **UTC ISO 8601** (`YYYY-MM-DDTHH:MM:SSZ`).
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

## 6. Fundamentals Raw Contract (`fundamentals/*.ndjson`)

Daily equity fundamental metrics emitted as newline-delimited JSON (`.ndjson`), one line per symbol:
- **Filename**: `<DD>.ndjson` under `data/fundamentals/<provider>/<YYYY>/<MM>/`.
  - e.g. `fundamentals/schwab/2026/09/20.ndjson`

### Field Definitions (Exact Keys Emitted):
| Field | Type | Description |
| :--- | :--- | :--- |
| `symbol` | `string` | Ticker symbol (e.g. `"AAPL"`) |
| `sample_date` | `string` | Date of snapshot collection (`YYYY-MM-DD`) |
| `asset_type` | `string \| null` | Upstream asset classification (e.g. `"EQUITY"`) |
| `exchange` | `string \| null` | Listing exchange (e.g. `"NASDAQ"`) |
| `description` | `string \| null` | Company / instrument description |
| `cusip` | `string \| null` | 9-character CUSIP identifier |
| `pe_ratio` | `float64 \| null` | Price to Earnings ratio |
| `peg_ratio` | `float64 \| null` | Price/Earnings to Growth ratio |
| `pb_ratio` | `float64 \| null` | Price to Book ratio |
| `pr_ratio` | `float64 \| null` | Price to Revenue / Sales ratio |
| `pcf_ratio` | `float64 \| null` | Price to Cash Flow ratio |
| `gross_margin_ttm` | `float64 \| null` | Trailing 12 months gross margin percentage |
| `gross_margin_mrq` | `float64 \| null` | Most recent quarter gross margin percentage |
| `net_profit_margin_ttm` | `float64 \| null` | Trailing 12 months net profit margin |
| `net_profit_margin_mrq` | `float64 \| null` | Most recent quarter net profit margin |
| `operating_margin_ttm` | `float64 \| null` | Trailing 12 months operating margin |
| `operating_margin_mrq` | `float64 \| null` | Most recent quarter operating margin |
| `return_on_equity` | `float64 \| null` | Return on Equity (ROE) |
| `return_on_assets` | `float64 \| null` | Return on Assets (ROA) |
| `return_on_investment` | `float64 \| null` | Return on Investment (ROI) |
| `quick_ratio` | `float64 \| null` | Acid-test / quick ratio |
| `current_ratio` | `float64 \| null` | Current assets to current liabilities ratio |
| `interest_coverage` | `float64 \| null` | Times interest earned |
| `total_debt_to_capital` | `float64 \| null` | Total debt / total capital |
| `lt_debt_to_equity` | `float64 \| null` | Long-term debt to equity ratio |
| `total_debt_to_equity` | `float64 \| null` | Total debt to equity ratio |
| `eps_ttm` | `float64 \| null` | Trailing 12 months Earnings Per Share |
| `eps_change_percent_ttm` | `float64 \| null` | EPS percent change TTM |
| `eps_change` | `float64 \| null` | Dollar EPS change |
| `eps_change_year` | `float64 \| null` | 1-year EPS change |
| `eps` | `float64 \| null` | Latest EPS figure |
| `rev_change_year` | `float64 \| null` | 1-year revenue change percentage |
| `rev_change_ttm` | `float64 \| null` | Trailing 12 months revenue change |
| `rev_change_in` | `float64 \| null` | Intermediate revenue change |
| `shares_outstanding` | `float64 \| null` | Total common shares outstanding |
| `market_cap_float` | `float64 \| null` | Floating shares market capitalization |
| `market_cap` | `float64 \| null` | Total market capitalization |
| `book_value_per_share` | `float64 \| null` | Book value per common share |
| `short_int_to_float` | `float64 \| null` | Short interest as percentage of float |
| `short_int_day_to_cover` | `float64 \| null` | Days to cover short interest |
| `fund_leverage_factor` | `float64 \| null` | Fund leverage factor multiplier (for leveraged ETFs) |
| `is_hard_to_borrow` | `boolean \| null` | Broker HTB borrow indicator |
| `is_shortable` | `boolean \| null` | Short locate availability flag |
| `htb_quantity` | `int64 \| null` | Available HTB shares available to borrow |
| `htb_rate` | `float64 \| null` | Annualized borrow fee rate percentage |
| `dividend_amount` | `float64 \| null` | Regular dividend payment dollar amount |
| `dividend_yield` | `float64 \| null` | Annualized dividend yield percentage |
| `dividend_date` | `string \| null` | Prior dividend date |
| `dividend_pay_date` | `string \| null` | Prior dividend payment date |
| `dividend_pay_amount` | `float64 \| null` | Prior payment dollar amount |
| `declaration_date` | `string \| null` | Dividend declaration announcement date |
| `next_div_ex_date` | `string \| null` | Next upcoming ex-dividend date |
| `next_div_pay_date` | `string \| null` | Next upcoming dividend payout date |
| `div_growth_rate_3year` | `float64 \| null` | 3-year compound annual dividend growth rate |
| `dividend_freq` | `int64 \| null` | Annual distribution frequency (e.g. `4` for quarterly) |
| `beta` | `float64 \| null` | Systematic risk sensitivity coefficient |
| `high_52` | `float64 \| null` | 52-week high market price |
| `low_52` | `float64 \| null` | 52-week low market price |
| `close_price` | `float64 \| null` | Prior day session closing price |
| `last_price` | `float64 \| null` | Most recent trade price |
| `net_change` | `float64 \| null` | Session net dollar price change |
| `net_percent_change` | `float64 \| null` | Session net percent change |
| `total_volume` | `int64 \| null` | Cumulative day trading volume |
| `avg_10_days_volume` | `float64 \| null` | Average daily volume over 10 trading days |
| `avg_1_day_volume` | `float64 \| null` | Prior 1-day volume |
| `avg_3_month_volume` | `float64 \| null` | Average daily volume over 3 months |
| `avg_1_year_volume` | `float64 \| null` | Average daily volume over 1 year |
| `last_earnings_date` | `string \| null` | Date of last reported company earnings (`YYYY-MM-DDTHH:MM:SSZ`) |
| `sampled_at` | `TIMESTAMPTZ` / `string` | Local collector scrape timestamp in UTC ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) |

---

## 7. Economic Events Raw Contract (`economic_events/*.ndjson`)

Scheduled macro releases, central bank decisions, and earnings events emitted as newline-delimited JSON (`.ndjson`):
- **Filename**: `<DD>.ndjson` under `data/economic_events/<source>/<category>/<YYYY>/<MM>/`.
  - e.g. `economic_events/bls/economic/2026/10/14.ndjson`
  - e.g. `economic_events/fred/fomc/2026/09/17.ndjson`
  - e.g. `economic_events/alpha_vantage/earnings/2026/10/15.ndjson`

### Field Definitions (Exact Keys Emitted):
| Field | Type | Description |
| :--- | :--- | :--- |
| `event_datetime` | `TIMESTAMPTZ` / `string` | Scheduled release / announcement time in UTC ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) |
| `event_name` | `string` | Name of event / economic release (e.g. `"CPI"`, `"GDP"`, `"FOMC Rate Decision"`, `"Earnings"`) |
| `category` | `string` | Category grouping: `"economic"`, `"fomc"`, `"earnings"` |
| `source` | `string` | Feed source provider: `"bls"`, `"fred"`, `"alpha_vantage"` |
| `actual` | `float64 \| null` | Released actual reported value (null if pending future announcement) |
| `estimate` | `float64 \| null` | Consensus / market median forecast estimate |
| `previous` | `float64 \| null` | Prior period reported value |
| `unit` | `string \| null` | Measurement unit (e.g. `"percent"`, `"USD"`, `"Index"`) |
| `symbol` | `string \| null` | Equity ticker symbol for company events (e.g. `"AAPL"` for earnings, null for macro) |
| `company_name` | `string \| null` | Full corporate entity name for earnings events |
| `time_of_day` | `string \| null` | Session timing for earnings releases (e.g. `"pre-market"`, `"post-market"`, null for macro) |
| `release_id` | `int64 \| null` | Upstream provider release identifier (e.g. FRED release ID `53`) |
| `series_id` | `string \| null` | Upstream series code (e.g. FRED series `"GDP"` or `"DFEDTARU"`) |
| `sampled_at` | `TIMESTAMPTZ` / `string` | Local collector scrape timestamp in UTC ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) |

---

## 8. Structured Logging Specification for Ingestion & Dashboarding

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
