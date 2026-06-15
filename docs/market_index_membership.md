# Market Index Membership

Tickrake stores market index membership as a normalized SQLite model with three main tables:

- `tickers`: one row per current accepted ticker
- `market_indexes`: one row per market index
- `market_index_memberships`: the dated join table between an index and a ticker

There is also an alias table:

- `ticker_aliases`: maps historical ticker symbols onto the current accepted ticker row

## Data Model

### `tickers`

This table stores the current ticker identity and the current metadata snapshot.

Relevant columns:

- `id`
- `ticker`
- `security_name`
- `gics_sector`
- `gics_sub_industry`
- `headquarters_location`
- `cik`
- `founded`
- `status`

This table does not determine whether a ticker is in a given index. It only defines the ticker entity.

### `market_indexes`

This table stores index identities.

For the current backfill, the main example row is:

- `code = SP500`

### `market_index_memberships`

This is the actual membership history table.

Relevant columns:

- `market_index_id`
- `ticker_id`
- `start_date`
- `end_date`

Each row means:

- ticker `ticker_id` was a member of index `market_index_id`
- starting on `start_date`
- through `end_date`
- with blank `end_date` meaning the membership is still active

### `ticker_aliases`

This table exists so historical symbols still resolve to the current accepted ticker.

Relevant columns:

- `ticker_id`
- `alias_ticker`
- `start_date`
- `end_date`

Example:

- `META` exists once in `tickers`
- `FB` appears in `ticker_aliases` and points to `META`
- historical member queries still return `META`, not `FB`

## Query Behavior

The current member query contract is:

- queries return the current accepted ticker
- they do not return historical aliases
- membership is determined entirely from `market_index_memberships`

For the current S&P 500 dataset:

- a 2018 query returns `META`, not `FB`
- a 2018 query returns `COR`, not `ABC`

The same model works for other indexes if additional rows are loaded into `market_indexes` and `market_index_memberships`.

## CLI Examples

Query index membership for a historical date. The current concrete example uses `SP500`:

```bash
tickrake query --type members --index SP500 --as-of 2018-01-01
```

JSON output:

```bash
tickrake query --type members --index SP500 --as-of 2018-01-01 --format json
```

Typical result shape:

```json
{
  "type": "members",
  "index": "SP500",
  "as_of": "2018-01-01",
  "count": 503,
  "tickers": ["A", "AAL", "AAP", "AAPL", "COR", "META"]
}
```

## SQL Examples

Find active members for an index and date:

```sql
SELECT t.ticker
FROM market_index_memberships m
JOIN market_indexes i
  ON i.id = m.market_index_id
JOIN tickers t
  ON t.id = m.ticker_id
WHERE i.code = 'SP500'
  AND m.start_date <= '2018-01-01'
  AND (m.end_date IS NULL OR m.end_date >= '2018-01-01')
ORDER BY t.ticker;
```

Inspect alias mappings for one current ticker:

```sql
SELECT t.ticker, a.alias_ticker, a.start_date, a.end_date
FROM ticker_aliases a
JOIN tickers t
  ON t.id = a.ticker_id
WHERE t.ticker = 'META'
ORDER BY a.start_date, a.alias_ticker;
```

That should show `FB` as an alias of `META`, while the membership query still returns only `META`.
