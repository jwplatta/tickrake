# Configuration and Environment Modernization

## Overview

Tickrake configuration management has been modernized to use `.env` files, ERB templating, Rails ActiveRecord database conventions, and streamlined top-level object datastore targets.

---

## 1. Automatic `.env` Loading & Precedence

Tickrake uses `dotenv` to load environment variables into `ENV` before processing configuration files.

### Precedence Order
1. Explicit CLI argument: `--env-file path/to/.env`
2. Environment variable: `ENV["TICKRAKE_ENV_FILE"]`
3. Current working directory: `./.env`
4. Tickrake home directory: `~/.tickrake/.env` (or `$TICKRAKE_HOME/.env`)

If an explicit path or `TICKRAKE_ENV_FILE` is given and does not exist, Tickrake will raise `Tickrake::ConfigError`. Existing environment variables are never overwritten by `.env` (`Dotenv.load`).

---

## 2. ERB Templating in YAML

All YAML configuration files (including external universe YAML files) are evaluated with ERB before YAML parsing.

Example:
```yaml
database:
  adapter: <%= ENV.fetch('TICKRAKE_DB_ADAPTER', 'sqlite3') %>
  database: <%= ENV.fetch('TICKRAKE_DB_DATABASE', Tickrake::PathSupport.sqlite_path) %>
```

---

## 3. Rails ActiveRecord Database Configuration

The configuration follows Rails ActiveRecord conventions under the `database:` block:

- **`adapter`**: Either `sqlite3` or `postgresql`. The legacy term `backend` is deprecated and removed.
- **`database`**:
  - For SQLite: path to `.sqlite3` file (defaults to `~/.tickrake/tickrake.sqlite3` or `TICKRAKE_HOME`). The root `sqlite_path:` key is removed.
  - For PostgreSQL: database name.
- **PostgreSQL parameters**: `host`, `port`, `user` (or `username`), `password`, `sslmode`, `connect_timeout`, `url`.

### SQLite Example
```yaml
database:
  adapter: sqlite3
  database: ~/.tickrake/tickrake.sqlite3
```

### PostgreSQL Example
```yaml
database:
  adapter: postgresql
  host: <%= ENV.fetch('TICKRAKE_DB_HOST', 'postgres') %>
  port: <%= ENV.fetch('TICKRAKE_DB_PORT', 5432) %>
  database: <%= ENV.fetch('TICKRAKE_DB_NAME', 'tickrake') %>
  user: <%= ENV.fetch('TICKRAKE_DB_USER', 'tickrake_user') %>
  password: <%= ENV.fetch('TICKRAKE_DB_PASSWORD', '') %>
```

---

## 4. Top-Level Datastore Targets (`staging_target` & `intraday_target`)

Generic `datastores:` and obsolete `s3_archive` mappings have been replaced with two top-level datastore targets:

- `staging_target:` target storage for daily scraping datasets (`options`, `candles`, `order_book`, `level_one`, etc.). Used by `tickrake publish-staging`, `tickrake purge-staging`, and `StagingPublisherJob`.
- `intraday_target:` target storage for intraday index publishing. Used by `IntradayPublisherJob`.

### Example
```yaml
staging_target:
  type: <%= ENV.fetch('STAGING_DATASTORE_TYPE', 's3_compatible') %>
  endpoint: <%= ENV.fetch('MINIO_ENDPOINT', 'http://minio:9000') %>
  bucket: <%= ENV.fetch('TICKRAKE_STAGING_BUCKET', 'tickrake-staging') %>
  access_key_id: <%= ENV.fetch('MINIO_ACCESS_KEY', 'minioadmin') %>
  secret_access_key: <%= ENV.fetch('MINIO_SECRET_KEY', 'minioadmin') %>

intraday_target:
  type: <%= ENV.fetch('INTRADAY_DATASTORE_TYPE', 's3_compatible') %>
  endpoint: <%= ENV.fetch('MINIO_ENDPOINT', 'http://minio:9000') %>
  bucket: <%= ENV.fetch('TICKRAKE_INTRADAY_BUCKET', 'tickrake-intraday') %>
  access_key_id: <%= ENV.fetch('MINIO_ACCESS_KEY', 'minioadmin') %>
  secret_access_key: <%= ENV.fetch('MINIO_SECRET_KEY', 'minioadmin') %>
```

---

## 5. CLI Updates

- Added `--env-file PATH` to `validate-config`, `migrate`, `publish-staging`, and `purge-staging`.
- Removed `--datastore NAME` option since targets are unambiguously specified by `staging_target` and `intraday_target`.
