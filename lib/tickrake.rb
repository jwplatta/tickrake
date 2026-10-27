# frozen_string_literal: true

require "active_record"
require "csv"
require "date"
require "digest"
require "fileutils"
require "json"
require "logger"
require "optparse"
require "pathname"
require "ruby-progressbar"
require "sqlite3"
require "time"
require "tmpdir"
require "timeout"
require "yaml"

require "schwab_rb"

module Tickrake
end

# Core Foundation & Configuration
require_relative "tickrake/version"
require_relative "tickrake/errors"
require_relative "tickrake/core/path_support"
require_relative "tickrake/core/env_loader"
require_relative "tickrake/core/logger_factory"
require_relative "tickrake/core/log_retention"
require_relative "tickrake/core/lockfile"
require_relative "tickrake/core/progress_reporter"
require_relative "tickrake/database_config"
require_relative "tickrake/config"
require_relative "tickrake/config_loader"
require_relative "tickrake/core/symbol_normalizer"
require_relative "tickrake/core/frequency_normalizer"
require_relative "tickrake/core/scheduled_run_result"

# Models & Data Structures
require_relative "tickrake/models/bar"
require_relative "tickrake/models/option_sample_row"
require_relative "tickrake/models/base"
require_relative "tickrake/models/fetch_run"
require_relative "tickrake/models/file_metadata"
require_relative "tickrake/models/market_index"
require_relative "tickrake/models/ticker"
require_relative "tickrake/models/ticker_alias"
require_relative "tickrake/models/market_index_membership"
require_relative "tickrake/models/api_rate_limit"
require_relative "tickrake/models/historical_file_ledger"

# Database & State Management
require_relative "tickrake/db/connection_manager"
require_relative "tickrake/db/migrator"
require_relative "tickrake/tracker"
require_relative "tickrake/core/rate_limiter"

# Storage & Persistence
require_relative "tickrake/storage/csv_writer"
require_relative "tickrake/storage/paths"
require_relative "tickrake/storage/option_sample_writer"
require_relative "tickrake/storage/candle_reconciler"
require_relative "tickrake/storage/candle_metadata"
require_relative "tickrake/storage/candle_metadata_sync"
require_relative "tickrake/storage/candles_scanner"
require_relative "tickrake/storage/options_scanner"
require_relative "tickrake/storage/s3_archive"
require_relative "tickrake/storage/staging_publisher"
require_relative "tickrake/storage/staging_purger"

# Importers
require_relative "tickrake/importers/massive_option_symbol"
require_relative "tickrake/importers/massive_options_importer"

# Public Data Loader
require_relative "tickrake/data_loader"

# Provider Integrations
require_relative "tickrake/providers/base"
require_relative "tickrake/providers/schwab"
require_relative "tickrake/providers/ibkr"
require_relative "tickrake/provider_factory"
require_relative "tickrake/core/client_factory"

# Runtime & Process Supervision
require_relative "tickrake/core/runtime"
require_relative "tickrake/core/job_registry"
require_relative "tickrake/core/job_runner"
require_relative "tickrake/core/job_control"
require_relative "tickrake/core/background_process"
require_relative "tickrake/core/foreground_process"
require_relative "tickrake/core/scheduler_supervisor"
require_relative "tickrake/core/scheduled_runner_support"

# Streaming Jobs & Components
require_relative "tickrake/streaming/event_stream_writer"
require_relative "tickrake/streaming/stream_config"
require_relative "tickrake/streaming/stream_job"
require_relative "tickrake/streaming/stream_runner"
require_relative "tickrake/streaming/level_one_job"
require_relative "tickrake/streaming/level_one_runner"
require_relative "tickrake/streaming/chart_stream_job"
require_relative "tickrake/streaming/chart_stream_runner"
require_relative "tickrake/streaming/order_book_job"
require_relative "tickrake/streaming/order_book_runner"
require_relative "tickrake/streaming/order_book/contract_resolver"

# Batch Jobs & Runners
require_relative "tickrake/jobs/candles_job"
require_relative "tickrake/jobs/candles_scheduler_runner"
require_relative "tickrake/jobs/options_job"
require_relative "tickrake/jobs/options_monitor_runner"
require_relative "tickrake/jobs/fundamentals_job"
require_relative "tickrake/jobs/fundamentals_scheduler_runner"
require_relative "tickrake/jobs/metadata_sync_job"
require_relative "tickrake/jobs/metadata_sync_scheduler_runner"
require_relative "tickrake/jobs/intraday_publisher_job"
require_relative "tickrake/jobs/intraday_publisher_scheduler_runner"
require_relative "tickrake/jobs/staging_publisher_job"
require_relative "tickrake/jobs/staging_publisher_scheduler_runner"
require_relative "tickrake/jobs/clients/fred_client"
require_relative "tickrake/jobs/clients/bls_calendar"
require_relative "tickrake/jobs/clients/fred_economic_calendar"
require_relative "tickrake/jobs/clients/fomc_calendar"
require_relative "tickrake/jobs/clients/alpha_vantage_earnings"
require_relative "tickrake/jobs/economic_events_job"
require_relative "tickrake/jobs/economic_events_scheduler_runner"

# CLI & DSL
require_relative "tickrake/cli"
require_relative "tickrake/dsl"
