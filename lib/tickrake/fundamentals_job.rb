# frozen_string_literal: true

module Tickrake
  class FundamentalsJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run(now: Time.now)
      sample_date = now.to_date
      collection_id = "fundamentals-#{now.utc.strftime("%Y%m%dT%H%M%SZ")}"
      @runtime.logger.info({ msg: "scrape_start", event: "scrape_start", data_type: "fundamentals", collection_id: collection_id })

      symbols = selected_symbols
      if symbols.empty?
        @runtime.logger.warn("fundamentals: no symbols configured")
        return ScheduledRunResult.new(success_count: 0, failure_count: 0)
      end

      started_at = Time.now
      begin
        rows = fetch_all(symbols, sample_date, now)
        path = write_parquet(rows, sample_date)
        elapsed_ms = ((Time.now - started_at) * 1000).round
        @runtime.logger.info({
          msg: "scrape_end", event: "scrape_end", data_type: "fundamentals",
          symbol_count: rows.length, duration_ms: elapsed_ms, path: path,
          collection_id: collection_id
        })
        write_sidecar(
          sample_date: sample_date, scheduled_for: now, started_at: started_at,
          status: "success", output_path: path, row_count: rows.length,
          collection_id: collection_id
        )
        ScheduledRunResult.new(success_count: rows.length, failure_count: 0)
      rescue StandardError => e
        elapsed_ms = ((Time.now - started_at) * 1000).round
        @runtime.logger.error({
          msg: "scrape_failed", event: "scrape_failed", data_type: "fundamentals",
          error_class: e.class.name, error_message: e.message,
          duration_ms: elapsed_ms, collection_id: collection_id
        })
        write_sidecar(
          sample_date: sample_date, scheduled_for: now, started_at: started_at,
          status: "failed", error_message: e.message, collection_id: collection_id
        )
        ScheduledRunResult.new(success_count: 0, failure_count: 1)
      end
    end

    private

    def fetch_all(symbols, sample_date, now)
      instrument_data = fetch_instruments(symbols)
      quote_supplements = fetch_quote_supplements(symbols)
      fetched_at = Time.now.utc.iso8601

      symbols.filter_map do |symbol|
        inst = instrument_data[symbol]
        next unless inst

        fundamental = inst[:fundamental] || {}
        supplement = quote_supplements[symbol] || {}

        build_row(symbol, inst, fundamental, supplement, sample_date, fetched_at)
      end
    end

    def fetch_instruments(symbols)
      batch_size = @scheduled_job&.settings&.fetch("batch_size", 50) || 50
      result = {}

      symbols.each_slice(batch_size) do |batch|
        rate_limiter&.consume!
        data = with_retries("get_instruments batch of #{batch.length}") do
          client.get_instruments(batch, SchwabRb::Orders::Instrument::Projections::FUNDAMENTAL, return_data_objects: false)
        end

        instruments = data[:instruments] || []
        instruments.each do |inst|
          sym = inst[:symbol]
          result[sym] = inst if sym
        end
      end

      result
    end

    def fetch_quote_supplements(symbols)
      batch_size = @scheduled_job&.settings&.fetch("batch_size", 50) || 50
      result = {}

      symbols.each_slice(batch_size) do |batch|
        rate_limiter&.consume!
        data = with_retries("get_quotes batch of #{batch.length}") do
          client.get_quotes(batch, return_data_objects: false)
        end

        data.each do |sym, quote_data|
          fundamental = quote_data[:fundamental]
          next unless fundamental

          result[sym.to_s] = {
            last_earnings_date: fundamental[:lastEarningsDate],
            avg_1_year_volume: fundamental[:avg1YearVolume]
          }
        end
      end

      result
    end

    def build_row(symbol, inst, fundamental, supplement, sample_date, fetched_at)
      {
        symbol: symbol,
        sample_date: sample_date.iso8601,
        asset_type: inst[:assetType],
        exchange: inst[:exchange],
        description: inst[:description],
        pe_ratio: fundamental[:peRatio],
        peg_ratio: fundamental[:pegRatio],
        pb_ratio: fundamental[:pbRatio],
        pr_ratio: fundamental[:prRatio],
        pcf_ratio: fundamental[:pcfRatio],
        gross_margin_ttm: fundamental[:grossMarginTTM],
        gross_margin_mrq: fundamental[:grossMarginMRQ],
        net_profit_margin_ttm: fundamental[:netProfitMarginTTM],
        net_profit_margin_mrq: fundamental[:netProfitMarginMRQ],
        operating_margin_ttm: fundamental[:operatingMarginTTM],
        operating_margin_mrq: fundamental[:operatingMarginMRQ],
        return_on_equity: fundamental[:returnOnEquity],
        return_on_assets: fundamental[:returnOnAssets],
        return_on_investment: fundamental[:returnOnInvestment],
        quick_ratio: fundamental[:quickRatio],
        current_ratio: fundamental[:currentRatio],
        interest_coverage: fundamental[:interestCoverage],
        total_debt_to_capital: fundamental[:totalDebtToCapital],
        lt_debt_to_equity: fundamental[:ltDebtToEquity],
        total_debt_to_equity: fundamental[:totalDebtToEquity],
        eps_ttm: fundamental[:epsTTM],
        eps_change_percent_ttm: fundamental[:epsChangePercentTTM],
        eps: fundamental[:eps],
        rev_change_year: fundamental[:revChangeYear],
        rev_change_ttm: fundamental[:revChangeTTM],
        shares_outstanding: fundamental[:sharesOutstanding],
        market_cap_float: fundamental[:marketCapFloat],
        market_cap: fundamental[:marketCap],
        book_value_per_share: fundamental[:bookValuePerShare],
        short_int_to_float: fundamental[:shortIntToFloat],
        short_int_day_to_cover: fundamental[:shortIntDayToCover],
        dividend_amount: fundamental[:dividendAmount],
        dividend_yield: fundamental[:dividendYield],
        dividend_date: fundamental[:dividendDate],
        dividend_pay_date: fundamental[:dividendPayDate],
        dividend_pay_amount: fundamental[:dividendPayAmount],
        div_growth_rate_3year: fundamental[:divGrowthRate3Year],
        dividend_freq: fundamental[:dividendFreq],
        beta: fundamental[:beta],
        high_52: fundamental[:high52],
        low_52: fundamental[:low52],
        avg_10_days_volume: fundamental[:avg10DaysVolume],
        avg_1_day_volume: fundamental[:avg1DayVolume],
        avg_3_month_volume: fundamental[:avg3MonthVolume],
        avg_1_year_volume: supplement[:avg_1_year_volume] || fundamental[:avg1YearVolume],
        last_earnings_date: supplement[:last_earnings_date],
        fetched_at: fetched_at
      }
    end

    def write_parquet(rows, sample_date)
      provider_name = @scheduled_job&.provider || "schwab"
      path = storage_paths.fundamentals_path(provider: provider_name, sample_date: sample_date)
      parquet_writer.write(path, rows: rows)
      path
    end

    def write_sidecar(sample_date:, scheduled_for:, started_at:, status:, output_path: nil, error_message: nil, row_count: nil, collection_id: nil)
      ts = scheduled_for.utc.strftime("%Y%m%dT%H%M%SZ")

      fetch_run = {
        "job_type" => @scheduled_job&.name || "fundamentals",
        "dataset_type" => "fundamentals",
        "symbol" => nil,
        "frequency" => nil,
        "option_root" => nil,
        "requested_buckets" => nil,
        "resolved_expiration" => nil,
        "scheduled_for" => scheduled_for.utc.iso8601,
        "started_at" => started_at.utc.iso8601,
        "finished_at" => Time.now.utc.iso8601,
        "status" => status,
        "output_path" => output_path,
        "error_message" => error_message,
        "collection_id" => collection_id
      }

      file_metadata = nil
      if status == "success" && output_path
        stat = File.stat(output_path)
        observed_at = scheduled_for.utc.iso8601
        file_metadata = {
          "path" => output_path,
          "dataset_type" => "fundamentals",
          "provider_name" => @scheduled_job&.provider || "schwab",
          "ticker" => "all",
          "frequency" => nil,
          "expiration_date" => nil,
          "row_count" => row_count,
          "first_observed_at" => observed_at,
          "last_observed_at" => observed_at,
          "file_mtime" => stat.mtime.to_i,
          "file_size" => stat.size,
          "updated_at" => Time.now.utc.iso8601,
          "collection_id" => collection_id
        }
      end

      sidecar = { "fetch_run" => fetch_run, "file_metadata" => file_metadata }
      pending_dir = @runtime.config.pending_metadata_dir
      FileUtils.mkdir_p(pending_dir)
      basename = "fundamentals_#{sample_date.iso8601}_#{ts}.meta.json"
      sidecar_path = File.join(pending_dir, basename)
      tmp_path = "#{sidecar_path}.tmp"
      File.write(tmp_path, JSON.generate(sidecar))
      File.rename(tmp_path, sidecar_path)
    end

    def selected_symbols
      universe = @scheduled_job&.universe || []
      universe.map { |entry| entry.respond_to?(:symbol) ? entry.symbol : entry.to_s }
    end

    def storage_paths
      @storage_paths ||= Storage::Paths.new(@runtime.config)
    end

    def parquet_writer
      @parquet_writer ||= Storage::FundamentalsParquetWriter.new
    end

    def client
      @runtime.client_factory.build
    end

    def rate_limiter
      return @rate_limiter if defined?(@rate_limiter)

      provider_name = @scheduled_job&.provider || "schwab"
      provider_def = @runtime.config.provider_definition(provider_name)
      max_req = provider_def.rate_limit_max_requests
      interval = provider_def.rate_limit_interval_seconds

      @rate_limiter = if max_req && interval
        Tickrake::DB::SqliteRateLimiter.new(
          @runtime.config,
          provider: provider_name,
          capacity: max_req,
          refill_rate: max_req.to_f / interval
        )
      end
    end

    def with_retries(label)
      attempts = 0
      begin
        attempts += 1
        yield
      rescue StandardError => e
        if attempts <= @runtime.config.retry_count
          @runtime.logger.warn("fundamentals: retry #{attempts} for #{label}: #{e.message}")
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.logger.error("fundamentals: exhausted retries for #{label}: #{e.message}")
        raise
      end
    end
  end
end
