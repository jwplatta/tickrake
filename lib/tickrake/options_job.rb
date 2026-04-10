# frozen_string_literal: true

module Tickrake
  class OptionsJob
    EXPIRATION_PROBE_BUFFER_DAYS = 7

    def initialize(runtime)
      @runtime = runtime
    end

    def run(now: Time.now)
      @runtime.logger.info("Starting options scrape at #{now.utc.iso8601}")
      client = @runtime.client_factory.build
      writer = Tickrake::OptionSampleWriter.new(client: client)
      run_time = now
      queue = build_queue(client, run_time.to_date)
      @runtime.logger.info("Resolved #{queue.length} option fetch tasks.")
      process_queue(queue, writer, run_time)
      @runtime.logger.info("Completed options scrape at #{Time.now.utc.iso8601}")
    end

    private

    def build_queue(client, base_date)
      @runtime.config.options_universe.flat_map do |entry|
        expiration_entries = probe_expiration_entries(client, entry, base_date)
        if expiration_entries.empty?
          @runtime.logger.warn("No expirations returned for #{entry.symbol}")
          next []
        end

        Tickrake::DteResolver.new(
          expiration_entries: expiration_entries,
          target_buckets: @runtime.config.dte_buckets
        ).resolve.map do |resolved|
          {
            symbol: entry.symbol,
            option_root: entry.option_root,
            resolved: resolved
          }
        end
      end
    end

    def probe_expiration_entries(client, entry, base_date)
      api_symbol = SchwabRb::PriceHistory::Downloader.api_symbol(entry.symbol)
      max_bucket = @runtime.config.dte_buckets.max || 0

      (0..(max_bucket + EXPIRATION_PROBE_BUFFER_DAYS)).filter_map do |offset|
        expiration_date = base_date + offset
        response = client.get_option_chain(
          api_symbol,
          contract_type: SchwabRb::Option::ContractTypes::ALL,
          strike_range: SchwabRb::Option::StrikeRanges::ALL,
          from_date: expiration_date,
          to_date: expiration_date,
          return_data_objects: false
        )
        next unless matching_contracts?(response, entry.option_root)

        Tickrake::OptionExpirationEntry.new(
          expiration_date: expiration_date.iso8601,
          days_to_expiration: offset
        )
      end
    end

    def matching_contracts?(response, option_root)
      rows = option_rows(response)
      return rows.any? if option_root.nil? || option_root.empty?

      normalized_root = option_root.to_s.upcase
      rows.any? { |row| row[:optionRoot].to_s.upcase == normalized_root }
    end

    def option_rows(response)
      [response[:callExpDateMap], response[:putExpDateMap]].compact.flat_map do |date_map|
        date_map.values.flat_map do |strikes|
          strikes.values.flatten.map { |option| option.transform_keys(&:to_sym) }
        end
      end
    end

    def process_queue(queue, writer, run_time)
      index = 0
      mutex = Mutex.new
      worker_count = [@runtime.config.max_workers, queue.length].min

      Array.new(worker_count) do
        Thread.new do
          loop do
            job = mutex.synchronize do
              current = queue[index]
              index += 1 if current
              current
            end
            break unless job

            fetch_one(job, writer, run_time)
          end
        end
      end.each(&:join)
    end

    def fetch_one(job, writer, run_time)
      @runtime.logger.info(
        "Fetching option chain for #{job.fetch(:symbol)} exp=#{job.fetch(:resolved).date} buckets=#{job.fetch(:resolved).requested_buckets.join(',')}"
      )
      id = @runtime.tracker.record_start(
        job_type: "options_monitor",
        dataset_type: "options",
        symbol: job.fetch(:symbol),
        option_root: job[:option_root],
        requested_buckets: job.fetch(:resolved).requested_buckets,
        resolved_expiration: job.fetch(:resolved).date.iso8601,
        scheduled_for: run_time,
        started_at: Time.now
      )

      retries = 0
      begin
        path = Timeout.timeout(@runtime.config.option_fetch_timeout_seconds) do
          writer.write(
            symbol: job.fetch(:symbol),
            option_root: job[:option_root],
            expiration_date: job.fetch(:resolved).date,
            directory: @runtime.config.options_dir,
            timestamp: run_time
          )
        end
        @runtime.logger.info("Wrote option chain for #{job.fetch(:symbol)} to #{path}")
        @runtime.tracker.record_finish(id: id, status: "success", finished_at: Time.now, output_path: path)
      rescue StandardError => e
        retries += 1
        if retries <= @runtime.config.retry_count
          @runtime.logger.warn("Retry #{retries} for #{job.fetch(:symbol)} exp=#{job.fetch(:resolved).date}: #{e.message}")
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.logger.error("Failed option fetch for #{job.fetch(:symbol)} exp=#{job.fetch(:resolved).date}: #{e.message}")
        @runtime.tracker.record_finish(id: id, status: "failed", finished_at: Time.now, error_message: e.message)
      end
    end
  end
end
