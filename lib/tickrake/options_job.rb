# frozen_string_literal: true

module Tickrake
  class OptionsJob
    def initialize(runtime)
      @runtime = runtime
    end

    def run(now: Time.now)
      unless @runtime.provider_definition.adapter == "schwab"
        raise Tickrake::Error, "OptionsJob currently supports provider=schwab only."
      end

      @runtime.logger.info("Starting options scrape at #{now.utc.iso8601}")
      client = @runtime.client_factory.build
      run_time = now
      queue = build_queue(client, run_time.to_date)
      @runtime.logger.info("Resolved #{queue.length} option fetch tasks.")
      process_queue(queue, client, run_time)
      @runtime.logger.info("Completed options scrape at #{Time.now.utc.iso8601}")
    end

    private

    def build_queue(_client, base_date)
      buckets = @runtime.config.dte_buckets.uniq.sort

      @runtime.config.options_universe.flat_map do |entry|
        buckets.map do |bucket|
          {
            symbol: entry.symbol,
            option_root: entry.option_root,
            expiration_date: base_date + bucket,
            requested_buckets: [bucket]
          }
        end
      end
    end

    def process_queue(queue, client, run_time)
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

            fetch_one(job, client, run_time)
          end
        end
      end.each(&:join)
    end

    def fetch_one(job, client, run_time)
      @runtime.logger.info(
        "Fetching option chain for #{job.fetch(:symbol)} exp=#{job.fetch(:expiration_date)} buckets=#{job.fetch(:requested_buckets).join(',')}"
      )
      id = @runtime.tracker.record_start(
        job_type: "options_monitor",
        dataset_type: "options",
        symbol: job.fetch(:symbol),
        option_root: job[:option_root],
        requested_buckets: job.fetch(:requested_buckets),
        resolved_expiration: job.fetch(:expiration_date).iso8601,
        scheduled_for: run_time,
        started_at: Time.now
      )

      retries = 0
      begin
        result = Timeout.timeout(@runtime.config.option_fetch_timeout_seconds) do
          option_sample_downloader.resolve(
            client: client,
            symbol: job.fetch(:symbol),
            expiration_date: job.fetch(:expiration_date),
            directory: @runtime.config.options_dir,
            format: "csv",
            timestamp: run_time,
            root: job[:option_root]
          )
        end
        path = extract_output_path(result)
        @runtime.logger.info("Wrote option chain for #{job.fetch(:symbol)} to #{path}")
        @runtime.tracker.record_finish(id: id, status: "success", finished_at: Time.now, output_path: path)
      rescue StandardError => e
        retries += 1
        if retries <= @runtime.config.retry_count
          @runtime.logger.warn("Retry #{retries} for #{job.fetch(:symbol)} exp=#{job.fetch(:expiration_date)}: #{e.message}")
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.logger.error("Failed option fetch for #{job.fetch(:symbol)} exp=#{job.fetch(:expiration_date)}: #{e.message}")
        @runtime.tracker.record_finish(id: id, status: "failed", finished_at: Time.now, error_message: e.message)
      end
    end

    def option_sample_downloader
      return SchwabRb::OptionSample::Downloader if defined?(SchwabRb::OptionSample::Downloader)

      raise Tickrake::Error,
            "Installed schwab_rb gem does not provide SchwabRb::OptionSample::Downloader. Upgrade schwab_rb before running option sampling."
    end

    def extract_output_path(result)
      return result.last if result.is_a?(Array) && result.length >= 2
      return result if result.is_a?(String)

      raise Tickrake::Error, "Unexpected SchwabRb::OptionSample::Downloader.resolve result: #{result.class}"
    end
  end
end
