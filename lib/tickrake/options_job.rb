# frozen_string_literal: true

module Tickrake
  class OptionsJob
    def initialize(runtime)
      @runtime = runtime
    end

    def run(now: Time.now)
      @runtime.logger.info("Starting options scrape at #{now.utc.iso8601}")
      client = @runtime.client_factory.build
      writer = Tickrake::OptionSampleWriter.new(client: client)
      run_time = now
      queue = build_queue(client)
      @runtime.logger.info("Resolved #{queue.length} option fetch tasks.")
      process_queue(queue, writer, run_time)
      @runtime.logger.info("Completed options scrape at #{Time.now.utc.iso8601}")
    end

    private

    def build_queue(client)
      @runtime.config.options_universe.flat_map do |entry|
        chain = client.get_option_expiration_chain(entry.symbol)
        Tickrake::DteResolver.new(
          expiration_chain: chain,
          target_buckets: @runtime.config.dte_buckets,
          option_root: entry.option_root
        ).resolve.map do |resolved|
          {
            symbol: entry.symbol,
            option_root: entry.option_root,
            resolved: resolved
          }
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
