# frozen_string_literal: true

module Tickrake
  class OptionsJob
    def initialize(runtime)
      @runtime = runtime
    end

    def run(now: Time.now)
      client = @runtime.client_factory.build
      run_time = now
      queue = build_queue(client)
      process_queue(queue, client, run_time)
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
        response = Timeout.timeout(@runtime.config.option_fetch_timeout_seconds) do
          client.get_option_chain(
            job.fetch(:symbol),
            contract_type: SchwabRb::Option::ContractTypes::ALL,
            strike_range: SchwabRb::Option::StrikeRanges::ALL,
            from_date: job.fetch(:resolved).date,
            to_date: job.fetch(:resolved).date,
            return_data_objects: false
          )
        end
        filtered = Tickrake::Serializers.filter_option_chain(response, job[:option_root])
        root = job[:option_root] || job.fetch(:symbol)
        path = Tickrake::Serializers.option_path(
          directory: @runtime.config.options_dir,
          root: root,
          expiration_date: job.fetch(:resolved).date,
          sampled_at: run_time
        )
        Tickrake::Serializers.write_option_csv(path, filtered)
        @runtime.tracker.record_finish(id: id, status: "success", finished_at: Time.now, output_path: path)
      rescue StandardError => e
        retries += 1
        if retries <= @runtime.config.retry_count
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.tracker.record_finish(id: id, status: "failed", finished_at: Time.now, error_message: e.message)
      end
    end
  end
end
