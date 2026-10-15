# frozen_string_literal: true

require "tmpdir"

RSpec.describe "Shared Provider Rate Limiting" do
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }

  describe "SqliteRateLimiter" do
    it "maintains independent token and cooldown state across providers" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        schwab_limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 2,
          refill_rate: 1.0,
          logger: logger
        )
        custom_limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "custom_provider",
          capacity: 5,
          refill_rate: 1.0,
          logger: logger
        )

        # Exhaust schwab tokens
        expect(schwab_limiter.try_consume).to eq(true)
        expect(schwab_limiter.try_consume).to eq(true)
        expect(schwab_limiter.try_consume).to eq(false)

        # custom_provider should be unaffected
        expect(custom_limiter.try_consume).to eq(true)
        expect(custom_limiter.try_consume).to eq(true)

        # Apply cooldown to schwab
        schwab_limiter.apply_cooldown!(10.0)
        expect(schwab_limiter.current_cooldown).to be > 0
        expect(schwab_limiter.try_consume).to eq(false)

        # custom_provider should still have 0 cooldown and be consumable
        expect(custom_limiter.current_cooldown).to eq(0.0)
        expect(custom_limiter.try_consume).to eq(true)
      end
    end

    it "coordinates concurrent jobs sharing the same provider bucket" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 50,
          refill_rate: 10.0,
          logger: logger
        )

        # 5 concurrent threads attempting 10 acquisitions each
        consumed = 0
        mutex = Mutex.new

        threads = Array.new(5) do
          Thread.new do
            job_limiter = Tickrake::DB::SqliteRateLimiter.new(
              db_path,
              provider: "schwab",
              capacity: 50,
              refill_rate: 10.0
            )
            10.times do
              if job_limiter.try_consume
                mutex.synchronize { consumed += 1 }
              end
            end
          end
        end
        threads.each(&:join)

        # Exactly 50 tokens available, so exactly 50 acquisitions should succeed
        expect(consumed).to eq(50)
        expect(limiter.try_consume).to eq(false)
      end
    end

    it "waits for token replenishment and retries acquisition until success" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 1,
          refill_rate: 10.0, # refills 1 token in 0.1s
          logger: logger
        )

        expect(limiter.try_consume).to eq(true)
        expect(limiter.try_consume).to eq(false)

        # consume! should wait briefly and succeed
        t0 = Time.now
        expect(limiter.consume!(timeout: 1.0, retry_interval: 0.05)).to eq(true)
        expect(Time.now - t0).to be >= 0.08
      end
    end

    it "raises RateLimitWaitTimeoutError with provider and elapsed time when wait deadline expires" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 1,
          refill_rate: 0.001, # essentially no refill
          logger: logger
        )

        expect(limiter.try_consume).to eq(true)

        expect do
          limiter.consume!(timeout: 0.15, retry_interval: 0.05)
        end.to raise_error(Tickrake::RateLimitWaitTimeoutError) do |error|
          expect(error.provider).to eq("schwab")
          expect(error.elapsed_seconds).to be >= 0.14
          expect(error.message).to match(/Rate limit wait timeout for provider schwab after \d+(\.\d+)?s/)
        end
      end
    end

    it "applies shared cooldown with integer Retry-After" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 10,
          refill_rate: 10.0,
          logger: logger
        )

        limiter.apply_cooldown!("5")
        expect(limiter.current_cooldown).to be_within(0.5).of(5.0)
        expect(limiter.try_consume).to eq(false)
      end
    end

    it "applies shared cooldown with HTTP-date Retry-After" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 10,
          refill_rate: 10.0,
          logger: logger
        )

        future_date = (Time.now + 10).httpdate
        limiter.apply_cooldown!(future_date)
        expect(limiter.current_cooldown).to be_within(1.5).of(10.0)
        expect(limiter.try_consume).to eq(false)
      end
    end

    it "applies bounded fallback cooldown when Retry-After is missing or invalid" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 10,
          refill_rate: 10.0,
          logger: logger
        )

        # Missing Retry-After
        limiter.apply_cooldown!(nil, fallback: 15.0)
        expect(limiter.current_cooldown).to be_within(0.5).of(15.0)

        # Invalid Retry-After
        limiter.apply_cooldown!("invalid-date-string", fallback: 20.0)
        expect(limiter.current_cooldown).to be_within(0.5).of(20.0)

        # Unbounded large Retry-After is capped at 300s
        limiter.apply_cooldown!("999999", fallback: 30.0)
        expect(limiter.current_cooldown).to be <= 300.0
      end
    end

    it "emits low-cardinality structured visibility without credentials" do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, "tickrake.sqlite3")
        events = []
        custom_logger = instance_double(Logger)
        allow(custom_logger).to receive(:info) { |evt| events << evt }
        allow(custom_logger).to receive(:warn) { |evt| events << evt }
        allow(custom_logger).to receive(:error) { |evt| events << evt }

        limiter = Tickrake::DB::SqliteRateLimiter.new(
          db_path,
          provider: "schwab",
          capacity: 1,
          refill_rate: 10.0,
          logger: custom_logger
        )

        # First request consumes token
        limiter.consume!(timeout: 1.0)
        # Second request must wait
        limiter.consume!(timeout: 1.0, retry_interval: 0.05)
        # Apply cooldown
        limiter.apply_cooldown!(2.0)

        allowed_evt = events.find { |e| e[:event] == "provider_request" }
        expect(allowed_evt).to include(msg: "provider_request", provider: "schwab")

        waited_evt = events.find { |e| e[:event] == "rate_limit_waited" }
        expect(waited_evt).to include(msg: "rate_limit_waited", provider: "schwab")
        expect(waited_evt[:wait_duration_ms]).to be > 0

        cooldown_evt = events.find { |e| e[:event] == "provider_cooldown" }
        expect(cooldown_evt).to include(msg: "provider_cooldown", provider: "schwab")
        expect(cooldown_evt[:cooldown_seconds]).to be_within(0.5).of(2.0)
      end
    end
  end

  describe "PostgresRateLimiter" do
    let(:pg_db_name) { "tickrake_rate_limit_test_#{Time.now.to_i}_#{rand(1000..9999)}" }
    let(:pg_config) do
      Tickrake::DatabaseConfig.new(
        backend: "postgres",
        database: pg_db_name,
        host: "localhost"
      )
    end

    before(:all) do
      begin
        conn = PG.connect(dbname: "postgres")
        conn.close
        @pg_available = true
      rescue StandardError
        @pg_available = false
      end
    end

    around(:each) do |example|
      if @pg_available
        admin_conn = PG.connect(dbname: "postgres")
        admin_conn.exec("CREATE DATABASE #{pg_db_name}")
        admin_conn.close

        begin
          example.run
        ensure
          admin_conn = PG.connect(dbname: "postgres")
          admin_conn.exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '#{pg_db_name}' AND pid <> pg_backend_pid()") rescue nil
          admin_conn.exec("DROP DATABASE IF EXISTS #{pg_db_name}") rescue nil
          admin_conn.close rescue nil
        end
      else
        skip "Local PostgreSQL not running"
      end
    end

    it "coordinates concurrent jobs sharing the same provider bucket and applies cooldown" do
      Tickrake::Tracker.migrate!(pg_config)
      adapter = Tickrake::DB::Adapters::PostgresAdapter.new(pg_config)
      limiter = Tickrake::DB::PostgresRateLimiter.new(
        adapter,
        provider: "schwab",
        capacity: 2,
        refill_rate: 1.0,
        logger: logger
      )

      expect(limiter.try_consume).to eq(true)
      expect(limiter.try_consume).to eq(true)
      expect(limiter.try_consume).to eq(false)

      # Cooldown
      limiter.apply_cooldown!(5.0)
      expect(limiter.current_cooldown).to be_within(0.5).of(5.0)
      expect(limiter.try_consume).to eq(false)

      adapter.close
    end
  end

  describe "ProviderDefinition default rate limiting" do
    it "defaults Schwab REST to 110 requests per 60 seconds" do
      definition = Tickrake::ProviderDefinition.new(
        name: "schwab",
        adapter: "schwab",
        settings: {},
        symbol_map: {}
      )
      expect(definition.rate_limit_max_requests).to eq(110)
      expect(definition.rate_limit_interval_seconds).to eq(60)
    end

    it "honors explicitly configured rate limit settings for Schwab" do
      definition = Tickrake::ProviderDefinition.new(
        name: "schwab",
        adapter: "schwab",
        settings: { "rate_limit_max_requests" => 50, "rate_limit_interval_seconds" => 30 },
        symbol_map: {}
      )
      expect(definition.rate_limit_max_requests).to eq(50)
      expect(definition.rate_limit_interval_seconds).to eq(30)
    end

    it "does not default rate limits for non-schwab providers" do
      definition = Tickrake::ProviderDefinition.new(
        name: "ibkr",
        adapter: "ibkr",
        settings: {},
        symbol_map: {}
      )
      expect(definition.rate_limit_max_requests).to be_nil
      expect(definition.rate_limit_interval_seconds).to be_nil
    end
  end
end
