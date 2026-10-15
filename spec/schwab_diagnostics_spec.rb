# frozen_string_literal: true

require "json"
require "stringio"
require "tmpdir"

RSpec.describe "Schwab REST HTTP Response Diagnostics and Correlation" do
  let(:fake_session) { double("session", expired?: false) }
  let(:fake_token_manager) { double("token_manager", token_age: 100) }
  let(:logger_output) { StringIO.new }
  let(:test_logger) do
    Logger.new(logger_output).tap do |l|
      l.formatter = proc { |_sev, _datetime, _prog, msg| "#{msg}\n" }
    end
  end

  let(:client) do
    SchwabRb::Client.new(
      "test_api_key",
      "test_app_secret",
      fake_session,
      token_manager: fake_token_manager,
      enforce_enums: false
    ).tap do |c|
      c.diagnostic_logger = test_logger
    end
  end

  def make_response(status:, body: "", headers: {})
    headers = {
      "content-type" => "application/json",
      "content-length" => body.bytesize.to_s
    }.merge(headers)
    double("response", status: status, body: body, headers: headers)
  end

  describe "Correlation ID assignment and uniqueness" do
    it "assigns each request a correlation ID containing the job name and process ID" do
      Thread.current[:tickrake_job_name] = "options_worker"
      response = make_response(status: 200, body: '{"candles":[]}')
      allow(fake_session).to receive(:get).and_return(response)

      client.get_price_history("SPY")

      output = logger_output.string
      expect(output).to match(/Req options_worker-#{Process.pid}-[0-9a-f]{8}-\d+: GET to/)
      expect(output).to match(/Resp options_worker-#{Process.pid}-[0-9a-f]{8}-\d+: Status 200/)
    ensure
      Thread.current[:tickrake_job_name] = nil
    end

    it "generates distinct, non-overlapping correlation IDs across concurrent threads" do
      response = make_response(status: 200, body: '{"candles":[]}')
      allow(fake_session).to receive(:get).and_return(response)

      ids = []
      mutex = Mutex.new

      threads = Array.new(10) do |i|
        Thread.new do
          Thread.current[:tickrake_job_name] = "worker_#{i}"
          client.get_price_history("SPY")
          metadata = Thread.current[:tickrake_last_response_metadata]
          mutex.synchronize { ids << metadata[:correlation_id] }
        end
      end
      threads.each(&:join)

      expect(ids.length).to eq(10)
      expect(ids.uniq.length).to eq(10)
      ids.each do |id|
        expect(id).to match(/worker_\d+-#{Process.pid}-[0-9a-f]{8}-\d+/)
      end
    end
  end

  describe "Response metadata preservation" do
    it "records status, content type, declared length, actual body bytes, duration, and success parse outcome" do
      json_body = '{"symbol":"AAPL","candles":[{"datetime":1000,"close":150.0}]}'
      response = make_response(
        status: 200,
        body: json_body,
        headers: { "content-type" => "application/json;charset=utf-8", "content-length" => json_body.bytesize.to_s }
      )
      allow(fake_session).to receive(:get).and_return(response)

      client.get_price_history("AAPL")

      metadata = Thread.current[:tickrake_last_response_metadata]
      expect(metadata).not_to be_nil
      expect(metadata[:http_status]).to eq(200)
      expect(metadata[:content_type]).to eq("application/json;charset=utf-8")
      expect(metadata[:content_length]).to eq(json_body.bytesize)
      expect(metadata[:body_bytes]).to eq(json_body.bytesize)
      expect(metadata[:duration_ms]).to be >= 0
      expect(metadata[:parse_outcome]).to eq(:success)

      log_text = logger_output.string
      expect(log_text).to include("type=application/json;charset=utf-8")
      expect(log_text).to include("bytes=#{json_body.bytesize}")
      expect(log_text).to include("parse=success")
    end

    it "preserves response metadata when JSON parsing fails on empty response body" do
      empty_response = make_response(status: 200, body: "", headers: { "content-length" => "0" })
      allow(fake_session).to receive(:get).and_return(empty_response)

      expect do
        client.get_option_chain("SPX")
      end.to raise_error(JSON::ParserError) do |error|
        expect(error.respond_to?(:response_metadata)).to eq(true)
        meta = error.response_metadata
        expect(meta[:http_status]).to eq(200)
        expect(meta[:body_bytes]).to eq(0)
        expect(meta[:parse_outcome]).to eq(:empty_body)
        expect(meta[:correlation_id]).not_to be_nil
        expect(error.correlation_id).to eq(meta[:correlation_id])
      end

      # Also verified via Thread context
      thread_meta = Thread.current[:tickrake_last_response_metadata]
      expect(thread_meta[:parse_outcome]).to eq(:empty_body)
    end

    it "preserves response metadata when JSON parsing fails on truncated/malformed response body" do
      truncated_body = '{"symbol":"SPX","underlyingPrice":5800.5,'
      response = make_response(status: 200, body: truncated_body)
      allow(fake_session).to receive(:get).and_return(response)

      expect do
        client.get_option_chain("SPX")
      end.to raise_error(JSON::ParserError) do |error|
        expect(error.respond_to?(:response_metadata)).to eq(true)
        meta = error.response_metadata
        expect(meta[:http_status]).to eq(200)
        expect(meta[:body_bytes]).to eq(truncated_body.bytesize)
        expect(meta[:parse_outcome]).to eq(:json_parser_error)
        expect(meta[:correlation_id]).not_to be_nil
      end
    end

    it "records transport and timeout failures with elapsed duration without HTTP response" do
      allow(fake_session).to receive(:get) do
        sleep 0.05
        raise Net::OpenTimeout, "execution expired"
      end

      expect do
        client.get_price_history("SPY")
      end.to raise_error(Net::OpenTimeout) do |error|
        expect(error.respond_to?(:response_metadata)).to eq(true)
        meta = error.response_metadata
        expect(meta[:http_status]).to be_nil
        expect(meta[:body_bytes]).to be_nil
        expect(meta[:parse_outcome]).to eq(:transport_error)
        expect(meta[:duration_ms]).to be >= 40
        expect(meta[:correlation_id]).not_to be_nil
        expect(error.correlation_id).to eq(meta[:correlation_id])
      end

      log_text = logger_output.string
      expect(log_text).to include("Error Net::OpenTimeout - execution expired")
      expect(log_text).to include("parse=transport_error")
    end

    it "triggers rate limiter cooldown on HTTP 429 and logs diagnostic" do
      limiter = instance_double(Tickrake::DB::SqliteRateLimiter, consume!: true, apply_cooldown!: 60.0)
      client.rate_limiter = limiter

      oauth_resp = make_response(status: 429, body: '{"message":"Too many requests"}', headers: { "retry-after" => "45" })
      oauth_err = OAuth2::Error.new(oauth_resp)
      allow(fake_session).to receive(:get).and_raise(oauth_err)

      expect do
        client.get_option_chain("SPX")
      end.to raise_error(OAuth2::Error)

      expect(limiter).to have_received(:apply_cooldown!).with("45")
      log_text = logger_output.string
      expect(log_text).to include("HTTP 429 Too Many Requests, applying cooldown")
      expect(log_text).to include("Retry-After: 45")
    end
  end

  describe "Sensitive data redaction" do
    it "does not log authorization headers, credentials, or full response bodies" do
      sensitive_body = '{"user_private_data":"example_payload_value","balance":1000000}'
      response = make_response(status: 200, body: sensitive_body)
      allow(fake_session).to receive(:get).and_return(response)

      client.get_price_history("SPY")

      output = logger_output.string
      expect(output).not_to include("user_private_data")
      expect(output).not_to include("example_payload_value")
      expect(output).not_to include("1000000")
      expect(output).not_to include("Bearer")
    end
  end

  describe "Propagation to OptionsJob fetch_retry and fetch_failed events" do
    let(:tracker) { instance_double(Tickrake::Tracker, adapter: nil) }
    let(:runtime_logger) { instance_double(Logger, info: nil, warn: nil, error: nil, :level= => nil) }
    let(:config) do
      Tickrake::ConfigLoader.load(File.expand_path("../config/tickrake.example.yml", __dir__))
    end

    it "includes correlation_id and response metadata in fetch_retry and fetch_failed events" do
      logged_warns = []
      logged_errors = []
      allow(runtime_logger).to receive(:warn) { |evt| logged_warns << evt }
      allow(runtime_logger).to receive(:error) { |evt| logged_errors << evt }

      truncated_resp = make_response(status: 200, body: '{"partial": true,')
      allow(fake_session).to receive(:get).and_return(truncated_resp)

      runtime = Tickrake::Runtime.new(
        config: config,
        tracker: tracker,
        client_factory: instance_double(Tickrake::ClientFactory, build: client),
        logger: runtime_logger
      )

      expiration_entry = Struct.new(:expiration_date, :days_to_expiration, :option_roots) do
        def date_object
          Date.iso8601(expiration_date)
        end
      end
      expiration_chain = instance_double(
        "SchwabRb::DataObjects::OptionExpirationChain",
        expiration_list: [expiration_entry.new("2026-04-06", 0, "SPXW")]
      )

      # Stub expiration chain to succeed, while get_option_chain fails with parse error
      allow(client).to receive(:get_option_expiration_chain).and_return(expiration_chain)

      job = Tickrake::OptionsJob.new(
        runtime,
        universe: [Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW")],
        expiration_date: Date.new(2026, 4, 6)
      )

      result = job.run(now: Time.utc(2026, 4, 6, 14, 30, 0))
      expect(result.failure_count).to eq(1)

      retry_events = logged_warns.select { |e| e.is_a?(Hash) && e[:event] == "fetch_retry" }
      expect(retry_events).not_to be_empty
      first_retry = retry_events.first
      expect(first_retry[:correlation_id]).to match(/options-#{Process.pid}-[0-9a-f]{8}-\d+/)
      expect(first_retry[:http_status]).to eq(200)
      expect(first_retry[:parse_outcome]).to eq(:json_parser_error)
      expect(first_retry[:body_bytes]).to eq(truncated_resp.body.bytesize)

      failed_events = logged_errors.select { |e| e.is_a?(Hash) && e[:event] == "fetch_failed" }
      expect(failed_events).not_to be_empty
      failed = failed_events.first
      expect(failed[:correlation_id]).to match(/options-#{Process.pid}-[0-9a-f]{8}-\d+/)
      expect(failed[:http_status]).to eq(200)
      expect(failed[:parse_outcome]).to eq(:json_parser_error)
      expect(failed[:body_bytes]).to eq(truncated_resp.body.bytesize)
    end
  end
end
