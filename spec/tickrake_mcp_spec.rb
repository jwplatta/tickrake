# frozen_string_literal: true

RSpec.describe Tickrake::MCPServer do
  describe ".new" do
    it "loads successfully" do
      expect { described_class.new }.not_to raise_error
    end
  end

  describe Tickrake::MCPTools::HelpTool do
    it "describes the initial tool set" do
      response = described_class.call(server_context: {})

      expect(response.content.first[:text]).to include("validate_config_tool")
      expect(response.content.first[:text]).to include("search_datasets_tool")
      expect(response.content.first[:text]).to include("restart_job_tool")
    end
  end

  describe Tickrake::MCPTools::ValidateConfigTool do
    it "loads the configured path and summarizes config" do
      config = instance_double(
        Tickrake::Config,
        default_provider_name: "schwab",
        providers: { "schwab" => {}, "ibkr-paper" => {} },
        data_dir: "/tmp/data",
        sqlite_path: "/tmp/tickrake.sqlite3"
      )

      allow(Tickrake::ConfigLoader).to receive(:load).with("/tmp/custom.yml").and_return(config)

      response = described_class.call(config_path: "/tmp/custom.yml", server_context: {})

      expect(response.content.first[:text]).to include("Config valid: /tmp/custom.yml")
      expect(response.content.first[:text]).to include("Providers: ibkr-paper, schwab")
    end
  end

  describe Tickrake::MCPTools::StatusTool do
    it "renders running and stopped jobs" do
      config = instance_double(Tickrake::Config, jobs: [double(name: "index_options"), double(name: "eod_candles")])
      registry = instance_double(Tickrake::JobRegistry)
      allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
      allow(Tickrake::JobRegistry).to receive(:new).and_return(registry)
      allow(registry).to receive(:registered_names).and_return([])
      allow(registry).to receive(:statuses).with(%w[eod_candles index_options]).and_return([
        { name: "index_options", state: "running", pid: 123, started_at: "2026-04-15T10:00:00Z", log_path: "/tmp/index_options.log" },
        { name: "eod_candles", state: "stopped" }
      ])

      response = described_class.call(server_context: {})

      expect(response.content.first[:text]).to include("index_options: running pid=123")
      expect(response.content.first[:text]).to include("eod_candles: stopped")
    end
  end

  describe Tickrake::MCPTools::SearchDatasetsTool do
    it "returns dataset metadata only" do
      config = instance_double(
        Tickrake::Config,
        sqlite_path: "/tmp/tickrake.sqlite3",
        providers: { "schwab" => {} }
      )
      tracker = instance_double(Tickrake::Tracker)
      candle_scanner = instance_double(Tickrake::Query::CandlesScanner)
      option_scanner = instance_double(Tickrake::Query::OptionsScanner)

      allow(Tickrake::ConfigLoader).to receive(:load).with(Tickrake::PathSupport.config_path).and_return(config)
      allow(Tickrake::Tracker).to receive(:new).with("/tmp/tickrake.sqlite3").and_return(tracker)
      allow(Tickrake::Query::CandlesScanner).to receive(:new).with(config: config, tracker: tracker).and_return(candle_scanner)
      allow(Tickrake::Query::OptionsScanner).to receive(:new).with(config: config, tracker: tracker).and_return(option_scanner)
      allow(candle_scanner).to receive(:scan).and_return([
        Tickrake::Query::CandlesScanner::Result.new(
          dataset_type: "candles",
          provider_name: "schwab",
          ticker: "SPY",
          frequency: "day",
          path: "/tmp/SPY_day.csv",
          row_count: 42,
          first_observed_at: "2026-04-01T00:00:00Z",
          last_observed_at: "2026-04-15T00:00:00Z",
          coverage: "all"
        )
      ])
      allow(option_scanner).to receive(:scan).and_return([])

      response = described_class.call(provider: "schwab", server_context: {})
      parsed = JSON.parse(response.content.first[:text])

      expect(parsed.fetch("filters")).not_to have_key("limit")
      expect(parsed.fetch("result_count")).to eq(1)
      expect(parsed.fetch("returned_count")).to eq(1)
      expect(parsed.fetch("results").first.fetch("dataset_type")).to eq("candles")
      expect(parsed.fetch("results").first.fetch("row_count")).to eq(42)
    end

    it "treats candle frequency all as no frequency filter" do
      config = instance_double(
        Tickrake::Config,
        sqlite_path: "/tmp/tickrake.sqlite3",
        providers: { "ibkr-paper" => {} }
      )
      tracker = instance_double(Tickrake::Tracker)
      candle_scanner = instance_double(Tickrake::Query::CandlesScanner)
      option_scanner = instance_double(Tickrake::Query::OptionsScanner)

      allow(Tickrake::ConfigLoader).to receive(:load).with(Tickrake::PathSupport.config_path).and_return(config)
      allow(Tickrake::Tracker).to receive(:new).with("/tmp/tickrake.sqlite3").and_return(tracker)
      allow(Tickrake::Query::CandlesScanner).to receive(:new).with(config: config, tracker: tracker).and_return(candle_scanner)
      allow(Tickrake::Query::OptionsScanner).to receive(:new).with(config: config, tracker: tracker).and_return(option_scanner)
      allow(candle_scanner).to receive(:scan).with(
        provider_name: "ibkr-paper",
        ticker: "SPX",
        frequency: nil,
        start_date: nil,
        end_date: nil
      ).and_return([])
      allow(option_scanner).to receive(:scan).and_return([])

      response = described_class.call(type: "candles", provider: "ibkr-paper", ticker: "SPX", frequency: "all", server_context: {})
      parsed = JSON.parse(response.content.first[:text])

      expect(parsed.fetch("filters").fetch("frequency")).to eq("all")
      expect(parsed.fetch("returned_count")).to eq(0)
    end

    it "returns only the 100 most recent option snapshots by default" do
      config = instance_double(
        Tickrake::Config,
        sqlite_path: "/tmp/tickrake.sqlite3",
        providers: { "schwab" => {} }
      )
      tracker = instance_double(Tickrake::Tracker)
      candle_scanner = instance_double(Tickrake::Query::CandlesScanner)
      option_scanner = instance_double(Tickrake::Query::OptionsScanner)
      option_results = 101.times.map do |index|
        Tickrake::Query::OptionsScanner::Result.new(
          dataset_type: "options",
          provider_name: "schwab",
          ticker: "SPX",
          root_symbol: "SPXW",
          expiration_date: "2026-04-17",
          sample_datetime: format("2026-04-%02dT14:30:00Z", (index % 28) + 1),
          file_path: "/tmp/SPXW_#{index}.csv"
        )
      end

      allow(Tickrake::ConfigLoader).to receive(:load).with(Tickrake::PathSupport.config_path).and_return(config)
      allow(Tickrake::Tracker).to receive(:new).with("/tmp/tickrake.sqlite3").and_return(tracker)
      allow(Tickrake::Query::CandlesScanner).to receive(:new).with(config: config, tracker: tracker).and_return(candle_scanner)
      allow(Tickrake::Query::OptionsScanner).to receive(:new).with(config: config, tracker: tracker).and_return(option_scanner)
      allow(candle_scanner).to receive(:scan).and_return([])
      allow(option_scanner).to receive(:scan).and_return(option_results)

      response = described_class.call(type: "options", provider: "schwab", server_context: {})

      parsed = JSON.parse(response.content.first[:text])
      expect(parsed.fetch("filters").fetch("limit")).to eq(100)
      expect(parsed.fetch("returned_count")).to eq(100)
      expect(parsed.fetch("result_count")).to eq(101)
      expect(parsed.fetch("results").length).to eq(100)
    end
  end

  describe Tickrake::MCPTools::StorageStatsTool do
    it "renders stats using the loaded config" do
      config = instance_double(Tickrake::Config)
      report = instance_double(Tickrake::Storage::StatsReport, render: "storage output")

      allow(Tickrake::ConfigLoader).to receive(:load).with(Tickrake::PathSupport.config_path).and_return(config)
      allow(Tickrake::Storage::StatsReport).to receive(:new).with(config).and_return(report)

      response = described_class.call(server_context: {})

      expect(response.content.first[:text]).to eq("storage output")
    end
  end

  describe Tickrake::MCPTools::LogsTool do
    it "tails the requested log file" do
      Dir.mktmpdir do |dir|
        log_path = File.join(dir, "cli.log")
        File.write(log_path, "one\ntwo\nthree\n")

        allow(Tickrake::PathSupport).to receive(:named_log_path).with("cli").and_return(log_path)

        response = described_class.call(target: "cli", tail: 2, server_context: {})

        expect(response.content.first[:text]).to eq("two\nthree\n")
      end
    end

    it "defaults to the last 10 log lines" do
      Dir.mktmpdir do |dir|
        log_path = File.join(dir, "cli.log")
        File.write(log_path, (1..12).map { |index| "line#{index}" }.join("\n") + "\n")

        allow(Tickrake::PathSupport).to receive(:named_log_path).with("cli").and_return(log_path)

        response = described_class.call(target: "cli", server_context: {})

        expect(response.content.first[:text]).to eq((3..12).map { |index| "line#{index}" }.join("\n") + "\n")
      end
    end
  end

  describe Tickrake::MCPTools::StartJobTool do
    it "starts the requested job" do
      controller = instance_double(Tickrake::JobControl)
      allow(Tickrake::JobControl).to receive(:new) do |stdout:|
        stdout.puts("Started index_options job with pid 123.")
        controller
      end
      allow(controller).to receive(:start)

      response = described_class.call(target: "index_options", server_context: {})

      expect(response.content.first[:text]).to include("Started index_options job")
    end
  end

  describe Tickrake::MCPTools::StopJobTool do
    it "stops the requested job" do
      controller = instance_double(Tickrake::JobControl)
      allow(Tickrake::JobControl).to receive(:new) do |stdout:|
        stdout.puts("Stopped index_options job (pid 123).")
        controller
      end
      allow(controller).to receive(:stop)

      response = described_class.call(target: "index_options", server_context: {})

      expect(response.content.first[:text]).to include("Stopped index_options job")
    end
  end

  describe Tickrake::MCPTools::RestartJobTool do
    it "restarts the requested job" do
      controller = instance_double(Tickrake::JobControl)
      allow(Tickrake::JobControl).to receive(:new) do |stdout:|
        stdout.puts("Waiting for eod_candles job to finish its current work before restarting. This can take a bit.")
        stdout.puts("Started eod_candles job with pid 456.")
        controller
      end
      allow(controller).to receive(:restart)

      response = described_class.call(target: "eod_candles", server_context: {})

      expect(response.content.first[:text]).to include("Started eod_candles job")
    end
  end
end
