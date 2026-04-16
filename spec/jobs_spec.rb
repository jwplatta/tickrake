# frozen_string_literal: true

RSpec.describe "job execution" do
  let(:config) do
    Tickrake::ConfigLoader.load(File.expand_path("../config/tickrake.example.yml", __dir__))
  end
  let(:tracker) { Tickrake::Tracker.new(File.join(Dir.mktmpdir, "tickrake.sqlite3")) }
  let(:logger) { Logger.new(nil) }

  def config_with(config, **overrides)
    attrs = config.instance_variables.each_with_object({}) do |ivar, hash|
      hash[ivar.to_s.delete("@").to_sym] = config.instance_variable_get(ivar)
    end
    Tickrake::Config.new(**attrs.merge(overrides))
  end

  it "writes option samples and tracks metadata" do
    Dir.mktmpdir do |dir|
      custom = config_with(config, options_dir: dir)
      client = instance_double("client")
      expiration_entry = Struct.new(:expiration_date, :days_to_expiration, :option_roots) do
        def date_object
          Date.iso8601(expiration_date)
        end
      end
      option = Struct.new(
        :put_call, :symbol, :description, :strike, :expiration_date, :mark, :bid, :bid_size, :ask, :ask_size,
        :last, :last_size, :open_interest, :total_volume, :delta, :gamma, :theta, :vega, :rho, :volatility,
        :theoretical_volatility, :theoretical_option_value, :intrinsic_value, :extrinsic_value, :option_root,
        keyword_init: true
      )
      expiration_chain = instance_double(
        "SchwabRb::DataObjects::OptionExpirationChain",
        expiration_list: [
          expiration_entry.new("2026-04-06", 0, "SPXW"),
          expiration_entry.new("2026-04-07", 1, "SPXW")
        ]
      )
      allow(client).to receive(:get_option_expiration_chain).and_return(expiration_chain)
      allow(client).to receive(:get_option_chain).and_return(
        instance_double(
          "SchwabRb::DataObjects::OptionChain",
          underlying_price: 5100.5,
          call_opts: [
            option.new(
              put_call: "CALL",
              symbol: "SPXW  260406C05100000",
              description: "SPXW call",
              strike: 5100.0,
              expiration_date: Date.new(2026, 4, 6),
              mark: 12.5,
              bid: 12.0,
              bid_size: 10,
              ask: 13.0,
              ask_size: 12,
              last: 12.4,
              last_size: 3,
              open_interest: 100,
              total_volume: 50,
              delta: 0.5,
              gamma: 0.1,
              theta: -0.2,
              vega: 0.3,
              rho: 0.05,
              volatility: 0.22,
              theoretical_volatility: 0.21,
              theoretical_option_value: 12.3,
              intrinsic_value: 1.0,
              extrinsic_value: 11.3,
              option_root: "SPXW"
            )
          ],
          put_opts: []
        )
      )
      client_factory = instance_double(Tickrake::ClientFactory, build: client)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: client_factory, logger: logger)

      Tickrake::OptionsJob.new(runtime).run(now: Time.utc(2026, 4, 6, 14, 30, 0))

      expected_path = File.join(dir, "schwab", "SPXW_exp2026-04-06_2026-04-06_14-30-00.csv")
      expect(File.exist?(expected_path)).to eq(true)
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
      expect(tracker.fetch_runs.map { |row| row["output_path"] }).to all(end_with(".csv"))
      expect(client).to have_received(:get_option_expiration_chain).with("$SPX").at_least(:once)
      expect(client).to have_received(:get_option_chain).with(
        "$SPX",
        hash_including(
          contract_type: SchwabRb::Option::ContractTypes::ALL,
          strike_range: SchwabRb::Option::StrikeRanges::ALL,
          from_date: Date.new(2026, 4, 6),
          to_date: Date.new(2026, 4, 6)
        )
      ).at_least(:once)
    end
  end

  it "rejects options collection for non-schwab providers" do
    custom = config_with(
      config,
      providers: { "ibkr" => Tickrake::ProviderDefinition.new(name: "ibkr", adapter: "ibkr", settings: { "host" => "127.0.0.1" }) },
      default_provider_name: "ibkr"
    )
    runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, logger: logger)

    expect do
      Tickrake::OptionsJob.new(runtime).run(now: Time.utc(2026, 4, 6, 14, 30, 0))
    end.to raise_error(Tickrake::Error, /provider=schwab only/)
  end

  it "downloads candles through schwab_rb's shared history downloader" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "SPY",
        frequencies: %w[day 1min 5min],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(config, history_dir: dir, candles_universe: [candle_entry])
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab", adapter_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger, provider_name: "schwab")
      rows = {
        "day" => [Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 21, 0, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)],
        "1min" => [Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 14, 0, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)],
        "5min" => [Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 14, 5, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)]
      }
      allow(provider).to receive(:fetch_bars) do |**kwargs|
        rows.fetch(kwargs.fetch(:frequency))
      end

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(File.exist?(File.join(dir, "schwab", "SPY_day.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "schwab", "SPY_1min.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "schwab", "SPY_5min.csv"))).to eq(true)
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
      expect(tracker.fetch_runs.map { |row| row["frequency"] }).to include("day", "1min", "5min")
      expect(provider).to have_received(:fetch_bars).with(
        hash_including(
          symbol: "SPY",
          frequency: "day",
          start_date: Date.new(2020, 1, 1),
          end_date: Date.new(2026, 4, 7)
        )
      )
    end
  end

  it "writes mapped futures candles under the canonical symbol while fetching with the provider symbol" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "/ES",
        frequencies: ["1min"],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      providers = config.providers.merge(
        "schwab" => Tickrake::ProviderDefinition.new(
          name: "schwab",
          adapter: "schwab",
          settings: {},
          symbol_map: { "/ES" => "^ES", "/NQ" => "^NQ", "/RTY" => "^RTY" }
        )
      )
      custom = config_with(config, history_dir: dir, candles_universe: [candle_entry], providers: providers)
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab", adapter_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger, provider_name: "schwab")
      allow(provider).to receive(:fetch_bars).and_return([
        Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 14, 0, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)
      ])

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(File.exist?(File.join(dir, "schwab", "^ES_1min.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "schwab", "ES_1min.csv"))).to eq(false)
      expect(tracker.fetch_runs.last["symbol"]).to eq("^ES")
      expect(provider).to have_received(:fetch_bars).with(hash_including(symbol: "/ES", frequency: "1min"))
    end
  end

  it "skips buckets that are not present in the expiration chain" do
    Dir.mktmpdir do |dir|
      custom = config_with(
        config,
        options_dir: dir,
        dte_buckets: [0, 2],
        options_universe: [Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW")]
      )
      client = instance_double("client")
      expiration_entry = Struct.new(:expiration_date, :days_to_expiration, :option_roots) do
        def date_object
          Date.iso8601(expiration_date)
        end
      end
      expiration_chain = instance_double(
        "SchwabRb::DataObjects::OptionExpirationChain",
        expiration_list: [expiration_entry.new("2026-04-06", 0, "SPXW")]
      )
      allow(client).to receive(:get_option_expiration_chain).with("$SPX").and_return(expiration_chain)
      allow(client).to receive(:get_option_chain).and_return(
        instance_double("SchwabRb::DataObjects::OptionChain", underlying_price: 5100.5, call_opts: [], put_opts: [])
      )

      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: instance_double(Tickrake::ClientFactory, build: client), logger: logger)

      Tickrake::OptionsJob.new(runtime).run(now: Time.utc(2026, 4, 6, 14, 30, 0))

      expect(client).to have_received(:get_option_chain).once.with(
        anything,
        hash_including(from_date: Date.new(2026, 4, 6), to_date: Date.new(2026, 4, 6))
      )
    end
  end

  it "uses an explicit expiration date for direct option runs" do
    Dir.mktmpdir do |dir|
      custom = config_with(
        config,
        options_dir: dir,
        options_universe: [Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW")]
      )
      client = instance_double("client")
      allow(client).to receive(:get_option_expiration_chain)
      allow(client).to receive(:get_option_chain).and_return(
        instance_double("SchwabRb::DataObjects::OptionChain", underlying_price: 5100.5, call_opts: [], put_opts: [])
      )
      runtime = Tickrake::Runtime.new(
        config: custom,
        tracker: tracker,
        client_factory: instance_double(Tickrake::ClientFactory, build: client),
        logger: logger
      )

      Tickrake::OptionsJob.new(
        runtime,
        universe: [Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW")],
        expiration_date: Date.new(2026, 4, 11)
      ).run(now: Time.utc(2026, 4, 6, 14, 30, 0))

      expect(client).not_to have_received(:get_option_expiration_chain)
      expect(client).to have_received(:get_option_chain).with(
        "$SPX",
        hash_including(from_date: Date.new(2026, 4, 11), to_date: Date.new(2026, 4, 11))
      )
    end
  end

  it "advances the option progress reporter for one-off runs" do
    Dir.mktmpdir do |dir|
      custom = config_with(
        config,
        options_dir: dir,
        options_universe: [Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW")]
      )
      client = instance_double("client")
      progress_reporter = instance_double(Tickrake::ProgressReporter, advance: true, finish: true)
      allow(client).to receive(:get_option_chain).and_return(
        instance_double("SchwabRb::DataObjects::OptionChain", underlying_price: 5100.5, call_opts: [], put_opts: [])
      )
      runtime = Tickrake::Runtime.new(
        config: custom,
        tracker: tracker,
        client_factory: instance_double(Tickrake::ClientFactory, build: client),
        logger: logger
      )

      Tickrake::OptionsJob.new(
        runtime,
        universe: [Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW")],
        expiration_date: Date.new(2026, 4, 11),
        progress_reporter: progress_reporter
      ).run(now: Time.utc(2026, 4, 6, 14, 30, 0))

      expect(progress_reporter).to have_received(:advance).with(title: "$SPX SPXW 2026-04-11")
      expect(progress_reporter).to have_received(:finish)
    end
  end

  it "uses the configured lookback window when a candle file already exists" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "SPY",
        frequencies: ["day"],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(config, history_dir: dir, candles_universe: [candle_entry], candle_lookback_days: 3)
      existing_dir = File.join(dir, "schwab")
      FileUtils.mkdir_p(existing_dir)
      existing_path = File.join(existing_dir, "SPY_day.csv")
      File.write(existing_path, "datetime,open,high,low,close,volume\n")
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab", adapter_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger, provider_name: "schwab")
      allow(provider).to receive(:fetch_bars).and_return([])

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 4, 3), end_date: Date.new(2026, 4, 7))
      )
    end
  end

  it "uses explicit candle date overrides for direct runs even when a file already exists" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "SPY",
        frequencies: ["day"],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(config, history_dir: dir, candles_universe: [candle_entry], candle_lookback_days: 3)
      existing_dir = File.join(dir, "schwab")
      FileUtils.mkdir_p(existing_dir)
      existing_path = File.join(existing_dir, "SPY_day.csv")
      File.write(existing_path, "datetime,open,high,low,close,volume\n")
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab", adapter_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger, provider_name: "schwab")
      allow(provider).to receive(:fetch_bars).and_return([])

      Tickrake::CandlesJob.new(
        runtime,
        universe: [candle_entry],
        start_date_override: Date.new(2026, 4, 1),
        end_date_override: Date.new(2026, 4, 6)
      ).run(now: Time.utc(2026, 4, 10, 21, 10, 0))

      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 4, 1), end_date: Date.new(2026, 4, 7), frequency: "day")
      )
    end
  end

  it "uses the configured lookback window for first-time ibkr intraday fetches" do
    candle_entry = Tickrake::CandleSymbol.new(
      symbol: "$SPX",
      frequencies: ["30min"],
      start_date: Date.iso8601("2020-01-01"),
      need_extended_hours_data: false,
      need_previous_close: false
    )
      custom = config_with(
        config,
        providers: { "ib_paper" => Tickrake::ProviderDefinition.new(name: "ib_paper", adapter: "ibkr", settings: { "host" => "127.0.0.1" }) },
        default_provider_name: "ib_paper",
        candles_universe: [candle_entry],
        candle_lookback_days: 3
      )
    provider = instance_double(Tickrake::Providers::Ibkr, provider_name: "ib_paper", adapter_name: "ibkr")
    provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
    runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger, provider_name: "ib_paper")
    allow(provider).to receive(:fetch_bars).and_return([])

    Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

    expect(provider).to have_received(:fetch_bars).with(
      hash_including(start_date: Date.new(2026, 4, 3), end_date: Date.new(2026, 4, 7), frequency: "30min")
    )
  end

  it "splits ibkr full backfills into smaller candle requests" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "$SPX",
        frequencies: ["30min"],
        start_date: Date.iso8601("2026-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(
        config,
        providers: { "ib_paper" => Tickrake::ProviderDefinition.new(name: "ib_paper", adapter: "ibkr", settings: { "host" => "127.0.0.1" }) },
        default_provider_name: "ib_paper",
        history_dir: dir,
        candles_universe: [candle_entry]
      )
      provider = instance_double(Tickrake::Providers::Ibkr, provider_name: "ib_paper", adapter_name: "ibkr")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger, provider_name: "ib_paper")
      allow(provider).to receive(:fetch_bars).and_return([])

      Tickrake::CandlesJob.new(runtime, from_config_start: true).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 1, 1), end_date: Date.new(2026, 2, 3), frequency: "30min")
      )
      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 2, 4), end_date: Date.new(2026, 3, 9), frequency: "30min")
      )
      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 3, 10), end_date: Date.new(2026, 4, 7), frequency: "30min")
      )
    end
  end

  it "writes candles under the selected provider name when multiple providers share an adapter" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "SPY",
        frequencies: ["day"],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(
        config,
        history_dir: dir,
        providers: {
          "schwab_live" => Tickrake::ProviderDefinition.new(name: "schwab_live", adapter: "schwab", settings: {}),
          "schwab_paper" => Tickrake::ProviderDefinition.new(name: "schwab_paper", adapter: "schwab", settings: {})
        },
        default_provider_name: "schwab_live",
        candles_universe: [candle_entry]
      )
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab_paper", adapter_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(
        config: custom,
        tracker: tracker,
        provider_factory: provider_factory,
        logger: logger,
        provider_name: "schwab_paper"
      )
      allow(provider).to receive(:fetch_bars).and_return([
        Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 21, 0, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)
      ])

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(File.exist?(File.join(dir, "schwab_paper", "SPY_day.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "schwab_live", "SPY_day.csv"))).to eq(false)
    end
  end

  it "advances the candle progress reporter for one-off runs" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "SPY",
        frequencies: ["day"],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(config, history_dir: dir, candles_universe: [candle_entry])
      progress_reporter = instance_double(Tickrake::ProgressReporter, advance: true, finish: true)
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab", adapter_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(
        config: custom,
        tracker: tracker,
        provider_factory: provider_factory,
        logger: logger,
        provider_name: "schwab"
      )
      allow(provider).to receive(:fetch_bars).and_return([])
      allow(Tickrake::ProgressReporter).to receive(:build).with(total: 1, title: "SPY day", output: anything).and_return(progress_reporter)

      Tickrake::CandlesJob.new(runtime, progress_output: StringIO.new).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(progress_reporter).to have_received(:advance).with(title: "SPY day")
      expect(progress_reporter).to have_received(:finish)
    end
  end

  it "advances candle progress per ibkr chunk for one-off runs" do
    candle_entry = Tickrake::CandleSymbol.new(
      symbol: "$SPX",
      frequencies: ["30min"],
      start_date: Date.iso8601("2026-01-01"),
      need_extended_hours_data: false,
      need_previous_close: false
    )
    custom = config_with(
      config,
      providers: { "ib_paper" => Tickrake::ProviderDefinition.new(name: "ib_paper", adapter: "ibkr", settings: { "host" => "127.0.0.1" }) },
      default_provider_name: "ib_paper",
      candles_universe: [candle_entry]
    )
    progress_reporter = instance_double(Tickrake::ProgressReporter, advance: true, finish: true)
    provider = instance_double(Tickrake::Providers::Ibkr, provider_name: "ib_paper", adapter_name: "ibkr")
    provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
    runtime = Tickrake::Runtime.new(
      config: custom,
      tracker: tracker,
      provider_factory: provider_factory,
      logger: logger,
      provider_name: "ib_paper"
    )
    allow(provider).to receive(:fetch_bars).and_return([])
    allow(Tickrake::ProgressReporter).to receive(:build).with(total: 3, title: "$SPX 30min chunk 1/3", output: anything).and_return(progress_reporter)

    Tickrake::CandlesJob.new(
      runtime,
      from_config_start: true,
      progress_output: StringIO.new
    ).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

    expect(progress_reporter).to have_received(:advance).with(title: "$SPX 30min chunk 1/3")
    expect(progress_reporter).to have_received(:advance).with(title: "$SPX 30min chunk 2/3")
    expect(progress_reporter).to have_received(:advance).with(title: "$SPX 30min chunk 3/3")
    expect(progress_reporter).to have_received(:finish)
  end
end
