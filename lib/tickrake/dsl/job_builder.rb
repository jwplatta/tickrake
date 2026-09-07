# frozen_string_literal: true

module Tickrake
  module DSL
    class JobBuilder
      def initialize(name)
        @name = name
        @provider = nil
        @type = nil
        @universe_name = nil
        @universe_builder = nil
        @inline_symbols = []
        @lookback_days = nil
        @schedule_builder = nil
        @options_builder = nil
        @candles_builder = nil
        @maintenance_builder = nil
      end

      def provider(name)
        @provider = name.to_s
      end

      def type(t)
        @type = t.to_s
      end

      def universe(name = nil, &block)
        if block
          @universe_builder = UniverseBuilder.new
          @universe_builder.instance_eval(&block)
        else
          @universe_name = name.to_s
        end
      end

      def symbols(*args)
        @inline_symbols = args.flatten.map(&:to_s)
      end

      def lookback(duration)
        @lookback_days = duration.to_lookback_days
      end

      def schedule(&block)
        @schedule_builder = ScheduleBuilder.new
        @schedule_builder.instance_eval(&block)
      end

      def options(&block)
        @options_builder = OptionsBuilder.new
        @options_builder.instance_eval(&block)
      end

      def candles(&block)
        @candles_builder = CandlesBuilder.new
        @candles_builder.instance_eval(&block)
      end

      def maintenance(&block)
        @maintenance_builder = MaintenanceBuilder.new
        @maintenance_builder.instance_eval(&block)
      end

      def build!(config)
        raise Tickrake::Error, "job `#{@name}` requires type" if @type.nil?
        raise Tickrake::Error, "job `#{@name}` requires provider" if @provider.nil?
        raise Tickrake::Error, "job `#{@name}` requires a schedule block" if @schedule_builder.nil?

        schedule = @schedule_builder.build!

        case @type
        when "options"     then build_options_job!(config, schedule)
        when "candles"     then build_candles_job!(config, schedule)
        when "maintenance" then build_maintenance_job!(schedule)
        else raise Tickrake::Error, "job `#{@name}` has unknown type: #{@type.inspect}"
        end
      end

      private

      def build_options_job!(config, schedule)
        has_universe = @universe_name || @universe_builder
        raise Tickrake::Error, "options job `#{@name}` requires universe" unless has_universe
        raise Tickrake::Error, "options job `#{@name}` requires an options block" if @options_builder.nil?

        entries = if @universe_builder
                    @universe_builder.entries
                  else
                    config.universe(@universe_name).entries
                  end

        universe = expand_option_entries(entries)
        opts     = @options_builder.build!

        Tickrake::ScheduledJobConfig.new(
          name: @name,
          type: "options",
          provider: @provider,
          interval_seconds: schedule[:interval_seconds],
          windows: schedule[:windows],
          run_at: schedule[:run_at],
          days: schedule[:days],
          lookback_days: nil,
          dte_buckets: opts[:dte_buckets],
          universe: universe,
          tasks: [],
          task: nil,
          settings: {},
          manual: false
        )
      end

      def build_candles_job!(config, schedule)
        candle_settings = @candles_builder&.build! || {}
        frequencies = candle_settings[:frequencies] || []
        start_date  = candle_settings[:start_date]

        symbol_list = if @universe_name
                        config.universe(@universe_name).entries.map(&:symbol)
                      else
                        @inline_symbols
                      end

        raise Tickrake::Error, "candles job `#{@name}` requires symbols or universe" if symbol_list.empty?

        universe = symbol_list.map do |symbol|
          Tickrake::CandleSymbol.new(
            symbol: symbol,
            provider: nil,
            frequencies: frequencies,
            start_date: start_date,
            need_extended_hours_data: false,
            need_previous_close: false
          )
        end

        Tickrake::ScheduledJobConfig.new(
          name: @name,
          type: "candles",
          provider: @provider,
          interval_seconds: schedule[:interval_seconds],
          windows: schedule[:windows],
          run_at: schedule[:run_at],
          days: schedule[:days],
          lookback_days: @lookback_days,
          dte_buckets: [],
          universe: universe,
          tasks: [],
          task: nil,
          settings: {},
          manual: false
        )
      end

      def build_maintenance_job!(schedule)
        raise Tickrake::Error, "maintenance job `#{@name}` requires a maintenance block" if @maintenance_builder.nil?

        Tickrake::ScheduledJobConfig.new(
          name: @name,
          type: "maintenance",
          provider: @provider,
          interval_seconds: schedule[:interval_seconds],
          windows: schedule[:windows],
          run_at: schedule[:run_at],
          days: schedule[:days],
          lookback_days: nil,
          dte_buckets: [],
          universe: [],
          tasks: @maintenance_builder.build!,
          task: nil,
          settings: {},
          manual: false
        )
      end

      def expand_option_entries(entries)
        entries.flat_map do |entry|
          roots = [*Array(entry.option_roots).map(&:to_s)]
          roots << entry.option_root.to_s unless entry.option_root.to_s.empty?
          roots = roots.reject(&:empty?).uniq
          roots = [nil] if roots.empty?
          roots.map { |root| Tickrake::OptionSymbol.new(symbol: entry.symbol, option_root: root, provider: nil) }
        end
      end
    end
  end
end
