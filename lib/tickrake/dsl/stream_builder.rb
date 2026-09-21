# frozen_string_literal: true

module Tickrake
  module DSL
    class StreamSubscriptionBuilder
      attr_reader :name, :kind

      def initialize(name, kind:)
        @name = name.to_s
        @kind = kind
        @symbols = []
        @services = []
        @rotation_interval_seconds = nil
        @flush_interval_seconds = nil
        @schedule_builder = nil
      end

      def symbols(*args)
        @symbols = args.flatten.map(&:to_s)
      end

      def services(list)
        @services = Array(list)
      end

      def rotation_interval(seconds)
        @rotation_interval_seconds = Integer(seconds)
      end

      def flush_interval(seconds)
        @flush_interval_seconds = Integer(seconds)
      end

      def schedule(&block)
        @schedule_builder = ScheduleBuilder.new
        @schedule_builder.instance_eval(&block)
      end

      def build!(parent_job_name)
        raise Tickrake::Error, "stream subscription `#{@name}` requires symbols" if @symbols.empty?
        raise Tickrake::Error, "stream subscription `#{@name}` requires services" if @services.empty?

        schedule = @schedule_builder&.build! || {}
        windows = schedule[:windows] || []

        settings = case @kind
                   when :level_one
                     symbols_sym = @services.map(&:to_sym)
                     unknown = symbols_sym.reject { |s| SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.key?(s) }
                     raise Tickrake::Error, "Unknown level_one services in `#{@name}`: #{unknown.join(", ")}" unless unknown.empty?

                     Tickrake::LevelOneConfig.new(
                       services: symbols_sym,
                       rotation_interval_seconds: @rotation_interval_seconds || 300
                     )
                   when :order_book
                     normalized = @services.map { |s| s.to_s.upcase }
                     all_valid = (OrderBookBuilder::EQUITY_SERVICES + OrderBookBuilder::OPTIONS_SERVICES)
                     unknown = normalized - all_valid
                     raise Tickrake::Error, "Unknown order_book services in `#{@name}`: #{unknown.join(", ")}" unless unknown.empty?

                     Tickrake::OrderBookConfig.new(
                       services: normalized,
                       rotation_interval_seconds: @rotation_interval_seconds || 300,
                       contracts: nil
                     )
                   when :chart_stream
                     symbols_sym = @services.map(&:to_sym)
                     unknown = symbols_sym.reject { |s| SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.key?(s) }
                     raise Tickrake::Error, "Unknown chart_stream services in `#{@name}`: #{unknown.join(", ")}" unless unknown.empty?

                     Tickrake::ChartStreamConfig.new(
                       services: symbols_sym,
                       flush_interval_seconds: @flush_interval_seconds || 60
                     )
                   else
                     raise Tickrake::Error, "Unknown subscription kind: #{@kind}"
                   end

        Tickrake::StreamSubscription.new(
          name: @name,
          kind: @kind,
          symbols: @symbols,
          settings: settings,
          windows: windows
        )
      end
    end

    class StreamBuilder
      DEFAULT_STALE_TIMEOUT = 60

      def initialize
        @stale_timeout_seconds = DEFAULT_STALE_TIMEOUT
        @subscription_builders = []
      end

      def stale_timeout(seconds)
        @stale_timeout_seconds = Integer(seconds)
      end

      def level_one(name, &block)
        builder = StreamSubscriptionBuilder.new(name, kind: :level_one)
        builder.instance_eval(&block)
        @subscription_builders << builder
      end

      def order_book(name, &block)
        builder = StreamSubscriptionBuilder.new(name, kind: :order_book)
        builder.instance_eval(&block)
        @subscription_builders << builder
      end

      def chart_stream(name, &block)
        builder = StreamSubscriptionBuilder.new(name, kind: :chart_stream)
        builder.instance_eval(&block)
        @subscription_builders << builder
      end

      def build!(job_name:)
        raise Tickrake::Error, "stream job `#{job_name}` requires at least one subscription block (e.g. level_one, order_book, chart_stream)" if @subscription_builders.empty?

        subscriptions = @subscription_builders.map { |sb| sb.build!(job_name) }

        Tickrake::StreamConfig.new(
          subscriptions: subscriptions,
          stale_timeout_seconds: @stale_timeout_seconds
        )
      end
    end
  end
end
