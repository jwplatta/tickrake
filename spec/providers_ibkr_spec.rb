# frozen_string_literal: true

RSpec.describe Tickrake::Providers::Ibkr do
  it "normalizes chunked historical responses into Tickrake bars" do
    fake_historical_class = Class.new
    fake_request_class = Class.new do
      attr_reader :data

      def initialize(data)
        @data = data
      end
    end
    fake_outgoing = Module.new
    fake_outgoing.const_set("RequestHistoricalData", fake_request_class)
    fake_incoming = Module.new
    fake_incoming.const_set("HistoricalData", fake_historical_class)
    fake_messages = Module.new
    fake_messages.const_set("Incoming", fake_incoming)
    fake_messages.const_set("Outgoing", fake_outgoing)
    fake_ib = Module.new
    fake_ib.const_set("Messages", fake_messages)
    fake_ib.const_set("Index", Class.new do
      attr_reader :symbol, :exchange, :currency

      def initialize(symbol:, exchange:, currency:)
        @symbol = symbol
        @exchange = exchange
        @currency = currency
      end
    end)
    fake_ib.const_set("Stock", Class.new do
      attr_reader :symbol, :exchange, :currency

      def initialize(symbol:, exchange:, currency:)
        @symbol = symbol
        @exchange = exchange
        @currency = currency
      end
    end)
    stub_const("IB", fake_ib)

    bar_one = Struct.new(:time, :open, :high, :low, :close, :volume).new(Time.utc(2026, 4, 1, 14, 0, 0), 1.0, 2.0, 0.5, 1.5, 10)
    bar_two = Struct.new(:time, :open, :high, :low, :close, :volume).new(Time.utc(2026, 4, 7, 14, 0, 0), 2.0, 3.0, 1.5, 2.5, 20)
    messages = Queue.new
    messages << Struct.new(:request_id, :results).new(1, [bar_one])
    messages << Struct.new(:request_id, :results).new(2, [bar_two])

    connection = instance_double("IB::Connection", disconnect: true)
    allow(connection).to receive(:subscribe).with(fake_historical_class).and_yield.and_return(:subscription)
    allow(connection).to receive(:send_message) do |request|
      request_id = request.data.fetch(:request_id)
      message = messages.pop
      fake_handler = nil
      allow(connection).to receive(:subscribe).with(fake_historical_class) do |&block|
        fake_handler = block
        :subscription
      end
      fake_handler&.call(message) if message.request_id == request_id
    end
    allow(connection).to receive(:unsubscribe).with(:subscription)

    queued_messages = []
    allow(connection).to receive(:subscribe).with(fake_historical_class) do |&block|
      queued_messages << block
      :subscription
    end
    allow(connection).to receive(:send_message) do |request|
      message = messages.pop
      queued_messages.shift.call(message)
    end

    provider = described_class.new(
      settings: { "historical_timeout_seconds" => 1 },
      connection_builder: ->(**) { connection }
    )

    bars = provider.fetch_bars(
      symbol: "$SPX",
      frequency: "1min",
      start_date: Date.new(2026, 4, 1),
      end_date: Date.new(2026, 4, 7),
      extended_hours: false,
      previous_close: false
    )

    expect(bars.map(&:datetime)).to eq([Time.utc(2026, 4, 1, 14, 0, 0), Time.utc(2026, 4, 7, 14, 0, 0)])
    expect(connection).to have_received(:send_message).twice
  end
end
