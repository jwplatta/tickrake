# frozen_string_literal: true

RSpec.describe Tickrake::Providers::Ibkr do
  let(:settings) { { "historical_timeout_seconds" => 1 } }
  let(:connection) { instance_double("IB::Connection", disconnect: true) }
  let(:provider) do
    described_class.new(
      provider_name: "ib_paper",
      settings: settings,
      connection_builder: ->(**) { connection }
    )
  end

  before do
    # Mock the IB namespace and its classes
    fake_historical_data = Class.new do
      attr_reader :request_id, :results
      def initialize(request_id:, results:)
        @request_id = request_id
        @results = results
      end
    end
    fake_alert = Class.new do
      attr_reader :error_id, :code, :message
      def initialize(error_id:, code:, message:)
        @error_id = error_id
        @code = code
        @message = message
      end
    end
    fake_request = Class.new do
      attr_reader :request_id, :contract
      def initialize(request_id:, contract:, **options)
        @request_id = request_id
        @contract = contract
      end
    end
    
    fake_incoming = Module.new
    fake_incoming.const_set("HistoricalData", fake_historical_data)
    fake_incoming.const_set("Alert", fake_alert)
    
    fake_outgoing = Module.new
    fake_outgoing.const_set("RequestHistoricalData", fake_request)
    
    fake_messages = Module.new
    fake_messages.const_set("Incoming", fake_incoming)
    fake_messages.const_set("Outgoing", fake_outgoing)
    
    fake_ib = Module.new
    fake_ib.const_set("Messages", fake_messages)
    fake_ib.const_set("Index", Struct.new(:symbol, :exchange, :currency, keyword_init: true))
    fake_ib.const_set("Stock", Struct.new(:symbol, :exchange, :currency, keyword_init: true))
    fake_ib.const_set("Connection", Class.new do
      def disconnect; end
      def subscribe(*args); end
      def send_message(*args); end
      def unsubscribe(*args); end
    end)
    
    stub_const("IB", fake_ib)
  end

  describe "#build_contract" do
    it "converts periods to spaces for stock tickers" do
      contract = provider.send(:build_contract, "BRK.B")
      expect(contract.symbol).to eq("BRK B")
    end

    it "handles indices correctly" do
      contract = provider.send(:build_contract, "$SPX")
      expect(contract.symbol).to eq("SPX")
      expect(contract.exchange).to eq("CBOE")
    end

    it "strips $ prefix and upcases symbols" do
      contract = provider.send(:build_contract, "spy")
      expect(contract.symbol).to eq("SPY")
    end
  end

  describe "#fetch_bars" do
    let(:bar_one) do
      Struct.new(:time, :open, :high, :low, :close, :volume).new(
        Time.utc(2026, 4, 1, 14, 0, 0), 100.0, 101.0, 99.0, 100.5, 1000
      )
    end

    it "returns bars when historical data is received" do
      allow(connection).to receive(:subscribe).with(:HistoricalData, :Alert) do |*args, &block|
        # Simulate receiving HistoricalData message
        msg = IB::Messages::Incoming::HistoricalData.new(request_id: 1, results: [bar_one])
        block.call(msg)
        :sub
      end
      allow(connection).to receive(:send_message)
      allow(connection).to receive(:unsubscribe)

      bars = provider.fetch_bars(
        symbol: "SPY",
        frequency: "day",
        start_date: Date.new(2026, 4, 1),
        end_date: Date.new(2026, 4, 1),
        extended_hours: false,
        previous_close: false
      )

      expect(bars.length).to eq(1)
      expect(bars.first.close).to eq(100.5)
    end

    it "returns empty array and continues when an Alert (error) is received" do
      allow(connection).to receive(:subscribe).with(:HistoricalData, :Alert) do |*args, &block|
        # Simulate receiving Alert message for the request
        msg = IB::Messages::Incoming::Alert.new(error_id: 1, code: 162, message: "No data")
        block.call(msg)
        :sub
      end
      allow(connection).to receive(:send_message)
      allow(connection).to receive(:unsubscribe)

      bars = provider.fetch_bars(
        symbol: "SPY",
        frequency: "day",
        start_date: Date.new(1900, 1, 1),
        end_date: Date.new(1900, 1, 1),
        extended_hours: false,
        previous_close: false
      )

      expect(bars).to eq([])
      expect(connection).to have_received(:send_message).once
    end
  end
end
