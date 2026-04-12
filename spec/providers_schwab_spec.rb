# frozen_string_literal: true

RSpec.describe Tickrake::Providers::Schwab do
  it "normalizes fetched candles into Tickrake bars" do
    client = instance_double("client")
    allow(client).to receive(:get_price_history).and_return(
      {
        candles: [
          {
            datetime: Time.utc(2026, 4, 1, 14, 0, 0).to_i * 1000,
            open: 1.0,
            high: 2.0,
            low: 0.5,
            close: 1.5,
            volume: 10
          }
        ]
      }
    )

    bars = described_class.new(provider_name: "schwab_main", client: client).fetch_bars(
      symbol: "SPX",
      frequency: "1min",
      start_date: Date.new(2026, 4, 1),
      end_date: Date.new(2026, 4, 2),
      extended_hours: false,
      previous_close: false
    )

    expect(bars.length).to eq(1)
    expect(bars.first).to be_a(Tickrake::Data::Bar)
    expect(bars.first.datetime).to eq(Time.utc(2026, 4, 1, 14, 0, 0))
    expect(bars.first.source).to eq("schwab_main")
    expect(client).to have_received(:get_price_history).with(
      "$SPX",
      hash_including(
        start_datetime: Date.new(2026, 4, 1),
        end_datetime: Date.new(2026, 4, 2),
        return_data_objects: false
      )
    )
  end
end
