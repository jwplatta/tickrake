# frozen_string_literal: true

require "tzinfo"
require "date"

module Tickrake
  module Fetchers
    class FredEconomicCalendar
      RELEASES = {
        "GDP"              => { id: 53, series: "GDP",   time_et: "08:30" },
        "PCE"              => { id: 83, series: "PCEPI", time_et: "08:30" },
        "Retail Sales"     => { id: 42, series: nil,     time_et: "08:30" },
        "Housing Starts"   => { id: 18, series: nil,     time_et: "08:30" },
        "Industrial Prod." => { id: 13, series: nil,     time_et: "09:15" },
        "Consumer Credit"  => { id: 14, series: nil,     time_et: "15:00" }
      }.freeze

      EASTERN_TZ = TZInfo::Timezone.get("America/New_York")

      def initialize(client: FredClient.new, logger: nil)
        @client = client
        @logger = logger
      end

      def fetch(from_date:, to_date:)
        fetched_at = Time.now.utc.iso8601
        rows = []

        RELEASES.each do |event_name, meta|
          rows.concat(fetch_for_release(event_name, meta, from_date, to_date, fetched_at))
        end

        rows
      end

      private

      def fetch_for_release(event_name, meta, from_date, to_date, fetched_at)
        data = @client.get("/release/dates", {
          release_id: meta[:id],
          realtime_start: from_date.iso8601,
          realtime_end: to_date.iso8601,
          include_release_dates_with_no_data: "true"
        })

        (data["release_dates"] || []).filter_map do |rd|
          date = Date.parse(rd["date"])
          next unless date >= from_date && date <= to_date

          actual = meta[:series] && date <= Date.today ? fetch_actual(meta[:series], date) : nil

          {
            event_datetime: et_to_utc(date, meta[:time_et]),
            event_name: event_name,
            category: "economic",
            source: "fred",
            actual: actual,
            estimate: nil,
            previous: nil,
            unit: nil,
            symbol: nil,
            company_name: nil,
            time_of_day: nil,
            release_id: meta[:id],
            series_id: meta[:series],
            fetched_at: fetched_at
          }
        end
      rescue StandardError => e
        @logger&.warn("FredEconomicCalendar: error fetching #{event_name}: #{e.message}")
        []
      end

      def fetch_actual(series_id, date)
        data = @client.get("/series/observations", {
          series_id: series_id,
          observation_start: date.iso8601,
          observation_end: date.iso8601
        })
        obs = (data["observations"] || []).first
        return nil unless obs

        val = obs["value"]
        return nil if val.nil? || val == "."

        val.to_f
      rescue StandardError
        nil
      end

      def et_to_utc(date, time_et)
        hour, min = time_et.split(":").map(&:to_i)
        local_time = Time.new(date.year, date.month, date.day, hour, min, 0)
        EASTERN_TZ.local_to_utc(local_time).strftime("%Y-%m-%dT%H:%M:%SZ")
      end
    end
  end
end
