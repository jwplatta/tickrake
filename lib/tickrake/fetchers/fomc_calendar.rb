# frozen_string_literal: true

require "tzinfo"
require "date"

module Tickrake
  module Fetchers
    class FomcCalendar
      DECISION_TIME_ET  = "14:00"
      FOMC_RELEASE_ID   = 21
      RATE_SERIES_UPPER = "DFEDTARU"
      EASTERN_TZ        = TZInfo::Timezone.get("America/New_York")

      def initialize(client: FredClient.new, logger: nil)
        @client = client
        @logger = logger
      end

      def fetch(from_date:, to_date:)
        fetched_at = Time.now.utc.iso8601
        dates = fetch_meeting_dates(from_date, to_date)
        return [] if dates.empty?

        rate_values = fetch_rate_values(from_date, to_date)

        dates.map do |date|
          {
            event_datetime: et_to_utc(date, DECISION_TIME_ET),
            event_name: "FOMC Rate Decision",
            category: "fomc",
            source: "fred",
            actual: rate_values[date],
            estimate: nil,
            previous: nil,
            unit: "percent",
            symbol: nil,
            company_name: nil,
            time_of_day: nil,
            release_id: FOMC_RELEASE_ID,
            series_id: RATE_SERIES_UPPER,
            fetched_at: fetched_at
          }
        end
      end

      private

      def fetch_meeting_dates(from_date, to_date)
        data = @client.get("/release/dates", {
          release_id: FOMC_RELEASE_ID,
          realtime_start: from_date.iso8601,
          realtime_end: to_date.iso8601,
          include_release_dates_with_no_data: "true"
        })
        (data["release_dates"] || []).filter_map do |rd|
          date = Date.parse(rd["date"])
          date if date >= from_date && date <= to_date
        end
      rescue StandardError => e
        @logger&.warn("FomcCalendar: error fetching meeting dates: #{e.message}")
        []
      end

      def fetch_rate_values(from_date, to_date)
        data = @client.get("/series/observations", {
          series_id: RATE_SERIES_UPPER,
          observation_start: from_date.iso8601,
          observation_end: to_date.iso8601
        })
        result = {}
        (data["observations"] || []).each do |obs|
          val = obs["value"]
          next if val.nil? || val == "."

          result[Date.parse(obs["date"])] = val.to_f
        end
        result
      rescue StandardError => e
        @logger&.warn("FomcCalendar: error fetching rate values: #{e.message}")
        {}
      end

      def et_to_utc(date, time_et)
        hour, min = time_et.split(":").map(&:to_i)
        local_time = Time.new(date.year, date.month, date.day, hour, min, 0)
        EASTERN_TZ.local_to_utc(local_time).strftime("%Y-%m-%dT%H:%M:%SZ")
      end
    end
  end
end
