# frozen_string_literal: true

require "net/http"
require "uri"
require "tzinfo"
require "date"

module Tickrake
  module Fetchers
    class BlsCalendar
      ICS_URL   = "https://www.bls.gov/schedule/news_release/bls.ics"
      EASTERN_TZ = TZInfo::Timezone.get("America/New_York")

      def fetch(from_date:, to_date:)
        body = download_ics
        fetched_at = Time.now.utc.iso8601
        parse_ics(body, from_date, to_date, fetched_at)
      end

      private

      def download_ics
        uri = URI(ICS_URL)
        req = Net::HTTP::Get.new(uri)
        req["User-Agent"] = "Tickrake/1.0 (economic events calendar fetcher)"
        response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https") { |http| http.request(req) }
        raise Tickrake::Error, "BLS ICS fetch failed #{response.code}" unless response.is_a?(Net::HTTPSuccess)

        response.body
      end

      def parse_ics(body, from_date, to_date, fetched_at)
        events = []
        current = nil

        body.each_line do |raw|
          line = raw.strip.encode("UTF-8", invalid: :replace, undef: :replace)
          case line
          when /\ABEGIN:VEVENT\z/
            current = {}
          when /\AEND:VEVENT\z/
            if current && current[:event_datetime] && current[:event_name]
              date = Date.parse(current[:event_datetime][0, 10])
              if date >= from_date && date <= to_date
                events << current.merge(
                  source: "bls",
                  category: "economic",
                  actual: nil,
                  estimate: nil,
                  previous: nil,
                  unit: nil,
                  symbol: nil,
                  company_name: nil,
                  time_of_day: nil,
                  release_id: nil,
                  series_id: nil,
                  fetched_at: fetched_at
                )
              end
            end
            current = nil
          when /\ADTSTART;TZID=([^:]+):(\d{8}T\d{6})\z/
            current[:event_datetime] = to_utc($1, $2) if current
          when /\ASUMMARY:(.*)\z/
            current[:event_name] = $1 if current
          end
        end

        events
      end

      def to_utc(tzid_str, dt_str)
        year  = dt_str[0, 4].to_i
        month = dt_str[4, 2].to_i
        day   = dt_str[6, 2].to_i
        hour  = dt_str[9, 2].to_i
        min   = dt_str[11, 2].to_i
        sec   = dt_str[13, 2].to_i

        tz = begin
          TZInfo::Timezone.get(tzid_str)
        rescue TZInfo::InvalidTimezoneIdentifier
          EASTERN_TZ
        end

        local_time = Time.new(year, month, day, hour, min, sec)
        tz.local_to_utc(local_time).strftime("%Y-%m-%dT%H:%M:%SZ")
      end
    end
  end
end
