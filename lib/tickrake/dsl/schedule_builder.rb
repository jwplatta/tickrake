# frozen_string_literal: true

module Tickrake
  module DSL
    class ScheduleBuilder
      WEEKDAYS = %w[mon tue wed thu fri].freeze
      WEEKENDS = %w[sat sun].freeze
      ALL_DAYS  = (WEEKDAYS + WEEKENDS).freeze

      def initialize
        @interval_seconds = nil
        @windows = []
        @run_at = nil
        @days = []
      end

      def every(duration)
        @interval_seconds = duration.to_interval_seconds
      end

      def at(clock)
        @run_at = parse_clock(clock)
      end

      def weekdays(**kwargs)
        if kwargs.empty?
          @days = WEEKDAYS.dup
        else
          from = parse_clock(kwargs.fetch(:from))
          to   = parse_clock(kwargs.fetch(:to))
          @windows << Tickrake::SchedulerWindow.new(days: WEEKDAYS.dup, start_time: from, end_time: to)
        end
      end

      def weekends(**kwargs)
        if kwargs.empty?
          @days = WEEKENDS.dup
        else
          from = parse_clock(kwargs.fetch(:from))
          to   = parse_clock(kwargs.fetch(:to))
          @windows << Tickrake::SchedulerWindow.new(days: WEEKENDS.dup, start_time: from, end_time: to)
        end
      end

      def every_day(**kwargs)
        if kwargs.empty?
          @days = ALL_DAYS.dup
        else
          from = parse_clock(kwargs.fetch(:from))
          to   = parse_clock(kwargs.fetch(:to))
          @windows << Tickrake::SchedulerWindow.new(days: ALL_DAYS.dup, start_time: from, end_time: to)
        end
      end

      def days(day_list, from:, to:)
        normalized = Array(day_list).map { |d| d.to_s.downcase[0, 3] }
        start_time = parse_clock(from)
        end_time   = parse_clock(to)
        @windows << Tickrake::SchedulerWindow.new(days: normalized, start_time: start_time, end_time: end_time)
      end

      def build!
        {
          interval_seconds: @interval_seconds,
          windows: @windows,
          run_at: @run_at,
          days: @days
        }
      end

      private

      def parse_clock(value)
        match = /\A(\d{1,2}):(\d{2})\z/.match(value.to_s)
        raise Tickrake::Error, "Invalid clock value: #{value.inspect}" unless match

        [Integer(match[1], 10), Integer(match[2], 10)]
      end
    end
  end
end
