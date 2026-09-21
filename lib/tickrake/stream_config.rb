# frozen_string_literal: true

module Tickrake
  # Configuration for a single stream subscription within a consolidated stream job.
  # Each subscription has its own symbols, service type, settings, and schedule windows.
  StreamSubscription = Struct.new(
    :name,        # String — used for logging and EventsWriter job_name
    :kind,        # :level_one | :order_book | :chart_stream
    :symbols,     # Array<String>
    :settings,    # LevelOneConfig | OrderBookConfig | ChartStreamConfig
    :windows,     # Array<SchedulerWindow> — nil/empty means always active
    keyword_init: true
  ) do
    def in_window?(time)
      return true if windows.nil? || windows.empty?

      day = time.strftime("%a").downcase[0, 3]
      minutes = (time.hour * 60) + time.min

      windows.any? do |window|
        next false unless window.days.include?(day)

        start_minutes = (window.start_time[0] * 60) + window.start_time[1]
        end_minutes   = (window.end_time[0] * 60) + window.end_time[1]
        minutes >= start_minutes && minutes <= end_minutes
      end
    end
  end

  # Top-level config for a consolidated stream job.
  StreamConfig = Struct.new(
    :subscriptions,             # Array<StreamSubscription>
    :stale_timeout_seconds,     # Integer
    keyword_init: true
  )
end
