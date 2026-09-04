# frozen_string_literal: true

module Tickrake
  module DB
    @connections = {}
    @mutex = Mutex.new

    class << self
      def connection(path)
        @mutex.synchronize do
          @connections[path] ||= SQLite3::Database.new(path).tap do |d|
            d.results_as_hash = true
            d.busy_timeout(5_000)
            d.execute("PRAGMA journal_mode = WAL")
          end
        end
      end
    end
  end
end
