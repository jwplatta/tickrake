# frozen_string_literal: true

module Tickrake
  module DB
    class << self
      def connection(path)
        return @connection if defined?(@connection)

        @connection = SQLite3::Database.new(path).tap do |d|
          d.results_as_hash = true
          d.busy_timeout(30_000)
          d.execute("PRAGMA journal_mode = WAL")
          d.execute("PRAGMA wal_autocheckpoint = 0")
        end
      end
    end
  end
end
