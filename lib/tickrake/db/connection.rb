# frozen_string_literal: true

module Tickrake
  module DB
    class << self
      def connection(path)
        return @connection if defined?(@connection)

        @connection = SQLite3::Database.new(path).tap do |d|
          d.results_as_hash = true
          d.busy_timeout(5_000)
          d.execute("PRAGMA journal_mode = WAL")
        end
      end
    end
  end
end
