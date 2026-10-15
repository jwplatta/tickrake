# frozen_string_literal: true

module Tickrake
  module DB
    class << self
      def connection(path, logger: nil)
        expanded = Tickrake::PathSupport.expand_path(path)
        return @connection if defined?(@connection) && @connection && @connection_path == expanded

        @connection&.close rescue nil
        @connection_path = expanded
        logger&.info({ msg: "Opening tickrake database", event: "tickrake_db_open", path: expanded })
        @connection = SQLite3::Database.new(expanded).tap do |d|
          d.results_as_hash = true
          d.busy_timeout(30_000)
          d.execute("PRAGMA journal_mode = WAL")
          d.execute("PRAGMA wal_autocheckpoint = 0")
        end
      end
    end
  end
end
