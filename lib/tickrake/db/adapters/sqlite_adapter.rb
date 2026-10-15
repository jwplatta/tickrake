# frozen_string_literal: true

require "sqlite3"

module Tickrake
  module DB
    module Adapters
      class SqliteAdapter
        attr_reader :path

        def initialize(path)
          @path = Tickrake::PathSupport.expand_path(path)
          FileUtils.mkdir_p(File.dirname(@path))
        end

        def backend
          "sqlite"
        end

        def connection
          @connection ||= SQLite3::Database.new(@path).tap do |db|
            db.busy_timeout(30_000)
            db.execute("PRAGMA journal_mode = WAL")
            db.execute("PRAGMA wal_autocheckpoint = 0")
            db.results_as_hash = true
          end
        end

        def close
          return unless @connection

          @connection.close
          @connection = nil
        end

        def checkpoint!
          connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
        end

        def transaction(mode = :deferred, &block)
          connection.transaction(mode, &block)
        end

        def transaction_active?
          connection.transaction_active?
        end

        def rollback
          connection.rollback
        end

        def commit
          connection.commit
        end

        def execute(sql, binds = [])
          connection.execute(sql, binds)
        end

        def execute_batch(sql)
          connection.execute_batch(sql)
        end

        def get_first_row(sql, binds = [])
          connection.get_first_row(sql, binds)
        end

        def get_first_value(sql, binds = [])
          connection.get_first_value(sql, binds)
        end

        def last_insert_id
          connection.last_insert_row_id
        end

        def changes
          connection.changes
        end

        def table_exists?(table_name)
          val = get_first_value(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            [table_name.to_s]
          )
          !val.nil?
        end

        def tables
          execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").map do |r|
            r["name"]
          end
        end

        def row_count(table_name)
          get_first_value("SELECT COUNT(*) FROM #{table_name}").to_i
        end

        def table_info(table_name)
          connection.table_info(table_name)
        end

        def method_missing(method_name, *args, &block)
          if connection.respond_to?(method_name)
            connection.public_send(method_name, *args, &block)
          else
            super
          end
        end

        def respond_to_missing?(method_name, include_private = false)
          connection.respond_to?(method_name, include_private) || super
        end
      end
    end
  end
end
