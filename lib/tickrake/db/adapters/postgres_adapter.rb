# frozen_string_literal: true

require "pg"

module Tickrake
  module DB
    module Adapters
      class PostgresAdapter
        attr_reader :config

        def initialize(config)
          @config = config
          @in_transaction = false
        end

        def backend
          "postgres"
        end

        def connection
          @connection ||= PG.connect(@config.postgres_connection_params)
        end

        def close
          return unless @connection

          @connection.close
          @connection = nil
        end

        def checkpoint!
          # No-op in PostgreSQL
        end

        def transaction
          if transaction_active?
            yield
          else
            connection.exec("BEGIN")
            @in_transaction = true
            begin
              res = yield
              connection.exec("COMMIT")
              res
            rescue StandardError
              connection.exec("ROLLBACK") rescue nil
              raise
            ensure
              @in_transaction = false
            end
          end
        end

        def transaction_active?
          @in_transaction || connection.transaction_status != PG::PQTRANS_IDLE
        end

        def rollback
          connection.exec("ROLLBACK")
          @in_transaction = false
        end

        def commit
          connection.exec("COMMIT")
          @in_transaction = false
        end

        def execute(sql, binds = [])
          pg_sql = convert_placeholders(sql)
          prepared_binds = prepare_binds(binds)
          res = if prepared_binds.empty?
                  connection.exec(pg_sql)
                else
                  connection.exec_params(pg_sql, prepared_binds)
                end

          @last_changes = res.cmd_tuples
          res.map { |row| row }
        ensure
          res&.clear rescue nil
        end

        def execute_batch(sql)
          connection.exec(sql)
        end

        def get_first_row(sql, binds = [])
          rows = execute(sql, binds)
          rows.first
        end

        def get_first_value(sql, binds = [])
          row = get_first_row(sql, binds)
          row ? row.values.first : nil
        end

        def changes
          @last_changes || 0
        end

        def table_exists?(table_name)
          val = get_first_value(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = $1",
            [table_name.to_s]
          )
          !val.nil?
        end

        def tables
          execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'").map do |r|
            r["tablename"]
          end
        end

        def row_count(table_name)
          get_first_value("SELECT COUNT(*) FROM #{table_name}").to_i
        end

        def reset_sequence!(table_name, pk = "id")
          execute(
            <<~SQL
              SELECT setval(
                pg_get_serial_sequence('#{table_name}', '#{pk}'),
                COALESCE((SELECT MAX(#{pk}) FROM #{table_name}), 1),
                (SELECT COUNT(*) > 0 FROM #{table_name})
              );
            SQL
          )
        rescue StandardError
          # Table might not have sequence for pk, ignore
        end

        private

        def convert_placeholders(sql)
          param_index = 0
          # Replace '?' with $1, $2, etc., but avoid questions inside strings
          # Since tickrake queries only use ? as parameter markers, scanning/gsub is safe.
          sql.gsub(/\?/) do
            param_index += 1
            "$#{param_index}"
          end
        end

        def prepare_binds(binds)
          Array(binds).map do |val|
            case val
            when Time, DateTime
              val.utc.iso8601
            when Date
              val.iso8601
            when true
              "t"
            when false
              "f"
            else
              val
            end
          end
        end
      end
    end
  end
end
