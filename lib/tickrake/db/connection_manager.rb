# frozen_string_literal: true

require "active_record"
require "fileutils"

module Tickrake
  module DB
    class ConnectionManager
      class << self
        def establish_connection(config_or_path = nil)
          db_config = resolve_config(config_or_path)
          spec = connection_spec(db_config)
          ActiveRecord::Base.establish_connection(spec)

          conn = verify_connection!(db_config)

          if db_config.sqlite?
            conn.execute("PRAGMA journal_mode = WAL") rescue nil
            conn.execute("PRAGMA wal_autocheckpoint = 0") rescue nil
            conn.execute("PRAGMA busy_timeout = 30000") rescue nil
          end

          ActiveRecord::Base.descendants.each(&:reset_column_information)

          conn
        end

        def verify_connection!(db_config = nil)
          ActiveRecord::Base.connection_pool.with_connection do |conn|
            conn.execute("SELECT 1")
            conn
          end
        rescue StandardError => e
          if connection_error?(e)
            raise Tickrake::DatabaseConnectionError, format_connection_error(e, db_config)
          end
          raise
        end

        def connection_error?(error)
          return true if defined?(ActiveRecord::ConnectionNotEstablished) && error.is_a?(ActiveRecord::ConnectionNotEstablished)
          return true if defined?(ActiveRecord::DatabaseConnectionError) && error.is_a?(ActiveRecord::DatabaseConnectionError)
          return true if defined?(ActiveRecord::NoDatabaseError) && error.is_a?(ActiveRecord::NoDatabaseError)
          return true if defined?(ActiveRecord::ConnectionFailed) && error.is_a?(ActiveRecord::ConnectionFailed)
          return true if defined?(PG::Error) && (error.is_a?(PG::Error) || (error.respond_to?(:cause) && error.cause.is_a?(PG::Error)))
          return true if defined?(PG::ConnectionBad) && (error.is_a?(PG::ConnectionBad) || (error.respond_to?(:cause) && error.cause.is_a?(PG::ConnectionBad)))

          false
        end

        def format_connection_error(error, db_config = nil)
          primary_msg = error.message.to_s.gsub(/\s+/, " ").strip
          cause_msg = error.cause&.message.to_s.gsub(/\s+/, " ").strip if error.respond_to?(:cause) && error.cause

          target_info = if db_config&.postgres?
                          host = db_config.host || "default"
                          db = db_config.database || "default"
                          user = db_config.user || "default"
                          " (target: #{user}@#{host}/#{db})"
                        elsif db_config&.sqlite?
                          path = db_config.sqlite_path || "sqlite"
                          " (target: #{path})"
                        else
                          ""
                        end

          detail = if cause_msg && !cause_msg.empty? && !primary_msg.include?(cause_msg)
                     "#{cause_msg} - #{primary_msg}"
                   else
                     primary_msg
                   end

          "Database connection error#{target_info}: #{detail}"
        end

        def connection_spec(db_config)
          if db_config.postgres?
            if db_config.url && !db_config.url.empty?
              db_config.url
            else
              {
                adapter: "postgresql",
                host: db_config.host,
                port: db_config.port ? Integer(db_config.port) : 5432,
                database: db_config.database,
                username: db_config.user,
                password: db_config.password,
                sslmode: db_config.sslmode,
                connect_timeout: db_config.connect_timeout ? Integer(db_config.connect_timeout) : 10,
                pool: 10
              }.compact
            end
          else
            path = Tickrake::PathSupport.expand_path(db_config.sqlite_path || Tickrake::PathSupport.sqlite_path)
            FileUtils.mkdir_p(File.dirname(path))
            {
              adapter: "sqlite3",
              database: path,
              timeout: 30_000,
              pool: 10
            }
          end
        end

        def resolve_config(config_or_path)
          if config_or_path.is_a?(Tickrake::DatabaseConfig)
            config_or_path
          elsif config_or_path.respond_to?(:database) && config_or_path.database.is_a?(Tickrake::DatabaseConfig)
            config_or_path.database
          elsif config_or_path.is_a?(String)
            Tickrake::DatabaseConfig.new(backend: "sqlite", sqlite_path: config_or_path)
          elsif config_or_path.respond_to?(:sqlite_path) && config_or_path.sqlite_path
            Tickrake::DatabaseConfig.new(backend: "sqlite", sqlite_path: config_or_path.sqlite_path)
          else
            Tickrake::DatabaseConfig.new(backend: "sqlite", sqlite_path: Tickrake::PathSupport.sqlite_path)
          end
        end
      end
    end
  end
end
