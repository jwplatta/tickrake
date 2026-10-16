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

          if db_config.sqlite?
            conn = ActiveRecord::Base.connection
            conn.execute("PRAGMA journal_mode = WAL") rescue nil
            conn.execute("PRAGMA wal_autocheckpoint = 0") rescue nil
            conn.execute("PRAGMA busy_timeout = 30000") rescue nil
          end

          ActiveRecord::Base.descendants.each(&:reset_column_information)

          ActiveRecord::Base.connection
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
