# frozen_string_literal: true

module Tickrake
  module DB
    module Adapters
      class << self
        def build(config_or_path)
          if config_or_path.respond_to?(:adapter) && config_or_path.adapter
            config_or_path.adapter
          elsif config_or_path.is_a?(SqliteAdapter) || config_or_path.is_a?(PostgresAdapter)
            config_or_path
          elsif config_or_path.is_a?(Tickrake::DatabaseConfig)
            build_from_database_config(config_or_path)
          elsif config_or_path.is_a?(Tickrake::Config)
            build_from_database_config(config_or_path.database)
          elsif config_or_path.is_a?(String)
            SqliteAdapter.new(config_or_path)
          elsif config_or_path.nil?
            SqliteAdapter.new(Tickrake::PathSupport.sqlite_path)
          else
            raise ArgumentError, "Cannot build database adapter from #{config_or_path.inspect}"
          end
        end

        private

        def build_from_database_config(db_config)
          if db_config.postgres?
            PostgresAdapter.new(db_config)
          else
            SqliteAdapter.new(db_config.sqlite_path || Tickrake::PathSupport.sqlite_path)
          end
        end
      end
    end
  end
end
