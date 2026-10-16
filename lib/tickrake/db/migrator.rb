# frozen_string_literal: true

require "active_record"
require_relative "connection_manager"

module Tickrake
  module DB
    class Migrator
      MIGRATIONS_PATH = File.expand_path("migrate", __dir__)

      class << self
        def migrations_path
          MIGRATIONS_PATH
        end

        def migrate!(config_or_path = nil)
          ConnectionManager.establish_connection(config_or_path)
          context = ActiveRecord::MigrationContext.new(migrations_path)
          context.migrate
        end

        def pending?(config_or_path = nil)
          ConnectionManager.establish_connection(config_or_path)
          context = ActiveRecord::MigrationContext.new(migrations_path)
          context.needs_migration?
        end
      end
    end
  end
end
