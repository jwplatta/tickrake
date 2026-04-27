# frozen_string_literal: true

module Tickrake
  module DB
    class Migrator
      def initialize(database, migrations:)
        @database = database
        @migrations = migrations.sort_by(&:version)
      end

      def migrate!
        ensure_schema_migrations_table
        @migrations.each do |migration|
          next if migrated_versions.include?(migration.version)

          migration.new(@database).up
          record_migration(migration.version)
        end
      end

      private

      def ensure_schema_migrations_table
        @database.execute_batch(
          <<~SQL
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY
            );
          SQL
        )
      end

      def migrated_versions
        @migrated_versions ||= @database.execute("SELECT version FROM schema_migrations ORDER BY version").map do |row|
          row["version"].to_i
        end
      end

      def record_migration(version)
        @database.execute("INSERT INTO schema_migrations (version) VALUES (?)", [version])
        migrated_versions << version
      end
    end
  end
end
