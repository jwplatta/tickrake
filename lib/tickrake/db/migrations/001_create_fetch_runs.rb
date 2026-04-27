# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class CreateFetchRuns
        def self.version
          1
        end

        def initialize(database)
          @database = database
        end

        def up
          @database.execute_batch(
            <<~SQL
              CREATE TABLE IF NOT EXISTS fetch_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                option_root TEXT,
                requested_buckets TEXT,
                resolved_expiration TEXT,
                scheduled_for TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                output_path TEXT,
                error_message TEXT
              );
            SQL
          )
        end
      end
    end
  end
end
