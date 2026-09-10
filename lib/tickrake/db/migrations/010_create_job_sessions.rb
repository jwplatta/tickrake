# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class CreateJobSessions
        def self.version = 10

        def initialize(database)
          @database = database
        end

        def up
          @database.execute_batch(<<~SQL)
            CREATE TABLE IF NOT EXISTS job_sessions (
              id             INTEGER PRIMARY KEY AUTOINCREMENT,
              job_name       TEXT NOT NULL UNIQUE,
              job_type       TEXT NOT NULL,
              provider       TEXT,
              parameters_json TEXT,
              started_at     INTEGER NOT NULL,
              heartbeat_at   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_job_sessions_heartbeat
              ON job_sessions (heartbeat_at);
          SQL
        end
      end
    end
  end
end
