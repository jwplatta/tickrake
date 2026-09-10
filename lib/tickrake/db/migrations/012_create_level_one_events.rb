# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class CreateLevelOneEvents
        def self.version = 12

        def initialize(database)
          @database = database
        end

        def up
          @database.execute_batch(<<~SQL)
            CREATE TABLE IF NOT EXISTS level_one_events (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              job_name      TEXT NOT NULL,
              received_at   INTEGER NOT NULL,
              symbol        TEXT NOT NULL,
              service       TEXT NOT NULL,
              quote_time_ms INTEGER,
              trade_time_ms INTEGER,
              bid           REAL,
              ask           REAL,
              last          REAL,
              bid_size      INTEGER,
              ask_size      INTEGER,
              volume        INTEGER,
              mark          REAL,
              extra_json    TEXT,
              flushed       INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_level_one_events_job_flushed
              ON level_one_events (job_name, flushed, received_at);
          SQL
        end
      end
    end
  end
end
