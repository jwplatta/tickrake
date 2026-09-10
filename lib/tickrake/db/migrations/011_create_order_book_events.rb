# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class CreateOrderBookEvents
        def self.version = 11

        def initialize(database)
          @database = database
        end

        def up
          @database.execute_batch(<<~SQL)
            CREATE TABLE IF NOT EXISTS order_book_events (
              id          INTEGER PRIMARY KEY AUTOINCREMENT,
              job_name    TEXT NOT NULL,
              received_at INTEGER NOT NULL,
              symbol      TEXT NOT NULL,
              service     TEXT NOT NULL,
              book_time_ms INTEGER,
              bids_json   TEXT,
              asks_json   TEXT,
              flushed     INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_order_book_events_job_flushed
              ON order_book_events (job_name, flushed, received_at);
          SQL
        end
      end
    end
  end
end
