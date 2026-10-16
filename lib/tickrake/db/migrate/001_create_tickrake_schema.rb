# frozen_string_literal: true

class CreateTickrakeSchema < ActiveRecord::Migration[7.1]
  def change
    create_table :fetch_runs do |t|
      t.string :job_type, null: false
      t.string :dataset_type, null: false
      t.string :symbol, null: false
      t.string :frequency
      t.string :option_root
      t.text :requested_buckets
      t.string :resolved_expiration
      t.string :scheduled_for
      t.string :started_at, null: false
      t.string :finished_at
      t.string :status, null: false
      t.string :output_path
      t.text :error_message
      t.string :collection_id
    end
    add_index :fetch_runs, %i[dataset_type option_root collection_id], name: "idx_fetch_runs_collection"

    create_table :file_metadata_cache, id: false do |t|
      t.string :path, null: false, primary_key: true
      t.string :dataset_type, null: false
      t.string :provider_name, null: false
      t.string :ticker, null: false
      t.string :frequency
      t.string :expiration_date
      t.bigint :row_count, null: false
      t.string :first_observed_at
      t.string :last_observed_at
      t.bigint :file_mtime, null: false
      t.bigint :file_size, null: false
      t.string :updated_at, null: false
      t.string :collection_id
    end
    add_index :file_metadata_cache, %i[dataset_type provider_name ticker frequency],
              name: "idx_file_metadata_candles_lookup"
    add_index :file_metadata_cache, %i[dataset_type provider_name expiration_date last_observed_at],
              name: "idx_file_metadata_options_lookup"
    add_index :file_metadata_cache, %i[dataset_type provider_name ticker last_observed_at expiration_date],
              name: "idx_file_metadata_options_ticker_time_lookup"
    add_index :file_metadata_cache, %i[dataset_type provider_name ticker collection_id],
              name: "idx_file_metadata_collection"

    create_table :market_indexes do |t|
      t.string :code, null: false
      t.string :name, null: false
      t.string :created_at, null: false
      t.string :updated_at, null: false
    end
    add_index :market_indexes, :code, unique: true, name: "idx_market_indexes_code"

    create_table :tickers do |t|
      t.string :ticker, null: false
      t.string :security_name
      t.string :gics_sector
      t.string :gics_sub_industry
      t.string :headquarters_location
      t.string :cik
      t.string :founded
      t.string :status
      t.string :created_at, null: false
      t.string :updated_at, null: false
    end
    add_index :tickers, :ticker, unique: true

    create_table :ticker_aliases do |t|
      t.references :ticker, null: false, foreign_key: { to_table: :tickers }, index: false
      t.string :alias_ticker, null: false
      t.string :start_date
      t.string :end_date
      t.string :created_at, null: false
      t.string :updated_at, null: false
    end
    add_index :ticker_aliases, :alias_ticker, unique: true
    add_index :ticker_aliases, %i[alias_ticker start_date end_date], name: "idx_ticker_aliases_alias_dates"
    add_index :ticker_aliases, %i[ticker_id start_date end_date], name: "idx_ticker_aliases_ticker_dates"

    create_table :market_index_memberships do |t|
      t.references :market_index, null: false, foreign_key: { to_table: :market_indexes }, index: false
      t.references :ticker, null: false, foreign_key: { to_table: :tickers }, index: false
      t.string :start_date, null: false
      t.string :end_date
      t.string :created_at, null: false
      t.string :updated_at, null: false
    end
    add_index :market_index_memberships, %i[market_index_id ticker_id start_date],
              unique: true, name: "idx_market_index_memberships_unique"
    add_index :market_index_memberships, %i[market_index_id start_date end_date ticker_id],
              name: "idx_market_index_memberships_index_dates_ticker"
    add_index :market_index_memberships, %i[ticker_id start_date end_date],
              name: "idx_market_index_memberships_ticker_dates"

    create_table :api_rate_limits, id: false do |t|
      t.string :provider, null: false, primary_key: true
      t.float :tokens, null: false
      t.float :last_refill_at, null: false
    end
  end
end
