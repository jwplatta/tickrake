# frozen_string_literal: true

class CreateHistoricalFileLedger < ActiveRecord::Migration[7.1]
  def change
    create_table :historical_file_ledger do |t|
      t.string :dataset_type, null: false
      t.string :provider, null: false
      t.string :symbol, null: false
      t.string :frequency
      t.string :date_or_year
      t.string :local_path, null: false
      t.string :remote_bucket, null: false
      t.string :remote_key, null: false
      t.string :remote_uri, null: false
      t.bigint :file_size, null: false
      t.bigint :row_count, null: false
      t.string :checksum, null: false
      t.string :published_at, null: false
      t.string :created_at, null: false
      t.string :updated_at, null: false
    end

    add_index :historical_file_ledger, :remote_key, unique: true, name: "idx_historical_ledger_remote_key"
    add_index :historical_file_ledger, %i[dataset_type provider symbol], name: "idx_historical_ledger_lookup"
  end
end
