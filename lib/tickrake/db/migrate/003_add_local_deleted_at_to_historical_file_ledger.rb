# frozen_string_literal: true

class AddLocalDeletedAtToHistoricalFileLedger < ActiveRecord::Migration[7.1]
  def change
    add_column :historical_file_ledger, :local_deleted_at, :string
    add_index :historical_file_ledger, :local_deleted_at, name: "idx_historical_ledger_local_deleted_at"
  end
end
