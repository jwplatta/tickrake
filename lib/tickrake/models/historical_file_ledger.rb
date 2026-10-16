# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class HistoricalFileLedger < Base
      self.table_name = "historical_file_ledger"

      validates :dataset_type, :provider, :symbol, :local_path, :remote_bucket,
                :remote_key, :remote_uri, :file_size, :row_count, :checksum,
                :published_at, presence: true

      scope :by_dataset, ->(type) { where(dataset_type: type.to_s) }

      def self.record_publication!(attrs)
        now = Time.now.utc.iso8601
        payload = attrs.merge(updated_at: now)

        record = find_or_initialize_by(remote_key: attrs[:remote_key])
        record.created_at ||= now
        record.assign_attributes(payload)
        record.save!
        record
      rescue ActiveRecord::RecordNotUnique
        find_by(remote_key: attrs[:remote_key])
      end
    end
  end
end
