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
      scope :locally_retained, -> { where(local_deleted_at: nil) }
      scope :eligible_for_retention_cleanup, lambda { |cutoff|
        locally_retained
          .where("published_at <= ?", cutoff)
          .where.not(dataset_type: "candles")
      }

      def mark_locally_deleted!(timestamp = Time.now.utc.iso8601)
        update!(local_deleted_at: timestamp, updated_at: Time.now.utc.iso8601)
      end

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
