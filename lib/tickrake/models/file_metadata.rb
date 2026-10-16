# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class FileMetadata < Base
      self.table_name = "file_metadata_cache"
      self.primary_key = :path

      class << self
        def aggregate_summary(where: nil, binds: [])
          rel = apply_where(all, where, binds)
          result = rel.pick(
            Arel.sql("COUNT(*) AS file_count"),
            Arel.sql("COALESCE(SUM(file_size), 0) AS total_bytes"),
            Arel.sql("MIN(last_observed_at) AS oldest_observed_at"),
            Arel.sql("MAX(last_observed_at) AS newest_observed_at")
          )

          return { "file_count" => 0, "total_bytes" => 0, "oldest_observed_at" => nil, "newest_observed_at" => nil } unless result

          {
            "file_count" => result[0].to_i,
            "total_bytes" => result[1].to_i,
            "oldest_observed_at" => result[2],
            "newest_observed_at" => result[3]
          }
        end

        def aggregate_by_provider(where: nil, binds: [])
          rel = apply_where(all, where, binds)
          rel.group(:provider_name)
             .order(:provider_name)
             .pluck(
               :provider_name,
               Arel.sql("COUNT(*)"),
               Arel.sql("COALESCE(SUM(file_size), 0)"),
               Arel.sql("MIN(last_observed_at)"),
               Arel.sql("MAX(last_observed_at)")
             ).map do |row|
            {
              "provider_name" => row[0],
              "file_count" => row[1].to_i,
              "total_bytes" => row[2].to_i,
              "oldest_observed_at" => row[3],
              "newest_observed_at" => row[4]
            }
          end
        end

        def largest(where: nil, binds: [], limit: 5)
          rel = apply_where(all, where, binds)
          rel.order(file_size: :desc)
             .limit(limit)
             .pluck(:path, :file_size)
             .map { |path, size| { "path" => path, "file_size" => size } }
        end

        def query_rows(where: nil, binds: [], order_by: nil, limit: nil)
          rel = apply_where(all, where, binds)
          rel = rel.order(Arel.sql(order_by)) if order_by && !order_by.empty?
          rel = rel.limit(limit) if limit
          rel.as_json
        end

        def intraday_active_roots(date: nil)
          target_date = date || Time.now.utc.strftime("%Y-%m-%d")
          where(dataset_type: "options")
            .where("substr(last_observed_at, 1, 10) = ?", target_date)
            .distinct
            .order(:provider_name, :ticker)
            .pluck(:provider_name, :ticker)
            .map { |provider, root| { provider_name: provider, root: root } }
        end

        def intraday_series_rows(provider_name:, root:, date: nil)
          target_date = date || Time.now.utc.strftime("%Y-%m-%d")
          where(dataset_type: "options", provider_name: provider_name, ticker: root)
            .where("substr(last_observed_at, 1, 10) = ?", target_date)
            .order(:expiration_date, :last_observed_at)
            .map do |record|
              {
                "provider_name" => record.provider_name,
                "root" => record.ticker,
                "collection_id" => record.collection_id,
                "sample_date" => record.last_observed_at ? record.last_observed_at[0, 10] : nil,
                "sampled_at" => record.last_observed_at,
                "expiration_date" => record.expiration_date,
                "path" => record.path,
                "row_count" => record.row_count,
                "file_size" => record.file_size,
                "updated_at" => record.updated_at
              }
            end
        end

        def intraday_index_rows(provider_name:, root:, date: nil)
          target_date = date || Time.now.utc.strftime("%Y-%m-%d")
          subquery = where(dataset_type: "options", provider_name: provider_name, ticker: root)
                     .where("substr(last_observed_at, 1, 10) = ?", target_date)
                     .group(:expiration_date)
                     .select("expiration_date, MAX(last_observed_at) AS latest_observed_at")

          joins("INNER JOIN (#{subquery.to_sql}) latest ON file_metadata_cache.expiration_date = latest.expiration_date AND file_metadata_cache.last_observed_at = latest.latest_observed_at")
            .where(dataset_type: "options", provider_name: provider_name, ticker: root)
            .where("substr(file_metadata_cache.last_observed_at, 1, 10) = ?", target_date)
            .order("file_metadata_cache.expiration_date")
            .map do |record|
              {
                "provider_name" => record.provider_name,
                "root" => record.ticker,
                "collection_id" => record.collection_id,
                "sample_date" => record.last_observed_at ? record.last_observed_at[0, 10] : nil,
                "sampled_at" => record.last_observed_at,
                "expiration_date" => record.expiration_date,
                "path" => record.path,
                "row_count" => record.row_count,
                "file_size" => record.file_size,
                "updated_at" => record.updated_at
              }
            end
        end

        private

        def apply_where(rel, where_clause, binds)
          return rel if where_clause.nil? || where_clause.empty?

          if binds.empty?
            rel.where(Arel.sql(where_clause))
          else
            rel.where(where_clause, *binds)
          end
        end
      end
    end
  end
end
