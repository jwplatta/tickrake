# frozen_string_literal: true

module Tickrake
  module Storage
    class DuckdbOptionCompactedWriter
      SCHEMA = (Tickrake::Storage::OptionSampleWriter::CSV_HEADERS.map { |name| [name, "VARCHAR"] } + [["sampled_at", "TIMESTAMP WITH TIME ZONE"]]).freeze

      Result = Struct.new(
        :csv_path,
        :parquet_path,
        :row_count,
        :first_sampled_at,
        :last_sampled_at,
        keyword_init: true
      )

      def write(raw_files:, csv_path:, parquet_path:, sampled_at_resolver:)
        raise Tickrake::Error, "DuckDB compaction requires at least one raw file." if raw_files.empty?

        FileUtils.mkdir_p(File.dirname(csv_path))
        FileUtils.mkdir_p(File.dirname(parquet_path))

        DuckDB::Database.open do |db|
          db.connect do |con|
            create_table(con)
            raw_files.each do |raw_file|
              insert_raw_file(con, raw_file, sampled_at_resolver.call(raw_file))
            end

            export(con, csv_path, "csv")
            export(con, parquet_path, "parquet")

            aggregate = con.query(<<~SQL).first
              SELECT COUNT(*) AS row_count, MIN(sampled_at) AS first_sampled_at, MAX(sampled_at) AS last_sampled_at
              FROM compacted_option_samples
            SQL

            Result.new(
              csv_path: csv_path,
              parquet_path: parquet_path,
              row_count: aggregate[0].to_i,
              first_sampled_at: aggregate[1],
              last_sampled_at: aggregate[2]
            )
          end
        end
      end

      private

      def create_table(con)
        con.query(<<~SQL)
          CREATE TEMP TABLE compacted_option_samples (
            #{SCHEMA.map { |name, type| "#{name} #{type}" }.join(", ")}
          )
        SQL
      end

      def insert_raw_file(con, raw_file, sampled_at)
        con.query(<<~SQL)
          INSERT INTO compacted_option_samples
          SELECT
            contract_type,
            symbol,
            description,
            strike,
            expiration_date,
            open,
            high,
            low,
            close,
            mark,
            bid,
            bid_size,
            ask,
            ask_size,
            last,
            last_size,
            open_interest,
            total_volume,
            transactions,
            delta,
            gamma,
            theta,
            vega,
            rho,
            volatility,
            theoretical_volatility,
            theoretical_option_value,
            intrinsic_value,
            extrinsic_value,
            underlying_price,
            TIMESTAMPTZ #{sql_string(sampled_at.utc.iso8601)}
          FROM read_csv_auto(
            #{sql_string(raw_file)},
            header = true,
            all_varchar = true
          )
        SQL
      end

      def export(con, path, format)
        select_list = format == "csv" ? csv_select_list : parquet_select_list

        con.query(<<~SQL)
          COPY (
            SELECT #{select_list}
            FROM compacted_option_samples
            ORDER BY sampled_at, expiration_date, contract_type, strike, symbol
          ) TO #{sql_string(path)} (FORMAT #{format.upcase})
        SQL
      end

      def csv_select_list
        (
          Tickrake::Storage::OptionSampleWriter::CSV_HEADERS +
          ["strftime(timezone('UTC', sampled_at), '%Y-%m-%dT%H:%M:%SZ') AS sampled_at"]
        ).join(", ")
      end

      def parquet_select_list
        [
          "contract_type",
          "symbol",
          "description",
          "TRY_CAST(strike AS DOUBLE) AS strike",
          "TRY_CAST(expiration_date AS DATE) AS expiration_date",
          "TRY_CAST(open AS DOUBLE) AS open",
          "TRY_CAST(high AS DOUBLE) AS high",
          "TRY_CAST(low AS DOUBLE) AS low",
          "TRY_CAST(close AS DOUBLE) AS close",
          "TRY_CAST(mark AS DOUBLE) AS mark",
          "TRY_CAST(bid AS DOUBLE) AS bid",
          "TRY_CAST(bid_size AS BIGINT) AS bid_size",
          "TRY_CAST(ask AS DOUBLE) AS ask",
          "TRY_CAST(ask_size AS BIGINT) AS ask_size",
          "TRY_CAST(last AS DOUBLE) AS last",
          "TRY_CAST(last_size AS DOUBLE) AS last_size",
          "TRY_CAST(open_interest AS DOUBLE) AS open_interest",
          "TRY_CAST(total_volume AS DOUBLE) AS total_volume",
          "TRY_CAST(transactions AS BIGINT) AS transactions",
          "TRY_CAST(delta AS DOUBLE) AS delta",
          "TRY_CAST(gamma AS DOUBLE) AS gamma",
          "TRY_CAST(theta AS DOUBLE) AS theta",
          "TRY_CAST(vega AS DOUBLE) AS vega",
          "TRY_CAST(rho AS DOUBLE) AS rho",
          "TRY_CAST(volatility AS DOUBLE) AS volatility",
          "TRY_CAST(theoretical_volatility AS DOUBLE) AS theoretical_volatility",
          "TRY_CAST(theoretical_option_value AS DOUBLE) AS theoretical_option_value",
          "TRY_CAST(intrinsic_value AS DOUBLE) AS intrinsic_value",
          "TRY_CAST(extrinsic_value AS DOUBLE) AS extrinsic_value",
          "TRY_CAST(underlying_price AS DOUBLE) AS underlying_price",
          "CAST(sampled_at AS TIMESTAMP WITH TIME ZONE) AS sampled_at"
        ].join(", ")
      end

      def sql_string(value)
        "'#{value.to_s.gsub("'", "''")}'"
      end
    end
  end
end
