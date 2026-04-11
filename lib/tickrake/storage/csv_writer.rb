# frozen_string_literal: true

module Tickrake
  module Storage
    class CsvWriter
      def write(path, headers:, rows:)
        directory = File.dirname(path)
        FileUtils.mkdir_p(directory)
        tmp_path = "#{path}.tmp"

        CSV.open(tmp_path, "wb") do |csv|
          csv << headers
          rows.each { |row| csv << row }
        end

        File.rename(tmp_path, path)
        path
      ensure
        File.delete(tmp_path) if defined?(tmp_path) && File.exist?(tmp_path)
      end
    end
  end
end
