# frozen_string_literal: true

require "find"

module Tickrake
  module Storage
    class StatsReport
      DEFAULT_LARGEST_FILES_LIMIT = 5

      def initialize(config, log_paths: nil, largest_files_limit: DEFAULT_LARGEST_FILES_LIMIT)
        @config = config
        @log_paths = log_paths || default_log_paths
        @largest_files_limit = largest_files_limit
      end

      def render
        data_summary = combine_summaries(
          scan_dir(@config.history_dir),
          scan_dir(@config.options_dir)
        )
        history_summary = scan_dir(@config.history_dir)
        options_summary = scan_dir(@config.options_dir)
        history_providers = provider_summaries(@config.history_dir)
        options_providers = provider_summaries(@config.options_dir)
        sqlite_summary = file_summary(@config.sqlite_path)
        log_summaries = @log_paths.to_h { |name, path| [name, file_summary(path)] }

        lines = []
        lines << "Storage stats for #{@config.data_dir}"
        lines << "Data files: #{data_summary[:file_count]} files using #{format_bytes(data_summary[:total_bytes])}"
        lines << "Provider folders with data: #{provider_count(history_providers, options_providers)}"
        lines << ""
        lines.concat(dataset_lines("History", @config.history_dir, history_summary, history_providers))
        lines << ""
        lines.concat(dataset_lines("Options", @config.options_dir, options_summary, options_providers))
        lines << ""
        lines << "Metadata"
        lines << "  SQLite: #{file_line(sqlite_summary)}"
        lines << "  Logs: #{format_bytes(log_summaries.values.sum { |summary| summary[:total_bytes] })} across #{log_summaries.length} files"
        log_summaries.each do |name, summary|
          lines << "    #{name}: #{file_line(summary)}"
        end
        lines.join("\n")
      end

      private

      def dataset_lines(label, path, summary, providers)
        lines = []
        lines << "#{label} (#{path})"
        if !summary[:exists]
          lines << "  missing"
          return lines
        end

        lines << "  #{summary[:file_count]} files in #{providers.length} provider folders using #{format_bytes(summary[:total_bytes])}"
        lines << "  average file size: #{format_bytes(summary[:average_file_size])}"
        lines << "  oldest file: #{format_timestamp(summary[:oldest_mtime])}"
        lines << "  newest file: #{format_timestamp(summary[:newest_mtime])}"

        if providers.empty?
          lines << "  per provider: none"
        else
          lines << "  per provider:"
          providers.each do |provider|
            lines << "    #{provider[:name]}: #{provider[:summary][:file_count]} files, #{format_bytes(provider[:summary][:total_bytes])}, newest #{format_timestamp(provider[:summary][:newest_mtime])}"
          end
        end

        largest_files = summary[:largest_files]
        if largest_files.empty?
          lines << "  largest files: none"
        else
          lines << "  largest files:"
          largest_files.each_with_index do |entry, index|
            lines << "    #{index + 1}. #{relative_display_path(path, entry[:path])} (#{format_bytes(entry[:size_bytes])})"
          end
        end
        lines
      end

      def provider_count(*provider_groups)
        provider_groups.flatten.count { |provider| provider[:summary][:file_count].positive? }
      end

      def provider_summaries(root_path)
        return [] unless Dir.exist?(root_path)

        Dir.children(root_path).sort.filter_map do |entry|
          next if entry.start_with?(".")

          path = File.join(root_path, entry)
          next unless File.directory?(path)

          { name: entry, path: path, summary: scan_dir(path) }
        end
      end

      def file_summary(path)
        expanded_path = File.expand_path(path.to_s)
        return missing_summary(expanded_path) unless File.file?(expanded_path)

        stat = File.stat(expanded_path)
        {
          exists: true,
          path: expanded_path,
          file_count: 1,
          total_bytes: stat.size,
          average_file_size: stat.size,
          oldest_mtime: stat.mtime,
          newest_mtime: stat.mtime,
          largest_files: [{ path: expanded_path, size_bytes: stat.size }]
        }
      end

      def scan_dir(path)
        expanded_path = File.expand_path(path.to_s)
        return missing_summary(expanded_path) unless Dir.exist?(expanded_path)

        files = []
        Find.find(expanded_path) do |candidate|
          next if candidate == expanded_path
          next unless File.file?(candidate)

          stat = File.stat(candidate)
          files << { path: candidate, size_bytes: stat.size, mtime: stat.mtime }
        end

        summarize_files(expanded_path, files)
      end

      def combine_summaries(*summaries)
        existing = summaries.select { |summary| summary[:exists] }
        {
          exists: existing.any?,
          path: @config.data_dir,
          file_count: summaries.sum { |summary| summary[:file_count] },
          total_bytes: summaries.sum { |summary| summary[:total_bytes] },
          average_file_size: average_file_size(summaries.sum { |summary| summary[:total_bytes] }, summaries.sum { |summary| summary[:file_count] }),
          oldest_mtime: existing.map { |summary| summary[:oldest_mtime] }.compact.min,
          newest_mtime: existing.map { |summary| summary[:newest_mtime] }.compact.max,
          largest_files: summaries.flat_map { |summary| summary[:largest_files] }.sort_by { |entry| -entry[:size_bytes] }.first(@largest_files_limit)
        }
      end

      def summarize_files(path, files)
        total_bytes = files.sum { |entry| entry[:size_bytes] }
        {
          exists: true,
          path: path,
          file_count: files.length,
          total_bytes: total_bytes,
          average_file_size: average_file_size(total_bytes, files.length),
          oldest_mtime: files.map { |entry| entry[:mtime] }.min,
          newest_mtime: files.map { |entry| entry[:mtime] }.max,
          largest_files: files.sort_by { |entry| -entry[:size_bytes] }.first(@largest_files_limit)
        }
      end

      def average_file_size(total_bytes, file_count)
        return 0 if file_count.zero?

        (total_bytes.to_f / file_count).round
      end

      def missing_summary(path)
        {
          exists: false,
          path: path,
          file_count: 0,
          total_bytes: 0,
          average_file_size: 0,
          oldest_mtime: nil,
          newest_mtime: nil,
          largest_files: []
        }
      end

      def file_line(summary)
        return "missing (#{summary[:path]})" unless summary[:exists]

        "#{format_bytes(summary[:total_bytes])} at #{summary[:path]}"
      end

      def relative_display_path(root_path, full_path)
        full_path.delete_prefix("#{File.expand_path(root_path)}/")
      end

      def format_timestamp(value)
        value ? value.iso8601 : "n/a"
      end

      def format_bytes(bytes)
        units = %w[B KB MB GB TB]
        value = bytes.to_f
        unit = units.shift
        while value >= 1024 && units.any?
          value /= 1024.0
          unit = units.shift
        end
        return "#{value.to_i} #{unit}" if value >= 10 || unit == "B"

        format("%.1f %s", value, unit)
      end

      def default_log_paths
        {
          cli: Tickrake::PathSupport.cli_log_path,
          options: Tickrake::PathSupport.options_log_path,
          candles: Tickrake::PathSupport.candles_log_path
        }
      end
    end
  end
end
