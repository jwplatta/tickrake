# frozen_string_literal: true

module Tickrake
  class LogRetention
    DEFAULT_RETENTION_DAYS = 14

    def initialize(log_path:, retention_days: DEFAULT_RETENTION_DAYS, now: Time.now)
      @log_path = File.expand_path(log_path)
      @retention_days = retention_days.to_i
      @now = now
    end

    def prune!
      return [] if @retention_days <= 0

      cutoff = @now - (@retention_days * 86_400)
      pruned = []
      log_family_paths.each do |path|
        next unless File.file?(path)
        next unless File.mtime(path) < cutoff

        File.delete(path)
        pruned << path
      end
      pruned
    end

    private

    def log_family_paths
      Dir.glob("#{@log_path}*").select do |path|
        suffix = path.delete_prefix(@log_path)
        suffix.empty? || /\A\.\d+\z/.match?(suffix)
      end
    end
  end
end
