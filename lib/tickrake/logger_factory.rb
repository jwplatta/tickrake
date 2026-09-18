# frozen_string_literal: true

require "json"

module Tickrake
  class LoggerFactory
    LOG_ROTATION_COUNT = 5
    LOG_ROTATION_SIZE = 10 * 1024 * 1024
    LOG_RETENTION_DAYS = 14

    def self.build(verbose:, stdout:, log_path: Tickrake::PathSupport.cli_log_path, context: {})
      FileUtils.mkdir_p(File.dirname(log_path))
      Tickrake::LogRetention.new(log_path: log_path, retention_days: LOG_RETENTION_DAYS).prune!

      devices = [Logger.new(log_path, LOG_ROTATION_COUNT, LOG_ROTATION_SIZE)]
      devices << stdout if verbose || ENV["TICKRAKE_LOG_STDOUT"] == "1"

      logger = Logger.new(MultiIO.new(*devices))
      logger.level = Logger::INFO
      logger.formatter = build_formatter(context)
      logger
    end

    def self.build_formatter(context)
      if ENV["TICKRAKE_LOG_FORMAT"] == "json"
        json_formatter(context)
      else
        text_formatter
      end
    end

    def self.text_formatter
      proc do |severity, datetime, _progname, message|
        "[#{datetime.utc.iso8601}] #{severity} #{message}\n"
      end
    end

    def self.json_formatter(context)
      base = context.reject { |_, v| v.nil? }
      proc do |severity, datetime, _progname, message|
        entry = { ts: datetime.utc.iso8601, level: severity }.merge(base)
        if message.is_a?(Hash)
          entry.merge!(message)
        else
          entry[:msg] = message.to_s
        end
        "#{JSON.generate(entry)}\n"
      end
    end

    private_class_method :build_formatter, :text_formatter, :json_formatter

    class MultiIO
      def initialize(*targets)
        @targets = targets
      end

      def write(*args)
        @targets.each do |target|
          if target.is_a?(Logger)
            target << args.join
          else
            target.write(*args)
          end
        end
      end

      def close
        @targets.each do |target|
          next if [STDOUT, STDERR, $stdout, $stderr].include?(target)

          if target.is_a?(Logger)
            target.close
          else
            target.close unless target.closed?
          end
        end
      end

      def flush
        @targets.each do |target|
          target.flush if target.respond_to?(:flush)
        end
      end
    end
  end
end
