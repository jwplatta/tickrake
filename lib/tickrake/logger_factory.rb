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

      text_devices = [Logger.new(log_path, LOG_ROTATION_COUNT, LOG_ROTATION_SIZE)]
      text_devices << stdout if verbose || ENV["TICKRAKE_LOG_STDOUT"] == "1"

      text_logger = Logger.new(MultiIO.new(*text_devices))
      text_logger.level = Logger::INFO
      text_logger.formatter = text_formatter

      unless ENV["TICKRAKE_LOG_FORMAT"] == "json"
        return text_logger
      end

      jsonl_path = log_path.sub(/\.log\z/, ".jsonl")
      Tickrake::LogRetention.new(log_path: jsonl_path, retention_days: LOG_RETENTION_DAYS).prune!
      json_logger = Logger.new(jsonl_path, LOG_ROTATION_COUNT, LOG_ROTATION_SIZE)
      json_logger.level = Logger::INFO
      json_logger.formatter = json_formatter(context)
      strip_log_header(jsonl_path)

      DualLogger.new(text_logger, json_logger)
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

    def self.strip_log_header(path)
      return unless File.exist?(path)

      content = File.read(path)
      return unless content.start_with?("# Logfile created on")

      first_newline = content.index("\n")
      File.write(path, first_newline ? content[(first_newline + 1)..] : "")
    end

    private_class_method :text_formatter, :json_formatter, :strip_log_header

    class DualLogger
      def initialize(text_logger, json_logger)
        @text_logger = text_logger
        @json_logger = json_logger
      end

      %i[debug info warn error fatal unknown].each do |level|
        define_method(level) do |message = nil, &block|
          message = block.call if block && message.nil?
          text_message = message.is_a?(Hash) ? message[:msg] || message.to_s : message
          @text_logger.send(level, text_message)
          @json_logger.send(level, message)
        end
      end

      def level
        @text_logger.level
      end

      def level=(val)
        @text_logger.level = val
        @json_logger.level = val
      end

      def close
        @text_logger.close
        @json_logger.close
      end
    end

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
