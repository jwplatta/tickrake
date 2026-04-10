# frozen_string_literal: true

module Tickrake
  class LoggerFactory
    LOG_ROTATION_COUNT = 10
    LOG_ROTATION_SIZE = 10 * 1024 * 1024

    def self.build(verbose:, stdout:, log_path: Tickrake::PathSupport.cli_log_path)
      FileUtils.mkdir_p(File.dirname(log_path))

      devices = [Logger.new(log_path, LOG_ROTATION_COUNT, LOG_ROTATION_SIZE)]
      devices << stdout if verbose

      logger = Logger.new(MultiIO.new(*devices))
      logger.level = Logger::INFO
      logger.formatter = proc do |severity, datetime, _progname, message|
        "[#{datetime.utc.iso8601}] #{severity} #{message}\n"
      end
      logger
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
