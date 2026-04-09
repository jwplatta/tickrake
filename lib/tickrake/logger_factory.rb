# frozen_string_literal: true

module Tickrake
  class LoggerFactory
    def self.build(verbose:, stdout:)
      log_path = Tickrake::PathSupport.log_path
      FileUtils.mkdir_p(File.dirname(log_path))

      devices = [File.open(log_path, "a")]
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
        @targets.each { |target| target.write(*args) }
      end

      def close
        @targets.each do |target|
          next if [STDOUT, STDERR, $stdout, $stderr].include?(target)

          target.close unless target.closed?
        end
      end

      def flush
        @targets.each(&:flush)
      end
    end
  end
end
