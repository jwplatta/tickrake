# frozen_string_literal: true

require "fileutils"

module Tickrake
  module Index
    class WriteLock
      def initialize(provider:, root:, locks_dir: nil)
        locks_dir ||= File.join(Tickrake::PathSupport.home_dir, "locks")
        FileUtils.mkdir_p(locks_dir)
        name = "index-options-#{provider}-#{root}".gsub(/[^a-zA-Z0-9\-]/, "_")
        @path = File.join(locks_dir, "#{name}.lock")
      end

      def synchronize
        File.open(@path, File::RDWR | File::CREAT, 0o644) do |file|
          file.flock(File::LOCK_EX)
          yield
        ensure
          file.flock(File::LOCK_UN)
        end
      end
    end
  end
end
