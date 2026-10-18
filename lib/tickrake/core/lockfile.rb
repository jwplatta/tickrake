# frozen_string_literal: true

module Tickrake
  class Lockfile
    def initialize(name)
      @path = Tickrake::PathSupport.lock_path(name)
      FileUtils.mkdir_p(File.dirname(@path))
    end

    def synchronize
      acquired = false
      result = try_synchronize do
        acquired = true
        yield
      end
      return result if acquired

      raise LockError, "Another process already holds #{@path}"
    end

    def try_synchronize
      File.open(@path, File::RDWR | File::CREAT, 0o644) do |file|
        return false unless file.flock(File::LOCK_EX | File::LOCK_NB)

        yield
      ensure
        file.flock(File::LOCK_UN)
      end
    end
  end
end
