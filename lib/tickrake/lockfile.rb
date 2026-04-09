# frozen_string_literal: true

module Tickrake
  class Lockfile
    def initialize(name)
      @path = Tickrake::PathSupport.lock_path(name)
      FileUtils.mkdir_p(File.dirname(@path))
    end

    def synchronize
      File.open(@path, File::RDWR | File::CREAT, 0o644) do |file|
        raise LockError, "Another process already holds #{@path}" unless file.flock(File::LOCK_EX | File::LOCK_NB)

        yield
      ensure
        file.flock(File::LOCK_UN)
      end
    end
  end
end
