# frozen_string_literal: true

require "json"
require "fileutils"

module Tickrake
  module Index
    class AtomicJsonWriter
      def write(target_path, payload)
        FileUtils.mkdir_p(File.dirname(target_path))
        tmp = "#{target_path}.tmp.#{Process.pid}"
        File.write(tmp, JSON.pretty_generate(payload))
        File.open(tmp) { |f| f.fsync }
        File.rename(tmp, target_path)
      ensure
        File.unlink(tmp) if tmp && File.exist?(tmp) && File.exist?(target_path)
      end
    end
  end
end
