# frozen_string_literal: true

require "monitor"

module Tickrake
  class EventsWriter
    def initialize(pending_events_dir:, job_name:, rotation_interval_seconds:, logger:, rotation_size_bytes: nil)
      @pending_events_dir = pending_events_dir
      @job_name = job_name
      @rotation_interval_seconds = rotation_interval_seconds
      @rotation_size_bytes = rotation_size_bytes
      @logger = logger
      @lock = Monitor.new
      @current_file = nil
      @current_path = nil
      @current_opened_at = nil
      @current_event_count = 0
    end

    def write(event_hash)
      @lock.synchronize do
        maybe_rotate
        @current_file.puts(JSON.generate(event_hash))
        @current_file.flush
        @current_event_count += 1
      end
    end

    def close
      @lock.synchronize do
        finalize_current_file
      end
    end

    def recover_stale_files
      threshold = @rotation_interval_seconds * 2
      Dir.glob(File.join(@pending_events_dir, "#{@job_name}_*.ndjson.tmp")).each do |path|
        age = Time.now - File.mtime(path)
        next unless age >= threshold

        final_path = path.sub(/\.tmp$/, "")
        File.rename(path, final_path)
        @logger.warn("events_writer: recovered stale file #{File.basename(final_path)}")
      end
    end

    private

    def maybe_rotate
      if @current_file.nil?
        open_new_file
      elsif should_rotate?
        finalize_current_file
        open_new_file
      end
    end

    def should_rotate?
      if @rotation_size_bytes && @current_file.pos >= @rotation_size_bytes
        true
      elsif (Time.now - @current_opened_at) >= @rotation_interval_seconds
        true
      else
        false
      end
    end

    def open_new_file
      FileUtils.mkdir_p(@pending_events_dir)
      now = Time.now.utc
      base_filename = "#{@job_name}_#{now.strftime('%Y%m%dT%H%M%SZ')}"
      filename = "#{base_filename}.ndjson.tmp"
      if File.exist?(File.join(@pending_events_dir, filename)) || File.exist?(File.join(@pending_events_dir, "#{base_filename}.ndjson"))
        filename = "#{@job_name}_#{now.strftime('%Y%m%dT%H%M%S')}_#{now.nsec}.ndjson.tmp"
      end
      @current_path = File.join(@pending_events_dir, filename)
      @current_file = File.open(@current_path, "a")
      @current_opened_at = Time.now
      @current_event_count = 0
    end

    def finalize_current_file
      return unless @current_file

      @current_file.close
      final_path = @current_path.sub(/\.tmp$/, "")
      File.rename(@current_path, final_path)
      @logger.info({ msg: "file_rotated", event: "file_rotated", file: File.basename(final_path), event_count: @current_event_count })
      @current_file = nil
      @current_path = nil
      @current_opened_at = nil
      @current_event_count = 0
    end
  end
end
