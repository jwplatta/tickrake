# frozen_string_literal: true

module Tickrake
  module PathSupport
    module_function

    def home_dir
      expand_path(ENV.fetch("TICKRAKE_HOME", "~/.tickrake"))
    end

    def config_path
      File.join(home_dir, "tickrake.yml")
    end

    def sqlite_path
      File.join(home_dir, "tickrake.sqlite3")
    end

    def cli_log_path
      File.join(logs_dir, "cli.log")
    end

    def options_log_path
      File.join(logs_dir, "options.log")
    end

    def candles_log_path
      File.join(logs_dir, "candles.log")
    end

    def named_log_path(name)
      return cli_log_path if name.to_s == "cli"

      File.join(logs_dir, "#{sanitize_symbol(name)}.log")
    end

    def logs_dir
      File.join(home_dir, "logs")
    end

    def staging_dir
      File.join(home_dir, "staging")
    end

    def default_pending_metadata_dir(data_dir: nil)
      if data_dir
        legacy = File.join(expand_path(data_dir), "pending_metadata")
        return legacy if Dir.exist?(legacy) && !Dir.empty?(legacy)
      end
      File.join(staging_dir, "pending_metadata")
    end

    def jobs_dir
      File.join(home_dir, "jobs")
    end

    def job_state_path(name)
      File.join(jobs_dir, "#{name}.json")
    end

    def lock_path(name)
      File.join(home_dir, "#{name}.lock")
    end

    def expand_path(path)
      File.expand_path(path.to_s)
    end

    def sanitize_symbol(symbol)
      cleaned = symbol.to_s.gsub(/[^a-zA-Z0-9]+/, "_").gsub(/\A_+|_+\z/, "")
      cleaned.empty? ? "symbol" : cleaned
    end
  end
end
