# frozen_string_literal: true

module Tickrake
  module PathSupport
    module_function

    def home_dir
      expand_path("~/.tickrake")
    end

    def config_path
      File.join(home_dir, "tickrake.yml")
    end

    def sqlite_path
      File.join(home_dir, "tickrake.sqlite3")
    end

    def cli_log_path
      File.join(home_dir, "cli.log")
    end

    def options_log_path
      File.join(home_dir, "options.log")
    end

    def candles_log_path
      File.join(home_dir, "candles.log")
    end

    def named_log_path(name)
      return cli_log_path if name.to_s == "cli"

      File.join(home_dir, "#{sanitize_symbol(name)}.log")
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
