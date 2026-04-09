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
