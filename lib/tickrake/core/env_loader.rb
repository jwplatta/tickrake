# frozen_string_literal: true

require "dotenv"

module Tickrake
  module Core
    module EnvLoader
      module_function

      # Resolves and loads environment variables using Dotenv.
      # Fallback precedence:
      # 1. explicit_path (e.g. from --env-file CLI option)
      # 2. ENV["TICKRAKE_ENV_FILE"]
      # 3. ./.env (working directory)
      # 4. ~/.tickrake/.env (home directory)
      #
      # Dotenv.load does not overwrite already-set environment variables.
      def load!(explicit_path = nil)
        candidate = resolve_path(explicit_path)
        if candidate && File.file?(candidate)
          Dotenv.load(candidate)
          candidate
        end
      end

      def resolve_path(explicit_path = nil)
        if explicit_path && !explicit_path.to_s.strip.empty?
          expanded = File.expand_path(explicit_path)
          raise Tickrake::ConfigError, "Environment file not found: #{expanded}" unless File.file?(expanded)

          return expanded
        end

        env_var_path = ENV["TICKRAKE_ENV_FILE"]
        if env_var_path && !env_var_path.strip.empty?
          expanded = File.expand_path(env_var_path)
          raise Tickrake::ConfigError, "Environment file specified by TICKRAKE_ENV_FILE not found: #{expanded}" unless File.file?(expanded)

          return expanded
        end

        working_dir_env = File.expand_path(".env", Dir.pwd)
        return working_dir_env if File.file?(working_dir_env)

        home_env = File.expand_path(".env", Tickrake::PathSupport.home_dir)
        return home_env if File.file?(home_env)

        nil
      end
    end
  end
end
