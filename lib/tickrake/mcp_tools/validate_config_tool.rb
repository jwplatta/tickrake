# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class ValidateConfigTool < MCP::Tool
      description "Validate a Tickrake config file and summarize the active provider and storage paths."

      input_schema(
        properties: {
          config_path: {
            type: "string",
            description: "Optional path to a Tickrake config file. Defaults to ~/.tickrake/tickrake.yml."
          }
        },
        required: []
      )

      annotations(
        title: "Validate Tickrake Config",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(config_path: nil, server_context:)
          path = config_path || Tickrake::PathSupport.config_path
          config = Tickrake::ConfigLoader.load(path)

          Response.text(<<~TEXT)
            Config valid: #{path}
            Default provider: #{config.default_provider_name}
            Providers: #{config.providers.keys.sort.join(", ")}
            Data dir: #{config.data_dir}
            SQLite: #{config.sqlite_path}
          TEXT
        end
      end
    end
  end
end
