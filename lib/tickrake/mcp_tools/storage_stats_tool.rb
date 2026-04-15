# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class StorageStatsTool < MCP::Tool
      description "Summarize Tickrake storage usage across history, options, SQLite, and logs."

      input_schema(
        properties: {
          config_path: {
            type: "string",
            description: "Optional path to a Tickrake config file."
          }
        },
        required: []
      )

      annotations(
        title: "Tickrake Storage Stats",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(config_path: nil, server_context:)
          config = Tickrake::ConfigLoader.load(config_path || Tickrake::PathSupport.config_path)
          Response.text(Tickrake::Storage::StatsReport.new(config).render)
        end
      end
    end
  end
end
