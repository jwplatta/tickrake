# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class HelpTool < MCP::Tool
      description "Describe the Tickrake MCP server and the tools it exposes."

      input_schema(
        properties: {},
        required: []
      )

      annotations(
        title: "Tickrake MCP Help",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(server_context:)
          Response.text(<<~TEXT)
            Tickrake MCP exposes Tickrake config, dataset discovery, scheduler control, storage stats, and log inspection over stdio MCP.

            Available tools:
            - help_tool: show this help text
            - validate_config_tool: validate a Tickrake config file and summarize the active storage paths
            - status_tool: show options and candles scheduler status
            - search_datasets_tool: list stored candle files and option snapshots without returning row data
            - storage_stats_tool: summarize history, options, SQLite, and log storage usage
            - logs_tool: tail the Tickrake CLI, options, or candles log
            - start_job_tool: start the options job, candles job, or both
            - stop_job_tool: stop the options job, candles job, or both
            - restart_job_tool: restart the options job, candles job, or both
          TEXT
        end
      end
    end
  end
end
