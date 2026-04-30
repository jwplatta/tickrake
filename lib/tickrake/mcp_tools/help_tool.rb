# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class HelpTool < MCP::Tool
      description "Describe the Tickrake MCP server and the tools it exposes."

      input_schema(
        properties: {}
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
            Tickrake MCP exposes Tickrake config, job discovery, dataset discovery, scheduler control, storage stats, and log inspection over stdio MCP.

            Available tools:
            - help_tool: show this help text
            - validate_config_tool: validate a Tickrake config file and summarize the active storage paths
            - list_jobs_tool: return configured job names, types, states, and log paths as structured JSON
            - status_tool: show human-readable status for configured jobs and any orphaned job state
            - search_datasets_tool: list stored candle files and option snapshots without returning row data
            - storage_stats_tool: summarize history, options, SQLite, and log storage usage
            - logs_tool: tail the Tickrake CLI log or a configured job log
            - start_job_tool: start a configured job or all configured jobs
            - stop_job_tool: stop a configured job or all configured jobs
            - restart_job_tool: restart a configured job or all configured jobs
          TEXT
        end
      end
    end
  end
end
