# frozen_string_literal: true

require "mcp"
require "stringio"

module Tickrake
  module MCPTools
    class StartJobTool < MCP::Tool
      description "Start a configured Tickrake scheduler job, or all configured jobs."

      input_schema(
        properties: {
          target: {
            type: "string",
            description: "Configured job name, or `all`."
          },
          config_path: {
            type: "string",
            description: "Optional config path to use when starting the job."
          },
          provider: {
            type: "string",
            description: "Optional provider override."
          },
          from_config_start: {
            type: "boolean",
            description: "For candles jobs only, force backfill from configured start_date."
          },
          restart: {
            type: "boolean",
            description: "Automatically restart the background scheduler if it exits unexpectedly."
          }
        },
        required: ["target"]
      )

      annotations(
        title: "Start Tickrake Job",
        read_only_hint: false,
        destructive_hint: true,
        idempotent_hint: false
      )

      class << self
        def call(target:, config_path: nil, provider: nil, from_config_start: false, restart: false, server_context:)
          stdout = StringIO.new
          Tickrake::JobControl.new(stdout: stdout).start(
            target: target,
            config_path: config_path || Tickrake::PathSupport.config_path,
            provider_name: provider,
            from_config_start: from_config_start,
            restart: restart
          )
          Response.text(stdout.string)
        end
      end
    end
  end
end
