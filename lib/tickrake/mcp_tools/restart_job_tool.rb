# frozen_string_literal: true

require "mcp"
require "stringio"

module Tickrake
  module MCPTools
    class RestartJobTool < MCP::Tool
      description "Restart a configured Tickrake scheduler job, or all configured jobs."

      input_schema(
        properties: {
          target: {
            type: "string",
            description: "Configured job name, or `all`."
          },
          config_path: {
            type: "string",
            description: "Optional config path override."
          },
          provider: {
            type: "string",
            description: "Optional provider override."
          },
          from_config_start: {
            type: "boolean",
            description: "For candles jobs only, override whether restart should backfill from configured start_date."
          },
          restart: {
            type: "boolean",
            description: "Override whether the restarted background scheduler should auto-restart on unexpected exit."
          }
        },
        required: ["target"]
      )

      annotations(
        title: "Restart Tickrake Job",
        read_only_hint: false,
        destructive_hint: true,
        idempotent_hint: false
      )

      class << self
        def call(target:, config_path: nil, provider: nil, from_config_start: nil, restart: nil, server_context:)
          stdout = StringIO.new
          Tickrake::JobControl.new(stdout: stdout).restart(
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
