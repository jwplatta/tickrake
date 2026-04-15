# frozen_string_literal: true

require "mcp"
require "stringio"

module Tickrake
  module MCPTools
    class RestartJobTool < MCP::Tool
      description "Restart the Tickrake options scheduler, candles scheduler, or both."

      input_schema(
        properties: {
          target: {
            type: "string",
            description: "Which job to restart.",
            enum: %w[options candles all]
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
            description: "For candles only, override whether restart should backfill from configured start_date."
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
        def call(target:, config_path: nil, provider: nil, from_config_start: nil, server_context:)
          stdout = StringIO.new
          Tickrake::JobControl.new(stdout: stdout).restart(
            target: target,
            config_path: config_path || Tickrake::PathSupport.config_path,
            provider_name: provider,
            from_config_start: from_config_start
          )
          Response.text(stdout.string)
        end
      end
    end
  end
end
