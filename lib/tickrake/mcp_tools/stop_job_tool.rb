# frozen_string_literal: true

require "mcp"
require "stringio"

module Tickrake
  module MCPTools
    class StopJobTool < MCP::Tool
      description "Stop a configured Tickrake scheduler job, or all configured jobs."

      input_schema(
        properties: {
          target: {
            type: "string",
            description: "Configured job name, or `all`."
          },
          config_path: {
            type: "string",
            description: "Optional config path override."
          }
        },
        required: ["target"]
      )

      annotations(
        title: "Stop Tickrake Job",
        read_only_hint: false,
        destructive_hint: true,
        idempotent_hint: false
      )

      class << self
        def call(target:, config_path: nil, server_context:)
          stdout = StringIO.new
          Tickrake::JobControl.new(stdout: stdout).stop(
            target: target,
            config_path: config_path || Tickrake::PathSupport.config_path
          )
          Response.text(stdout.string)
        end
      end
    end
  end
end
