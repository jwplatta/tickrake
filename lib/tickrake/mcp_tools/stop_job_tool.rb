# frozen_string_literal: true

require "mcp"
require "stringio"

module Tickrake
  module MCPTools
    class StopJobTool < MCP::Tool
      description "Stop the Tickrake options scheduler, candles scheduler, or both."

      input_schema(
        properties: {
          target: {
            type: "string",
            description: "Which job to stop.",
            enum: %w[options candles all]
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
        def call(target:, server_context:)
          stdout = StringIO.new
          Tickrake::JobControl.new(stdout: stdout).stop(target: target)
          Response.text(stdout.string)
        end
      end
    end
  end
end
