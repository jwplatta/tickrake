# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class LogsTool < MCP::Tool
      DEFAULT_TARGET = "cli"
      DEFAULT_TAIL = 10

      description "Tail the Tickrake CLI log or any configured job log."

      input_schema(
        properties: {
          target: {
            type: "string",
            description: "Log target to read. Use `cli` or a configured job name."
          },
          tail: {
            type: "integer",
            description: "Number of lines to read from the end of the file. Defaults to 10."
          }
        }
      )

      annotations(
        title: "Read Tickrake Logs",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(target: DEFAULT_TARGET, tail: DEFAULT_TAIL, server_context:)
          log_path = Tickrake::PathSupport.named_log_path(target)
          return Response.text("No log file at #{log_path}") unless File.exist?(log_path)

          content = File.read(log_path)
          content = content.lines.last(tail).join

          Response.text(content)
        end
      end
    end
  end
end
