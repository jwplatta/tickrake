# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class StatusTool < MCP::Tool
      description "Show the current status of the Tickrake options and candles schedulers."

      input_schema(
        properties: {},
        required: []
      )

      annotations(
        title: "Tickrake Job Status",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(server_context:)
          lines = Tickrake::JobRegistry.new.statuses.map do |job|
            case job[:state]
            when "running"
              "#{job[:name]}: running pid=#{job[:pid]} started_at=#{job[:started_at]} log=#{job[:log_path]}"
            when "stale"
              "#{job[:name]}: stale pid=#{job[:pid]} started_at=#{job[:started_at]}"
            else
              "#{job[:name]}: stopped"
            end
          end

          Response.text(lines.join("\n"))
        end
      end
    end
  end
end
