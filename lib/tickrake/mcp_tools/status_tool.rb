# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class StatusTool < MCP::Tool
      description "Show the current status of configured Tickrake jobs and any orphaned registry entries."

      input_schema(
        properties: {
          config_path: {
            type: "string",
            description: "Optional config path override."
          }
        }
      )

      annotations(
        title: "Tickrake Job Status",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(config_path: nil, server_context:)
          config = Tickrake::ConfigLoader.load(config_path || Tickrake::PathSupport.config_path)
          registry = Tickrake::JobRegistry.new
          names = (config.jobs.map(&:name) + registry.registered_names).uniq.sort

          lines = registry.statuses(names).map do |job|
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
