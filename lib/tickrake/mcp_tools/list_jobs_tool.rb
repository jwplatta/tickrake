# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    class ListJobsTool < MCP::Tool
      description "List configured Tickrake jobs with their type, current state, and log path."

      input_schema(
        properties: {
          config_path: {
            type: "string",
            description: "Optional config path override."
          }
        },
        required: []
      )

      annotations(
        title: "List Tickrake Jobs",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(config_path: nil, server_context:)
          config = Tickrake::ConfigLoader.load(config_path || Tickrake::PathSupport.config_path)
          registry = Tickrake::JobRegistry.new
          statuses = registry.statuses(config.jobs.map(&:name)).to_h { |status| [status.fetch(:name), status] }

          Response.text(JSON.pretty_generate(
            "jobs" => config.jobs.map do |job|
              status = statuses.fetch(job.name, { state: "stopped" })
              {
                "name" => job.name,
                "type" => job.type,
                "state" => status.fetch(:state, "stopped"),
                "log_path" => status[:log_path] || Tickrake::PathSupport.named_log_path(job.name)
              }
            end
          ))
        end
      end
    end
  end
end
