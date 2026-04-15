# frozen_string_literal: true

require "mcp"
require "mcp/server/transports/stdio_transport"

module Tickrake
  class MCPServer
    TOOLS = [
      Tickrake::MCPTools::HelpTool,
      Tickrake::MCPTools::ValidateConfigTool,
      Tickrake::MCPTools::StatusTool,
      Tickrake::MCPTools::SearchDatasetsTool,
      Tickrake::MCPTools::StorageStatsTool,
      Tickrake::MCPTools::LogsTool,
      Tickrake::MCPTools::StartJobTool,
      Tickrake::MCPTools::StopJobTool,
      Tickrake::MCPTools::RestartJobTool
    ].freeze

    def initialize
      @server = MCP::Server.new(
        name: "tickrake_mcp_server",
        version: Tickrake::VERSION,
        server_context: {},
        tools: TOOLS
      )
    end

    def start
      configure_mcp
      stdio_transport_class.new(@server).open
    end

    private

    def configure_mcp
      MCP.configure do |config|
        config.exception_reporter = lambda do |exception, server_context|
          warn("tickrake_mcp error: #{exception.class}: #{exception.message}")
        end
      end
    end

    def stdio_transport_class
      if defined?(MCP::Server::Transports::StdioTransport)
        MCP::Server::Transports::StdioTransport
      else
        MCP::Transports::StdioTransport
      end
    end
  end
end
