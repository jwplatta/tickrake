# frozen_string_literal: true

require "mcp"

module Tickrake
  module MCPTools
    module Response
      module_function

      def text(body)
        MCP::Tool::Response.new([{ type: "text", text: body }])
      end
    end
  end
end
