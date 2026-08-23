# frozen_string_literal: true

module Tickrake
  module Index
    class UriBuilder
      def self.build(path:, storage_location:, remote_uri:)
        if remote_uri && storage_location != "local"
          remote_uri
        else
          "file://#{path}"
        end
      end
    end
  end
end
