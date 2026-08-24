# frozen_string_literal: true

module Tickrake
  module Index
    class UriBuilder
      def self.build(path:, storage_location:, remote_uri:)
        return remote_uri if remote_uri

        "file://#{path}"
      end
    end
  end
end
