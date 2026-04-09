# frozen_string_literal: true

module Tickrake
  class Error < StandardError; end
  class ConfigError < Error; end
  class LockError < Error; end
end
