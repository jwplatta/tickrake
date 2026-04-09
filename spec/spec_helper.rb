# frozen_string_literal: true

require_relative "../lib/tickrake"
require "tmpdir"

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.expect_with :rspec do |expectations|
    expectations.syntax = :expect
  end
end
