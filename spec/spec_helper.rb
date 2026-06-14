# frozen_string_literal: true

require_relative "../lib/tickrake"
require "tmpdir"

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.expect_with :rspec do |expectations|
    expectations.syntax = :expect
  end
end

module TrackerSpecAutoMigrate
  def initialize(path, migrate: true)
    super(path, migrate: migrate)
  end
end

Tickrake::Tracker.prepend(TrackerSpecAutoMigrate)
