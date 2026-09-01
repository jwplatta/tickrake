# frozen_string_literal: true

require_relative "../lib/tickrake"
require "tmpdir"

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.expect_with :rspec do |expectations|
    expectations.syntax = :expect
  end

  config.before(:each) do
    @_tickrake_tmpdir = Dir.mktmpdir
    allow(Tickrake::PathSupport).to receive(:home_dir).and_return(@_tickrake_tmpdir)
  end

  config.after(:each) do
    FileUtils.rm_rf(@_tickrake_tmpdir) if @_tickrake_tmpdir
  end
end

module TrackerSpecAutoMigrate
  def initialize(path, migrate: true)
    super(path, migrate: migrate)
  end
end

Tickrake::Tracker.prepend(TrackerSpecAutoMigrate)
