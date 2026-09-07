# frozen_string_literal: true

module Tickrake
  module DSL
  end
end

require_relative "dsl/numeric_duration"
require_relative "dsl/schedule_builder"
require_relative "dsl/options_builder"
require_relative "dsl/candles_builder"
require_relative "dsl/maintenance_builder"
require_relative "dsl/universe_builder"
require_relative "dsl/job_builder"

module Tickrake
  def self.config
    @config ||= Tickrake::ConfigLoader.load(Tickrake::PathSupport.config_path)
  end

  def self.job(name, &block)
    builder = Tickrake::DSL::JobBuilder.new(name)
    builder.instance_eval(&block)
    job_config = builder.build!(config)
    runtime = Tickrake::Runtime.new(
      config: config,
      provider_name: job_config.provider,
      log_path: Tickrake::PathSupport.named_log_path(name),
      config_path: Tickrake::PathSupport.config_path
    )
    Tickrake::JobRunner.run(runtime, job_config, from_config_start: false, restart: false)
  end
end
