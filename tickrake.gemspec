# frozen_string_literal: true

require_relative "lib/tickrake/version"

Gem::Specification.new do |spec|
  spec.name = "tickrake"
  spec.version = Tickrake::VERSION
  spec.authors = ["OpenAI Codex"]
  spec.email = ["support@openai.com"]

  spec.summary = "Scheduled options and candle collection on top of schwab_rb"
  spec.description = "Tickrake schedules option-chain sampling and end-of-day candle collection."
  spec.homepage = "https://example.com/tickrake"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  spec.files = Dir.chdir(__dir__) do
    Dir.glob("config/**/*") +
      Dir.glob("exe/*") +
      Dir.glob("lib/**/*") +
      %w[README.md]
  end
  spec.bindir = "exe"
  spec.executables = ["tickrake"]
  spec.require_paths = ["lib"]

  spec.add_dependency "schwab_rb"
  spec.add_dependency "sqlite3", ">= 1.6"

  spec.add_development_dependency "rspec", "~> 3.13"
end
