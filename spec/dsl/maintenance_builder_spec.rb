# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::MaintenanceBuilder do
  subject(:builder) { described_class.new }

  describe "#compact" do
    it "adds a compact MaintenanceStepConfig" do
      builder.compact(:option_samples, universe: "spx_symbols", delete_sources: true)
      tasks = builder.build!
      expect(tasks.length).to eq(1)
      step = tasks.first
      expect(step).to be_a(Tickrake::MaintenanceStepConfig)
      expect(step.action).to eq("compact")
      expect(step.subject).to eq("option_samples")
      expect(step.universe).to eq("spx_symbols")
      expect(step.universes).to eq([])
      expect(step.delete_sources).to be(true)
      expect(step.destination).to be_nil
    end

    it "supports universes: array" do
      builder.compact(:option_samples, universes: %w[stock_option_symbols etf_option_symbols], delete_sources: true)
      step = builder.build!.first
      expect(step.universe).to be_nil
      expect(step.universes).to eq(%w[stock_option_symbols etf_option_symbols])
    end
  end

  describe "#archive" do
    it "adds an archive MaintenanceStepConfig" do
      builder.archive(:option_samples, universe: "spx_symbols",
                      to: :s3_archive, artifacts: %i[csv parquet],
                      retain: { parquet: true })
      tasks = builder.build!
      expect(tasks.length).to eq(1)
      step = tasks.first
      expect(step.action).to eq("archive")
      expect(step.subject).to eq("option_samples")
      expect(step.destination).to eq("s3_archive")
      expect(step.artifacts).to eq(%w[csv parquet])
      expect(step.retain_local).to eq("csv" => false, "parquet" => true)
      expect(step.delete_sources).to be(false)
    end

    it "defaults all retain_local values to false when retain is empty" do
      builder.archive(:option_samples, universe: "spx_symbols",
                      to: :s3_archive, artifacts: %i[csv parquet])
      step = builder.build!.first
      expect(step.retain_local).to eq("csv" => false, "parquet" => false)
    end

    it "supports universes: array" do
      builder.archive(:option_samples,
                      universes: %w[stock_option_symbols etf_option_symbols],
                      to: :s3_archive, artifacts: %i[csv parquet],
                      retain: { parquet: true })
      step = builder.build!.first
      expect(step.universe).to be_nil
      expect(step.universes).to eq(%w[stock_option_symbols etf_option_symbols])
    end
  end

  describe "#build!" do
    it "accumulates multiple tasks in order" do
      builder.compact(:option_samples, universe: "spx_symbols", delete_sources: true)
      builder.archive(:option_samples, universe: "spx_symbols",
                      to: :s3_archive, artifacts: %i[csv parquet])
      tasks = builder.build!
      expect(tasks.map(&:action)).to eq(%w[compact archive])
    end

    it "returns an empty array with no tasks" do
      expect(builder.build!).to eq([])
    end
  end
end
