# frozen_string_literal: true

require_relative "../spec_helper"
require "tmpdir"
require "json"

RSpec.describe Tickrake::Index::AtomicJsonWriter do
  subject(:writer) { described_class.new }

  describe "#write" do
    it "writes a JSON file at the target path" do
      Dir.mktmpdir do |dir|
        target = File.join(dir, "SPXW.json")
        payload = { "schema_version" => 1, "root" => "SPXW" }

        writer.write(target, payload)

        expect(File.exist?(target)).to be true
        expect(JSON.parse(File.read(target))).to eq(payload)
      end
    end

    it "creates parent directories if they do not exist" do
      Dir.mktmpdir do |dir|
        target = File.join(dir, "schwab", "SPXW.json")
        writer.write(target, { "root" => "SPXW" })
        expect(File.exist?(target)).to be true
      end
    end

    it "overwrites an existing file atomically" do
      Dir.mktmpdir do |dir|
        target = File.join(dir, "SPXW.json")
        File.write(target, JSON.generate({ "old" => true }))

        writer.write(target, { "new" => true })

        expect(JSON.parse(File.read(target))).to eq({ "new" => true })
      end
    end

    it "leaves no temp file behind after a successful write" do
      Dir.mktmpdir do |dir|
        target = File.join(dir, "SPXW.json")
        writer.write(target, { "root" => "SPXW" })
        tmp_files = Dir.glob("#{target}.tmp.*")
        expect(tmp_files).to be_empty
      end
    end
  end
end
