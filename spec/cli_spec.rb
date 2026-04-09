# frozen_string_literal: true

RSpec.describe Tickrake::CLI do
  it "initializes config in Tickrake's own dot folder" do
    Dir.mktmpdir do |dir|
      fake_home = File.join(dir, ".tickrake")
      stdout = StringIO.new
      stderr = StringIO.new

      allow(Tickrake::PathSupport).to receive(:home_dir).and_return(fake_home)
      allow(Tickrake::PathSupport).to receive(:config_path).and_return(File.join(fake_home, "tickrake.yml"))
      allow(Tickrake::PathSupport).to receive(:sqlite_path).and_return(File.join(fake_home, "tickrake.sqlite3"))
      allow(Tickrake::PathSupport).to receive(:lock_path) { |name| File.join(fake_home, "#{name}.lock") }

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["init"])

      expect(exit_code).to eq(0)
      expect(File.exist?(File.join(fake_home, "tickrake.yml"))).to eq(true)
      expect(stdout.string).to include("Initialized Tickrake home")
    end
  end
end
