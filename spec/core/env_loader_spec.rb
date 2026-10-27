# frozen_string_literal: true

RSpec.describe Tickrake::Core::EnvLoader do
  let(:tmpdir) { Dir.mktmpdir }

  after do
    FileUtils.remove_entry(tmpdir)
    ENV.delete("TICKRAKE_ENV_FILE")
    ENV.delete("TEST_ENV_VAR")
  end

  it "loads environment variables from explicit path when passed" do
    env_file = File.join(tmpdir, "custom.env")
    File.write(env_file, "TEST_ENV_VAR=explicit_success\n")

    described_class.load!(env_file)
    expect(ENV["TEST_ENV_VAR"]).to eq("explicit_success")
  end

  it "raises ConfigError if explicit path does not exist" do
    expect {
      described_class.load!(File.join(tmpdir, "nonexistent.env"))
    }.to raise_error(Tickrake::ConfigError, /Environment file not found/)
  end

  it "loads from TICKRAKE_ENV_FILE if no explicit path given" do
    env_file = File.join(tmpdir, "env_var.env")
    File.write(env_file, "TEST_ENV_VAR=env_var_success\n")
    ENV["TICKRAKE_ENV_FILE"] = env_file

    described_class.load!
    expect(ENV["TEST_ENV_VAR"]).to eq("env_var_success")
  end

  it "raises ConfigError if TICKRAKE_ENV_FILE does not exist" do
    ENV["TICKRAKE_ENV_FILE"] = File.join(tmpdir, "missing.env")

    expect {
      described_class.load!
    }.to raise_error(Tickrake::ConfigError, /Environment file specified by TICKRAKE_ENV_FILE not found/)
  end

  it "loads from working directory .env if present" do
    env_file = File.join(tmpdir, ".env")
    File.write(env_file, "TEST_ENV_VAR=workdir_success\n")

    Dir.chdir(tmpdir) do
      described_class.load!
    end

    expect(ENV["TEST_ENV_VAR"]).to eq("workdir_success")
  end

  it "does not overwrite existing environment variables" do
    ENV["TEST_ENV_VAR"] = "existing_value"
    env_file = File.join(tmpdir, "test.env")
    File.write(env_file, "TEST_ENV_VAR=new_value\n")

    described_class.load!(env_file)
    expect(ENV["TEST_ENV_VAR"]).to eq("existing_value")
  end
end
