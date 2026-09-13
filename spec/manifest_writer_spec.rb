# frozen_string_literal: true

RSpec.describe Tickrake::Maintenance::OptionSamples::ManifestWriter do
  let(:s3_archive) { instance_double(Tickrake::Storage::S3Archive) }
  let(:writer) { described_class.new(s3_archive: s3_archive) }

  let(:args) do
    {
      dataset_type: "options",
      provider: "schwab",
      root: "SPXW",
      sample_date: Date.new(2026, 9, 13)
    }
  end

  let(:expected_key) { "manifests/options/schwab/SPXW_2026-09-13.json" }

  describe "#write" do
    it "uploads correct manifest JSON and returns the S3 URI" do
      allow(s3_archive).to receive(:bucket).and_return("tickrake")
      allow(s3_archive).to receive(:upload_content)

      artifacts = { "csv" => { "uri" => "s3://tickrake/options/schwab/SPXW_samples_2026-09-13.csv", "row_count" => 42 } }
      uri = writer.write(**args, artifacts: artifacts, archived_at: Time.utc(2026, 9, 13, 21, 0, 0))

      expect(uri).to eq("s3://tickrake/#{expected_key}")

      expect(s3_archive).to have_received(:upload_content).with(
        expected_key,
        satisfy { |json|
          parsed = JSON.parse(json)
          parsed["schema_version"] == 1 &&
            parsed["provider"] == "schwab" &&
            parsed["root"] == "SPXW" &&
            parsed["sample_date"] == "2026-09-13" &&
            parsed["artifacts"] == artifacts
        }
      )
    end
  end

  describe "#manifest_exists?" do
    it "returns true when the key is found in S3" do
      allow(s3_archive).to receive(:list_keys).with(prefix: expected_key).and_return([expected_key])
      expect(writer.manifest_exists?(**args)).to eq(true)
    end

    it "returns false when the key is not found in S3" do
      allow(s3_archive).to receive(:list_keys).with(prefix: expected_key).and_return([])
      expect(writer.manifest_exists?(**args)).to eq(false)
    end
  end

  describe "#read" do
    it "downloads and parses manifest JSON from S3" do
      payload = { "schema_version" => 1, "provider" => "schwab", "root" => "SPXW", "sample_date" => "2026-09-13" }
      allow(s3_archive).to receive(:download_content).with(expected_key).and_return(JSON.generate(payload))

      result = writer.read(**args)

      expect(result).to eq(payload)
    end
  end
end
