# frozen_string_literal: true

module Tickrake
  class OptionSampleWriter
    def initialize(client:, cli_app: SchwabRb::CLI::App.new)
      @client = client
      @cli_app = cli_app
    end

    def write(symbol:, option_root:, expiration_date:, directory:, timestamp:)
      options = {
        symbol: symbol,
        root: option_root,
        expiration_date: expiration_date,
        dir: directory,
        format: "csv",
        timestamp: timestamp
      }

      response = @cli_app.send(:fetch_option_sample, @client, options)
      FileUtils.mkdir_p(directory)
      output_path = @cli_app.send(:option_sample_output_path, directory, options, response)
      @cli_app.send(:write_option_sample, output_path, response, options)
      output_path
    end
  end
end
