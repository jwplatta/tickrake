# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class Context
        attr_reader :config, :tracker, :provider_name, :option_root, :sample_date, :logger, :storage_paths

        def initialize(config:, tracker:, provider_name:, option_root:, sample_date:, logger:, storage_paths: Tickrake::Storage::Paths.new(config))
          @config = config
          @tracker = tracker
          @provider_name = provider_name.to_s
          @option_root = option_root.to_s
          @sample_date = sample_date
          @logger = logger
          @storage_paths = storage_paths
        end

        def dataset
          @dataset ||= Tickrake::Storage::OptionCompactionDataset.new(
            config: config,
            provider_name: provider_name,
            option_root: option_root,
            storage_paths: storage_paths
          )
        end

        def compacted_path(format)
          storage_paths.option_compacted_sample_path(
            provider: provider_name,
            root: option_root,
            sample_date: sample_date,
            format: format
          )
        end

        def log(level, message)
          logger&.public_send(level, "maintenance option_samples provider=#{provider_name} root=#{option_root} sample_date=#{sample_date}: #{message}")
        end
      end
    end
  end
end
