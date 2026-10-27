# frozen_string_literal: true

module Tickrake
  class StagingPublisherJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run
      settings = @scheduled_job.settings || {}
      datasets = settings["datasets"]
      batch_size = settings["batch_size"]
      delete_after_upload = settings.fetch("delete_after_upload", true)

      publisher = Storage::StagingPublisher.new(runtime: @runtime)
      publisher.scan_and_publish(
        dataset_types: datasets,
        batch_size: batch_size,
        delete_after_upload: delete_after_upload
      )
    end
  end
end
