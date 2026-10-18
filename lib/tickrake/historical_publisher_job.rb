# frozen_string_literal: true

module Tickrake
  class HistoricalPublisherJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run
      settings = @scheduled_job.settings || {}
      datastore_name = settings["datastore_name"]
      datasets = settings["datasets"]
      batch_size = settings["batch_size"]
      delete_after_upload = settings["delete_after_upload"]
      retain_days = settings["retain_days"]

      publisher = Storage::HistoricalPublisher.new(
        runtime: @runtime,
        datastore_name: datastore_name
      )
      publisher.scan_and_publish(
        dataset_types: datasets,
        batch_size: batch_size,
        delete_after_upload: delete_after_upload,
        retain_days: retain_days
      )
    end
  end
end
