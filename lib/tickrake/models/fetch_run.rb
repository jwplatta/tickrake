# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class FetchRun < Base
      self.table_name = "fetch_runs"

      scope :running, -> { where(status: "running") }
      scope :completed, -> { where(status: "completed") }
      scope :failed, -> { where(status: "failed") }
    end
  end
end
