# frozen_string_literal: true

module Tickrake
  ScheduledRunResult = Struct.new(:success_count, :failure_count, keyword_init: true) do
    def successful?
      failure_count.to_i.zero? && (success_count.to_i.positive? || no_tasks?)
    end

    def no_tasks?
      success_count.to_i.zero? && failure_count.to_i.zero?
    end

    def degraded?
      !successful?
    end
  end
end
