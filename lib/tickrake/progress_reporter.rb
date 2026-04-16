# frozen_string_literal: true

module Tickrake
  class ProgressReporter
    def self.build(total:, title:, output:)
      return if total.to_i <= 0

      new(
        progressbar: ProgressBar.create(
          total: total,
          title: title,
          output: output,
          autofinish: false
        )
      )
    end

    def initialize(progressbar:)
      @progressbar = progressbar
      @mutex = Mutex.new
    end

    def advance(title: nil)
      @mutex.synchronize do
        @progressbar.title = title if title
        @progressbar.increment
      end
    end

    def add_total(delta)
      return if delta.to_i <= 0

      @mutex.synchronize do
        @progressbar.total = @progressbar.total + delta
      end
    end

    def finish
      @mutex.synchronize do
        @progressbar.finish unless @progressbar.finished?
      end
    end
  end
end
