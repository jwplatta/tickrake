# frozen_string_literal: true

module Tickrake
  class ProgressReporter
    def self.build(total:, title:, output:)
      return if total.to_i <= 0

      if tty_output?(output)
        new(
          total: total,
          title: title,
          output: output,
          progressbar: ProgressBar.create(
            total: total,
            title: title,
            output: output,
            autofinish: false
          )
        )
      else
        new(total: total, title: title, output: output)
      end
    end

    def self.tty_output?(output)
      output.respond_to?(:tty?) && output.tty?
    end

    def initialize(total:, title:, output:, progressbar: nil)
      @total = total.to_i
      @title = title
      @output = output
      @progressbar = progressbar
      @current = 0
      @finished = false
      @mutex = Mutex.new
    end

    def advance(title: nil)
      @mutex.synchronize do
        @title = title if title
        @current += 1

        if @progressbar
          @progressbar.title = @title if @title
          @progressbar.increment
        else
          @output.puts("#{display_title} (#{@current}/#{@total})")
          @output.flush if @output.respond_to?(:flush)
        end
      end
    end

    def add_total(delta)
      return if delta.to_i <= 0

      @mutex.synchronize do
        @total += delta.to_i
        @progressbar.total = @progressbar.total + delta if @progressbar
      end
    end

    def finish
      @mutex.synchronize do
        return if @finished

        if @progressbar
          @progressbar.finish unless @progressbar.finished?
        elsif @current.zero?
          @output.puts("#{display_title} (0/#{@total})")
          @output.flush if @output.respond_to?(:flush)
        end
        @finished = true
      end
    end

    private

    def display_title
      @title.to_s.empty? ? "Progress" : @title
    end
  end
end
