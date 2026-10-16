# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class ApiRateLimit < Base
      self.table_name = "api_rate_limits"
      self.primary_key = :provider

      class << self
        def try_consume(provider:, capacity:, refill_rate:)
          now = Time.now.to_f
          transaction do
            record = where(provider: provider).take
            if record.nil?
              begin
                record = create!(provider: provider, tokens: capacity, last_refill_at: now)
              rescue ActiveRecord::RecordNotUnique
                record = where(provider: provider).take!
              end
            end

            record.with_lock do
              elapsed = [now - record.last_refill_at.to_f, 0.0].max
              available = [record.tokens.to_f + (elapsed * refill_rate), capacity].min
              return false if available < 1.0

              record.update!(tokens: [available - 1.0, 0.0].max, last_refill_at: now)
              true
            end
          end
        rescue StandardError
          false
        end
      end
    end
  end
end
