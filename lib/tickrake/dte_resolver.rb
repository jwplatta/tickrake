# frozen_string_literal: true

module Tickrake
  ResolvedExpiration = Struct.new(:date, :days_to_expiration, :requested_buckets, keyword_init: true)
  OptionExpirationEntry = Struct.new(:expiration_date, :days_to_expiration, keyword_init: true)

  class DteResolver
    def initialize(expiration_entries:, target_buckets:)
      @expiration_entries = expiration_entries
      @target_buckets = target_buckets
    end

    def resolve
      matches = {}

      @target_buckets.each do |bucket|
        expiration = @expiration_entries.min_by do |entry|
          [(entry.days_to_expiration - bucket).abs, entry.days_to_expiration]
        end
        next unless expiration

        key = expiration.expiration_date
        matches[key] ||= ResolvedExpiration.new(
          date: Date.iso8601(expiration.expiration_date),
          days_to_expiration: expiration.days_to_expiration,
          requested_buckets: []
        )
        matches[key].requested_buckets << bucket
      end

      matches.values.sort_by(&:date)
    end
  end
end
