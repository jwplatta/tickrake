# frozen_string_literal: true

module Tickrake
  ResolvedExpiration = Struct.new(:date, :days_to_expiration, :requested_buckets, keyword_init: true)

  class DteResolver
    def initialize(expiration_chain:, target_buckets:, option_root: nil)
      @expiration_chain = expiration_chain
      @target_buckets = target_buckets
      @option_root = option_root
    end

    def resolve
      matches = {}

      @target_buckets.each do |bucket|
        expiration = eligible_expirations.min_by do |entry|
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

    private

    def eligible_expirations
      @eligible_expirations ||= @expiration_chain.expiration_list.select do |expiration|
        next true if @option_root.nil? || @option_root.empty?

        roots = Array(expiration.option_roots.to_s.split(",")).map(&:strip)
        roots.include?(@option_root)
      end
    end
  end
end
