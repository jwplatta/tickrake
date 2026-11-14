# frozen_string_literal: true

require "set"

module Tickrake
  module Streaming
    class OptionContractResolver
      def initialize(client, contracts_config)
        @client = client
        @underlying = contracts_config.underlying
        @option_root = contracts_config.respond_to?(:option_root) ? contracts_config.option_root : nil
        @atm_strikes = contracts_config.respond_to?(:atm_strikes) ? contracts_config.atm_strikes : nil
        @atm_points = contracts_config.respond_to?(:atm_points) ? contracts_config.atm_points : nil
        @front_n = (contracts_config.respond_to?(:front_n) && contracts_config.front_n) || 1
      end

      def resolve
        api_sym = Tickrake::OptionsJob.option_chain_api_symbol(@underlying)
        params = { include_underlying_quote: true }
        if @atm_strikes && @atm_points.nil?
          params[:strike_count] = @atm_strikes * 2
        end

        target_dates = resolve_target_expiration_dates(api_sym)
        if target_dates.any?
          params[:from_date] = target_dates.first
          params[:to_date] = target_dates.last
        end

        chain = @client.get_option_chain(api_sym, **params)
        return [] if chain.nil?

        if chain.respond_to?(:option_expiration_list) && !chain.respond_to?(:call_opts)
          return resolve_from_expiration_list(chain)
        end

        all_options = Array(chain.call_opts) + Array(chain.put_opts)
        unless @option_root.nil? || @option_root.to_s.empty?
          target_root = @option_root.to_s.upcase
          all_options = all_options.select { |opt| opt.respond_to?(:option_root) && opt.option_root.to_s.upcase == target_root }
        end

        return [] if all_options.empty?

        exp_dates = all_options.filter_map { |opt| normalize_date(opt.expiration_date) }.uniq.sort.first(@front_n)
        in_exp_options = all_options.select { |opt| exp_dates.include?(normalize_date(opt.expiration_date)) }

        spot = chain.respond_to?(:underlying_price) ? chain.underlying_price&.to_f : nil
        filtered = filter_atm_options(in_exp_options, spot)

        filtered.sort_by do |opt|
          [
            normalize_date(opt.expiration_date)&.iso8601.to_s,
            opt.respond_to?(:put_call) ? opt.put_call.to_s : "",
            opt.strike.to_f
          ]
        end.map(&:symbol).uniq
      end

      private

      def resolve_target_expiration_dates(api_sym)
        return [] unless @client.respond_to?(:get_option_expiration_chain)

        exp_chain = @client.get_option_expiration_chain(api_sym)
        return [] unless exp_chain.respond_to?(:expiration_list)

        matching = Array(exp_chain.expiration_list).select do |exp|
          next false if exp.respond_to?(:days_to_expiration) && exp.days_to_expiration.to_i < 0
          next true if @option_root.nil? || @option_root.to_s.empty?

          extract_roots(exp).include?(@option_root.to_s.upcase)
        end

        matching.filter_map do |exp|
          if exp.respond_to?(:date_object) && exp.date_object
            exp.date_object
          elsif exp.respond_to?(:expiration_date)
            normalize_date(exp.expiration_date)
          end
        end.uniq.sort.first(@front_n)
      end

      def extract_roots(exp_entry)
        roots = exp_entry.respond_to?(:option_roots) ? exp_entry.option_roots : nil
        case roots
        when Array
          roots.map { |r| r.to_s.strip.upcase }
        when String
          roots.split(",").map { |r| r.strip.upcase }
        else
          []
        end
      end

      def filter_atm_options(options, spot)
        if @atm_points && spot && spot > 0
          options.select { |opt| (opt.strike.to_f - spot).abs <= @atm_points.to_f }
        elsif @atm_strikes && spot && spot > 0
          options.group_by { |opt| normalize_date(opt.expiration_date) }.flat_map do |_, exp_opts|
            strikes = exp_opts.map { |o| o.strike.to_f }.uniq.sort
            below = strikes.select { |s| s <= spot }.last(@atm_strikes)
            above = strikes.select { |s| s > spot }.first(@atm_strikes)
            allowed = (below + above).to_set
            exp_opts.select { |o| allowed.include?(o.strike.to_f) }
          end
        else
          options
        end
      end

      def normalize_date(value)
        return nil if value.nil?
        return value if value.is_a?(Date)

        Date.parse(value.to_s)
      end

      def resolve_from_expiration_list(chain)
        expirations = chain.option_expiration_list
                           .sort_by(&:expiration_date)
                           .first(@front_n)
        expirations.flat_map { |exp| exp.calls.map(&:symbol) + exp.puts.map(&:symbol) }
      end
    end
  end

  module OrderBook
    ContractResolver = Streaming::OptionContractResolver
  end
end
