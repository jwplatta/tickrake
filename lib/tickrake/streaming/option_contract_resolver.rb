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
