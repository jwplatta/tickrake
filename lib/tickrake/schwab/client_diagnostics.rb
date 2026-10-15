# frozen_string_literal: true

require "securerandom"
require "json"

module Tickrake
  module Schwab
    module ClientDiagnostics
      attr_accessor :rate_limiter, :diagnostic_logger

      class << self
        def correlation_id_generator(service = nil)
          svc = service || Thread.current[:tickrake_job_name] || ENV["TICKRAKE_SERVICE_NAME"] || "schwab"
          @counter ||= 0
          @counter += 1
          "#{svc}-#{Process.pid}-#{SecureRandom.hex(4)}-#{@counter}"
        end
      end

      # Override req_num so legacy internal log calls get unique correlation IDs
      def req_num
        next_correlation_id
      end

      def next_correlation_id
        ClientDiagnostics.correlation_id_generator
      end

      # Wrap public API methods that parse JSON to attach response metadata on parse errors
      %i[
        get_option_chain
        get_option_expiration_chain
        get_price_history
        get_instruments
        get_quotes
        get_account
        get_accounts
        get_account_numbers
        get_order
        get_orders_for_account
        get_all_orders
        get_user_preferences
        get_market_hours
        get_transaction
        get_transactions
        get_market_movers
      ].each do |method_name|
        define_method(method_name) do |*args, **kwargs, &block|
          super(*args, **kwargs, &block)
        rescue JSON::ParserError => e
          metadata = Thread.current[:tickrake_last_response_metadata] || {}
          attach_metadata_to_error(e, metadata)
          raise e
        end
      end

      private

      def get(path, params = {})
        dest = URI(URI::DEFAULT_PARSER.escape("#{SchwabRb::Client::BASE_URL}#{path}"))
        dest.query = URI.encode_www_form(params) if params.any?

        execute_diagnostic_request("GET", dest, params) do
          session.get(dest)
        end
      end

      def post(path, data = {})
        dest = URI(URI::DEFAULT_PARSER.escape("#{SchwabRb::Client::BASE_URL}#{path}"))

        execute_diagnostic_request("POST", dest, data) do
          session.post(
            dest,
            {
              body: data.to_json,
              headers: { "Content-Type" => "application/json" }
            }
          )
        end
      end

      def put(path, data = {})
        dest = URI(URI::DEFAULT_PARSER.escape("#{SchwabRb::Client::BASE_URL}#{path}"))

        execute_diagnostic_request("PUT", dest, data) do
          session.put(
            dest,
            {
              body: data.to_json,
              headers: { "Content-Type" => "application/json" }
            }
          )
        end
      end

      def delete(path)
        dest = URI(URI::DEFAULT_PARSER.escape("#{SchwabRb::Client::BASE_URL}#{path}"))

        execute_diagnostic_request("DELETE", dest) do
          session.delete(dest)
        end
      end

      def execute_diagnostic_request(method, dest, payload = nil)
        corr_id = next_correlation_id
        Thread.current[:tickrake_current_correlation_id] = corr_id
        start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        limiter = rate_limiter || Thread.current[:tickrake_rate_limiter]
        limiter&.consume!

        log_request_diagnostics(method, corr_id, dest, payload)

        begin
          response = yield
          elapsed_ms = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time) * 1000).round

          metadata = build_response_metadata(response, corr_id, elapsed_ms)
          Thread.current[:tickrake_last_response_metadata] = metadata

          if response.status == 429
            retry_after = extract_header(response.headers, "retry-after")
            limiter&.apply_cooldown!(retry_after)
            log_rate_limit_429(corr_id, retry_after)
          end

          log_response_diagnostics(response, metadata)
          response
        rescue StandardError, Exception => e
          elapsed_ms = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time) * 1000).round
          response_obj = e.respond_to?(:response) ? e.response : nil

          metadata = if response_obj
                       build_response_metadata(response_obj, corr_id, elapsed_ms)
                     else
                       {
                         correlation_id: corr_id,
                         http_status: nil,
                         content_type: nil,
                         content_length: nil,
                         body_bytes: nil,
                         duration_ms: elapsed_ms,
                         parse_outcome: :transport_error
                       }
                     end

          Thread.current[:tickrake_last_response_metadata] = metadata
          attach_metadata_to_error(e, metadata)

          if response_obj && response_obj.status == 429
            retry_after = extract_header(response_obj.headers, "retry-after")
            limiter&.apply_cooldown!(retry_after)
            log_rate_limit_429(corr_id, retry_after)
          end

          log_error_diagnostics(e, metadata)
          raise e
        end
      end

      def build_response_metadata(response, corr_id, duration_ms)
        status = response.status
        headers = response.respond_to?(:headers) ? response.headers : {}
        content_type = extract_header(headers, "content-type")
        raw_len = extract_header(headers, "content-length")
        content_length = raw_len ? raw_len.to_i : nil
        body_str = response.respond_to?(:body) ? response.body.to_s : ""
        body_bytes = body_str.bytesize

        parse_outcome = if body_str.strip.empty?
                          :empty_body
                        else
                          begin
                            JSON.parse(body_str)
                            :success
                          rescue JSON::ParserError
                            :json_parser_error
                          end
                        end

        {
          correlation_id: corr_id,
          http_status: status,
          content_type: content_type,
          content_length: content_length,
          body_bytes: body_bytes,
          duration_ms: duration_ms,
          parse_outcome: parse_outcome
        }
      end

      def extract_header(headers, name)
        return nil unless headers.is_a?(Hash)

        headers[name] || headers[name.downcase] || headers[name.capitalize]
      end

      def attach_metadata_to_error(error, metadata)
        error.instance_variable_set(:@response_metadata, metadata)
        unless error.respond_to?(:response_metadata)
          error.define_singleton_method(:response_metadata) { @response_metadata }
        end
        unless error.respond_to?(:correlation_id)
          error.define_singleton_method(:correlation_id) { @response_metadata&.dig(:correlation_id) }
        end
      end

      def log_request_diagnostics(method, corr_id, dest, payload)
        redacted_dest = SchwabRb::Redactor.redact_url(dest.to_s)
        logger = effective_logger
        logger.info("Req #{corr_id}: #{method} to #{redacted_dest}")
      end

      def log_response_diagnostics(response, metadata)
        logger = effective_logger
        logger.info(
          "Resp #{metadata[:correlation_id]}: Status #{metadata[:http_status]} " \
          "type=#{metadata[:content_type] || '-'} len=#{metadata[:content_length] || '-'} " \
          "bytes=#{metadata[:body_bytes]} duration=#{metadata[:duration_ms]}ms parse=#{metadata[:parse_outcome]}"
        )
      end

      def log_error_diagnostics(error, metadata)
        logger = effective_logger
        status_part = metadata[:http_status] ? " status=#{metadata[:http_status]}" : ""
        logger.error(
          "Req #{metadata[:correlation_id]}: Error #{error.class.name} - #{error.message} " \
          "duration=#{metadata[:duration_ms]}ms#{status_part} parse=#{metadata[:parse_outcome]}"
        )
      end

      def log_rate_limit_429(corr_id, retry_after)
        logger = effective_logger
        logger.warn(
          "Resp #{corr_id}: HTTP 429 Too Many Requests, applying cooldown (Retry-After: #{retry_after || 'none'})"
        )
      end

      def effective_logger
        diagnostic_logger || SchwabRb::Logger.logger
      end
    end
  end
end
