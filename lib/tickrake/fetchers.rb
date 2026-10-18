# frozen_string_literal: true

require_relative "jobs/clients/fred_client"
require_relative "jobs/clients/bls_calendar"
require_relative "jobs/clients/fred_economic_calendar"
require_relative "jobs/clients/fomc_calendar"
require_relative "jobs/clients/alpha_vantage_earnings"

module Tickrake
  # Backward-compatible alias for existing callers
  Fetchers = Jobs::Clients
end
