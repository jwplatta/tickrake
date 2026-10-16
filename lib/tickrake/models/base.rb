# frozen_string_literal: true

require "active_record"

module Tickrake
  module Models
    class Base < ActiveRecord::Base
      self.abstract_class = true
    end
  end
end
