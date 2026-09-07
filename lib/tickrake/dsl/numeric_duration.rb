# frozen_string_literal: true

module Tickrake
  module DSL
    NumericDuration = Struct.new(:value, :unit, keyword_init: true) do
      def to_interval_seconds
        case unit
        when :seconds then value
        when :minutes then value * 60
        when :hours   then value * 3_600
        else raise Tickrake::Error, "Cannot convert #{unit} to interval seconds"
        end
      end

      def to_lookback_days
        case unit
        when :days  then value
        when :weeks then value * 7
        else raise Tickrake::Error, "Cannot convert #{unit} to lookback days"
        end
      end
    end
  end
end

class Integer
  def seconds = Tickrake::DSL::NumericDuration.new(value: self, unit: :seconds)
  def second  = seconds
  def minutes = Tickrake::DSL::NumericDuration.new(value: self, unit: :minutes)
  def minute  = minutes
  def hours   = Tickrake::DSL::NumericDuration.new(value: self, unit: :hours)
  def hour    = hours
  def days    = Tickrake::DSL::NumericDuration.new(value: self, unit: :days)
  def day     = days
  def weeks   = Tickrake::DSL::NumericDuration.new(value: self, unit: :weeks)
  def week    = weeks
end
