# frozen_string_literal: true

module Tickrake
  DatabaseConfig = Struct.new(
    :adapter,
    :url,
    :host,
    :port,
    :database,
    :user,
    :password,
    :sslmode,
    :connect_timeout,
    keyword_init: true
  ) do
    def sqlite?
      adapter.to_s.empty? || adapter.to_s == "sqlite3"
    end

    def postgres?
      adapter.to_s == "postgresql"
    end

    def sqlite_path
      database || Tickrake::PathSupport.sqlite_path
    end

    def postgres_connection_params
      if url && !url.empty?
        { dbname: url }
      else
        {
          host: host,
          port: port ? Integer(port) : nil,
          dbname: database,
          user: user,
          password: password,
          sslmode: sslmode,
          connect_timeout: connect_timeout ? Integer(connect_timeout) : 10
        }.compact
      end
    end
  end
end
