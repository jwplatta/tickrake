# frozen_string_literal: true

module Tickrake
  DatabaseConfig = Struct.new(
    :backend,
    :sqlite_path,
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
      backend.to_s.empty? || backend.to_s == "sqlite"
    end

    def postgres?
      backend.to_s == "postgres" || backend.to_s == "postgresql"
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
