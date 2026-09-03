FROM ruby:3.3-slim

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
      build-essential \
      git \
      libsqlite3-dev \
      libssl-dev \
      curl \
      unzip \
    && rm -rf /var/lib/apt/lists/*

# Install DuckDB headers and shared library (required to build the duckdb gem)
ARG DUCKDB_VERSION=1.5.4
RUN curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-aarch64.zip" -o /tmp/libduckdb.zip \
    && unzip /tmp/libduckdb.zip -d /tmp/libduckdb \
    && cp /tmp/libduckdb/duckdb.h /usr/local/include/ \
    && cp /tmp/libduckdb/libduckdb.so /usr/local/lib/ \
    && ldconfig \
    && rm -rf /tmp/libduckdb /tmp/libduckdb.zip

WORKDIR /app

COPY . .

RUN bundle install

RUN chmod +x bin/docker-entrypoint.sh

# ~/.tickrake and ~/.schwab_rb are mounted as volumes at runtime
ENV HOME=/root

ENTRYPOINT ["bin/docker-entrypoint.sh"]
