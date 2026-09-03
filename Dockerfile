FROM ruby:3.3-slim

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
      build-essential \
      git \
      libsqlite3-dev \
      libssl-dev \
      curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

# Use precompiled native gems (avoids building duckdb from source)
RUN bundle config set --local force_ruby_platform false && \
    bundle install

RUN chmod +x bin/docker-entrypoint.sh

# ~/.tickrake and ~/.schwab_rb are mounted as volumes at runtime
ENV HOME=/root

ENTRYPOINT ["bin/docker-entrypoint.sh"]
