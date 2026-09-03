FROM ruby:3.3-slim

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
      build-essential \
      git \
      libsqlite3-dev \
      curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN bundle install

RUN chmod +x bin/docker-entrypoint.sh

# ~/.tickrake and ~/.schwab_rb are mounted as volumes at runtime
ENV HOME=/root

ENTRYPOINT ["bin/docker-entrypoint.sh"]
