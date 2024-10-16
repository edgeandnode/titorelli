FROM rust:1-slim-bookworm AS build
RUN apt-get update && apt-get install -y \
  clang \
  cmake \
  librdkafka-dev \
  libsasl2-dev \
  libssl-dev \
  pkg-config \
  && rm -rf /var/lib/apt/lists/*
COPY . /src/
WORKDIR /src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
  ca-certificates \
  libsasl2-dev \
  libssl-dev \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/titorelli /opt/titorelli
CMD ["/opt/titorelli", "/opt/config.json"]
