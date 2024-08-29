FROM rust AS chef
# We only pay the installation cost once,
# it will be cached from the second build onwards
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release

# We do not need the Rust toolchain to run the binary!
FROM debian:stable-slim AS runtime
RUN apt-get update && apt-get install -y net-tools dnsutils netcat-openbsd
COPY --from=builder /app/target/release/frontend /usr/local/bin
ENV RUST_BACKTRACE=full
ENTRYPOINT ["/usr/local/bin/frontend"]
# ENTRYPOINT [ "sleep", "infinity" ]
