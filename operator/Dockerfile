FROM clux/muslrust:stable AS chef
USER root
# We only pay the installation cost once,
# it will be cached from the second build onwards
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN rustup target add x86_64-unknown-linux-musl
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --target aarch64-unknown-linux-musl --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --target aarch64-unknown-linux-musl --release

# We do not need the Rust toolchain to run the binary!
FROM alpine:latest AS runtime
RUN addgroup -S noroot && adduser -S noroot -G noroot
COPY --from=builder /app/target/aarch64-unknown-linux-musl/release/operator /usr/local/bin
USER noroot
ENV RUST_BACKTRACE=full
CMD ["/usr/local/bin/operator"]
# ENTRYPOINT [ "sleep", "infinity" ]
