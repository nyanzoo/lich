# Build
ARG FEATURE="bad"
FROM rust as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p lich --features "${FEATURE}"

# Run
FROM alpine:latest
COPY --from=builder ./app/target/release/lich /bin
RUN mkdir -p /etc/lich
ENTRYPOINT [ "lich" ]
