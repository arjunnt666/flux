FROM rust:1.80-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p flux-cli

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/flux /usr/local/bin/flux
CMD ["flux", "version"]
