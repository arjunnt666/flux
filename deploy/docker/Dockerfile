FROM rust:1.80-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p flux-cli || true

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/flux /usr/local/bin/flux || true
CMD ["flux", "version"]
