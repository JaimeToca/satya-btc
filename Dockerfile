# ---- build stage ----
FROM rust:1-slim AS builder
WORKDIR /app

# Cache dependencies: build against a stub main first, so `cargo build` only
# re-downloads/re-compiles deps when Cargo.toml/lock change, not on every src edit.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Real sources; touch to force our crate (not deps) to rebuild.
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---- runtime stage ----
FROM debian:bookworm-slim AS runtime
# curl is used by the HEALTHCHECK below; ca-certificates lets reqwest verify TLS
# when BTC_RPC_URL is an https:// provider. Clean apt lists to keep the image slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Non-root user.
RUN useradd --system --uid 10001 --user-group app
COPY --from=builder /app/target/release/satya /usr/local/bin/satya
USER app

# Bind to all interfaces inside the container (the app defaults to 127.0.0.1).
ENV HTTP_BIND=0.0.0.0:8080 \
    RUST_LOG=info
EXPOSE 8080

# Report unhealthy if /health can't be reached. Note this only checks the HTTP
# server is up, not caught_up=true (which is false during any node outage).
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["satya"]
