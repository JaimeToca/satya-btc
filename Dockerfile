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
# Non-root user.
RUN useradd --system --uid 10001 --user-group app
COPY --from=builder /app/target/release/btc-indexer /usr/local/bin/btc-indexer
USER app

# Bind to all interfaces inside the container (the app defaults to 127.0.0.1).
ENV HTTP_BIND=0.0.0.0:8080 \
    RUST_LOG=info
EXPOSE 8080

ENTRYPOINT ["btc-indexer"]
