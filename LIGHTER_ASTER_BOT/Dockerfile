# syntax=docker/dockerfile:1
#
# Aster/Lighter/Hyperliquid bot: XEMM plus taker arbitrage on Aster routes,
# taker alone on Lighter/Hyperliquid, in one binary and process.
# Multi-stage: a Rust builder compiles the release binary; a slim Debian runtime
# carries ONLY the binary + the (non-secret) live config. Secrets
# (per-venue *.env), signer binaries, and the runs/ output dir are MOUNTED at runtime
# by docker-compose and are NEVER baked into an image layer.
#
# Build locally (fast box, plenty of RAM) and ship the image to the 2 GB VPS:
#   docker compose --profile live build bot    # tags lighter_aster_bot:live
#   docker save lighter_aster_bot:live | gzip | ssh ... 'gunzip | docker load'
# The VPS never compiles (a Rust LTO release build would risk OOM on 1.9 GiB).

############################
# 1) Builder
############################
# Match Cargo.toml and the verified release toolchain; retain bookworm glibc.
FROM rust:1.92-bookworm AS builder

WORKDIR /app

# Only what the crate needs to compile: manifests + lockfile + sources.
# (No build.rs / workspace; TLS=rustls, all *-sys crates build via the stock image's
# `cc` toolchain — no extra apt packages required.)
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# BuildKit cache mounts make iterative rebuilds (synced bug fixes) fast; the final
# binary is copied to a stable, non-cache path so the runtime stage can pull it.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --bin lighter_aster_bot \
 && cp /app/target/release/lighter_aster_bot /usr/local/bin/lighter_aster_bot

############################
# 2) Runtime
############################
FROM debian:bookworm-slim AS runtime

# rustls/webpki-roots bundles Mozilla roots, but install ca-certificates so every
# reqwest/TLS path has system roots (outbound HTTPS to fapi.asterdex.com /
# mainnet.zklighter.elliot.ai at startup for exchangeInfo + Lighter specs). No OpenSSL needed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# The live Aster nonce counter's mount point: a fresh named volume takes this owner, the
# compose file's default XEMM_UID/GID.
RUN mkdir -m 700 /nonce && chown 1000:1000 /nonce

COPY --from=builder /usr/local/bin/lighter_aster_bot /usr/local/bin/lighter_aster_bot
# Non-secret live config baked in as a fallback; compose also bind-mounts it so the
# markets/edge can be tuned without rebuilding the image.
COPY bot.toml /app/bot.toml

# SIGINT, SIGTERM and SIGHUP all start the same graceful drain (cancel resting orders,
# hedge fills, correct net residuals, verify; paired positions stay open). The controller allows
# 200 s per engine after an in-progress status read: stop with `docker stop -t 460` (compose sets
# stop_grace_period), never the 10 s default, which SIGKILLs a legitimate drain.
STOPSIGNAL SIGINT

ENTRYPOINT ["lighter_aster_bot"]
# Safe default: print help and exit. Real runs pass an explicit command via
# `docker compose --profile live run --rm bot <args>` (see docker-compose.yml / RUNBOOK.md).
CMD ["--help"]
