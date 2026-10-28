#!/usr/bin/env bash
# Deploy the bot image to the VPS WITHOUT compiling on the VPS (2 GiB RAM would
# risk OOM on a Rust LTO release build). Build the image locally, ship it via
# `docker save | ssh docker load`, and sync the source tree + (once) the secrets.
#
# Run from this crate directory (beside docker-compose.yml) in Git Bash, with VPS_HOST and KEY set:
#   export VPS_HOST='ubuntu@<host>'
#   export KEY="$HOME/.ssh/<deploy-key>.pem"
#   scripts/deploy_vps.sh image      # build locally + ship the image to the VPS
#   scripts/deploy_vps.sh source     # sync source/config/compose (NOT secrets)
#   scripts/deploy_vps.sh secrets    # copy aster.env + lighter.env (chmod 600)
#   scripts/deploy_vps.sh all        # source + image (use `secrets` once, separately)
#
# After a code/config change (sync loop): `scripts/deploy_vps.sh image && scripts/deploy_vps.sh source`
set -euo pipefail

VPS_HOST="${VPS_HOST:-}"
KEY="${KEY:-}"
DEST="${DEST:-/home/ubuntu/LIGHTER_ASTER_BOT}"
IMAGE="${IMAGE:-lighter_aster_bot:live}"

SSH=(ssh -i "$KEY" -o StrictHostKeyChecking=accept-new "$VPS_HOST")

log() { printf '\n=== %s ===\n' "$*" >&2; }

require_deploy_env() {
  if [[ -z "$VPS_HOST" || -z "$KEY" ]]; then
    cat >&2 <<'EOF'
Set VPS_HOST and KEY before deploying.

Example:
  export VPS_HOST='ubuntu@<host>'
  export KEY="$HOME/.ssh/<deploy-key>.pem"
EOF
    exit 2
  fi
  if [[ ! -f "$KEY" ]]; then
    echo "KEY does not point to an existing file: $KEY" >&2
    exit 2
  fi
}

build_image() {
  log "build image locally ($IMAGE)"
  docker compose build bot    # the live service, behind the `live` profile
}

ship_image() {
  build_image
  log "ship image -> $VPS_HOST (docker save | gzip | ssh docker load)"
  docker save "$IMAGE" | gzip -c | "${SSH[@]}" 'gunzip -c | docker load'
}

sync_source() {
  log "sync source -> $VPS_HOST:$DEST (excludes secrets, target*, runs, .git)"
  "${SSH[@]}" "mkdir -p '$DEST/runs'"
  # tar the build inputs + ops files; secrets are handled separately by `secrets`.
  tar czf - \
    Dockerfile docker-compose.yml .dockerignore bot.toml \
    Cargo.toml Cargo.lock src scripts signers RUNBOOK.md \
    | "${SSH[@]}" "tar xzf - -C '$DEST'"
}

copy_secrets() {
  log "copy secrets -> $VPS_HOST:$DEST (aster.env, lighter.env; chmod 600)"
  scp -i "$KEY" -o StrictHostKeyChecking=accept-new \
    aster.env lighter.env "$VPS_HOST:$DEST/"
  "${SSH[@]}" "chmod 600 '$DEST/aster.env' '$DEST/lighter.env'"
}

case "${1:-}" in
  image)   require_deploy_env; ship_image ;;
  source)  require_deploy_env; sync_source ;;
  secrets) require_deploy_env; copy_secrets ;;
  all)     require_deploy_env; sync_source; ship_image ;;
  *) echo "usage: $0 {image|source|secrets|all}" >&2; exit 2 ;;
esac

log "done: ${1:-}"
