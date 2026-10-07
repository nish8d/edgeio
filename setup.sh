#!/usr/bin/env bash
# Install, upgrade or remove the edge health agent on this device.
#
#   git clone -b devicehealth https://github.com/nish8d/edgeio.git edgehealth-src
#   cd edgehealth-src && ./setup.sh --server <server-tailscale-ip>:9094
#
# Every container on the device is reported under its own name, plus the docker and
# tailscaled host services. --streamer / --rename only change the names some are reported as.
#
# Everything installed lives in ~/edgehealth (override with EDGEHEALTH_DIR):
#   ~/edgehealth/src/       the agent source the image is built from
#   ~/edgehealth/agent.env  this device's settings (kept across upgrades)
#   ~/edgehealth/VERSION    the installed git revision
#   ~/edgehealth/build.log  output of the last image build
# Unsent readings are kept in the Docker volume edgeio-agent-spool.
#
# Upgrade:   git pull && ./setup.sh
# Remove:    ./setup.sh --uninstall           (keeps settings and unsent readings)
#            ./setup.sh --uninstall --purge   (removes everything)

set -euo pipefail

INSTALL_DIR="${EDGEHEALTH_DIR:-$HOME/edgehealth}"
CONTAINER=edgeio-agent
IMAGE=edgehealth/agent
VOLUME=edgeio-agent-spool
LEGACY_ENV="$HOME/edgeio-agent.env"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

server="" streamer="" renames=() uninstall=false purge=false

usage() {
    cat <<EOF
Usage: ./setup.sh [--server HOST:PORT] [--streamer CONTAINER] [--rename NAME=KEY]...
       ./setup.sh --uninstall [--purge]

  --server HOST:PORT     Kafka TAILNET listener of the edgeio server (e.g. 100.x.y.z:9094).
                         Required on first install; remembered afterwards.
  --streamer CONTAINER   Optional. Report this container as "edge_streamer" instead of its name.
  --rename NAME=KEY      Optional. Report container NAME under service key KEY (repeatable).

Every container on the device is reported as a service under its own name; the options
above only rename some of them.
  --uninstall            Stop and remove the agent container and image.
  --purge                With --uninstall: also delete settings and unsent readings.

Installs into $INSTALL_DIR (set EDGEHEALTH_DIR to change).
EOF
}

say() { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarning:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server) server="${2:?--server needs HOST:PORT}"; shift 2 ;;
        --streamer) streamer="${2:?--streamer needs a container name}"; shift 2 ;;
        --rename) renames+=("${2:?--rename needs NAME=KEY}"); shift 2 ;;
        --uninstall) uninstall=true; shift ;;
        --purge) purge=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown option: $1" ;;
    esac
done

[[ -z "$server" || "$server" == ?*:[0-9]* ]] || die "--server '$server' must be HOST:PORT"
for rename in "${renames[@]}"; do
    [[ "$rename" == ?*=?* ]] || die "--rename '$rename' must be NAME=KEY"
done

case "$INSTALL_DIR" in
    ""|/|"$HOME"|"$HOME/") die "refusing to install into '$INSTALL_DIR'" ;;
esac

# --- preflight -------------------------------------------------------------------------------

[[ "$(uname -s)" == Linux ]] || die "the agent runs on Linux only"
command -v docker >/dev/null || die "Docker is not installed (Ubuntu: sudo apt-get install docker.io)"
if ! docker info >/dev/null 2>&1; then
    die "cannot talk to Docker as $(id -un). Run: sudo usermod -aG docker $(id -un), then log out and back in"
fi

# --- uninstall -------------------------------------------------------------------------------

if $uninstall; then
    say "Removing container $CONTAINER"
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    say "Removing images $IMAGE"
    docker image ls -q "$IMAGE" | sort -u | xargs -r docker image rm -f >/dev/null 2>&1 || true
    if $purge; then
        say "Deleting unsent readings ($VOLUME) and $INSTALL_DIR"
        docker volume rm "$VOLUME" >/dev/null 2>&1 || true
        rm -rf -- "$INSTALL_DIR"
    else
        echo "Kept $INSTALL_DIR and volume $VOLUME (use --purge to delete them)."
    fi
    exit 0
fi

[[ -f /sys/fs/cgroup/cgroup.controllers ]] \
    || warn "cgroup v2 not found; host services (docker, tailscaled) will be reported as unknown"

tailscale_ip="$(ip -4 -o addr show 2>/dev/null \
    | awk '{split($4, a, "/"); print a[1]}' \
    | awk -F. '$1 == 100 && $2 >= 64 && $2 <= 127 {print; exit}')"
[[ -n "$tailscale_ip" ]] || tailscale_ip="$(tailscale ip -4 2>/dev/null | head -n 1 || true)"
[[ -n "$tailscale_ip" ]] \
    || die "no Tailscale address (100.64.0.0/10) on this machine; join the tailnet first (tailscale up)"

# --- settings --------------------------------------------------------------------------------

mkdir -p "$INSTALL_DIR"
chmod 700 "$INSTALL_DIR"
env_file="$INSTALL_DIR/agent.env"

if [[ ! -f "$env_file" && -f "$LEGACY_ENV" ]]; then
    say "Importing settings from $LEGACY_ENV"
    install -m 600 "$LEGACY_ENV" "$env_file"
fi
[[ -f "$env_file" ]] || install -m 600 /dev/null "$env_file"

get_key() { sed -n "s/^$1=//p" "$env_file" | tail -n 1; }
set_key() {
    local tmp
    tmp="$(mktemp "$INSTALL_DIR/.agent.env.XXXXXX")"
    grep -v "^$1=" "$env_file" > "$tmp" || true
    printf '%s=%s\n' "$1" "$2" >> "$tmp"
    chmod 600 "$tmp"
    mv "$tmp" "$env_file"
}

if [[ -z "$server" && -z "$(get_key KAFKA_BOOTSTRAP)" && -t 0 ]]; then
    read -rp "edgeio server Kafka address (e.g. 100.x.y.z:9094): " server
fi
[[ -n "$server" ]] && set_key KAFKA_BOOTSTRAP "$server"
bootstrap="$(get_key KAFKA_BOOTSTRAP)"
[[ -n "$bootstrap" ]] || die "no server configured; pass --server HOST:PORT"
[[ "$bootstrap" == *:* ]] || die "KAFKA_BOOTSTRAP '$bootstrap' must be HOST:PORT"

[[ -n "$streamer" ]] && renames=("$streamer=edge_streamer" "${renames[@]}")
if [[ ${#renames[@]} -gt 0 ]]; then
    set_key AGENT_SERVICE_RENAMES "$(IFS=,; echo "${renames[*]}")"
fi

host="${bootstrap%:*}" port="${bootstrap##*:}"
if timeout 5 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then
    say "Server $bootstrap is reachable"
else
    warn "cannot reach $bootstrap right now; readings will be kept on disk until it is"
fi

# --- code + image ----------------------------------------------------------------------------

version="$(git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null || echo local)"
if [[ -n "$(git -C "$REPO_DIR" status --porcelain 2>/dev/null)" ]]; then
    version="$version-modified"
fi

say "Copying agent source ($version) to $INSTALL_DIR/src"
rm -rf -- "$INSTALL_DIR/src.new"
mkdir "$INSTALL_DIR/src.new"
tar -C "$REPO_DIR" --exclude=.git --exclude=.venv --exclude=__pycache__ -cf - . \
    | tar -C "$INSTALL_DIR/src.new" -xf -
rm -rf -- "$INSTALL_DIR/src"
mv "$INSTALL_DIR/src.new" "$INSTALL_DIR/src"
echo "$version" > "$INSTALL_DIR/VERSION"

say "Building image $IMAGE:$version (the first build downloads the base image)"
# Build on the host network: Docker's default build network often can't use the device's DNS
# (systemd-resolved on 127.0.0.53, Tailscale's 100.100.100.100), so downloads would fail.
build_log="$INSTALL_DIR/build.log"
if ! docker build --network host -t "$IMAGE:$version" -t "$IMAGE:latest" "$INSTALL_DIR/src" \
        >"$build_log" 2>&1; then
    tail -n 25 "$build_log" >&2
    die "image build failed (full log: $build_log). The device needs internet access to Docker Hub, ghcr.io, PyPI and deb.debian.org"
fi

# --- run -------------------------------------------------------------------------------------

if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
    say "Replacing the running agent (unsent readings in $VOLUME are kept)"
    docker rm -f "$CONTAINER" >/dev/null
fi

say "Starting $CONTAINER"
docker run -d --name "$CONTAINER" --restart=always \
    --network host --pid host --uts host \
    --log-opt max-size=10m --log-opt max-file=3 \
    -v /:/host:ro \
    -v /sys/fs/cgroup:/host-cgroup:ro \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    -v "$VOLUME":/spool \
    --env-file "$env_file" \
    "$IMAGE:$version" >/dev/null

# Drop images from earlier installs; the running one is in use and stays.
docker image ls --format '{{.Repository}}:{{.Tag}}' "$IMAGE" \
    | grep -v -e ":$version\$" -e ':latest$' \
    | xargs -r docker image rm >/dev/null 2>&1 || true

say "Waiting for the first reading"
for _ in $(seq 1 30); do
    if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != true ]]; then
        docker logs --tail 20 "$CONTAINER" >&2 || true
        die "the agent stopped; see the log above"
    fi
    tick="$(docker logs "$CONTAINER" 2>&1 | grep '"message": "tick"' | tail -n 1 || true)"
    [[ -n "$tick" ]] && break
    sleep 2
done

if [[ -z "${tick:-}" ]]; then
    warn "no reading yet; check: docker logs -f $CONTAINER"
elif docker logs "$CONTAINER" 2>&1 | grep -q 'deliveries failed'; then
    warn "first reading saved but not delivered yet: $tick"
else
    say "First reading delivered: $tick"
fi

cat <<EOF

Installed edgehealth $version for $tailscale_ip, reporting to $bootstrap every 5 minutes.
  logs:      docker logs -f $CONTAINER
  settings:  $env_file  (then re-run ./setup.sh)
  upgrade:   git pull && ./setup.sh
  remove:    ./setup.sh --uninstall [--purge]
EOF
