#!/usr/bin/env bash
# End-to-end test for install.sh: does the published install path actually
# produce a working pg_dbmigrator plus a usable pg_dump/pg_restore, on a clean
# machine, for each mainstream libc and package manager?
#
# Flow:
#   1. Build (or accept) a static musl binary for the requested arch.
#   2. Stage it as a release tarball + checksums.txt and serve it over HTTP.
#   3. For each distro image: run install.sh with PG_DBMIGRATOR_BASE_URL pointed
#      at that server, then assert both halves of the install work.
#
# Usage:
#   tests/integration/run_install_script.sh [x86_64|aarch64]
#
# Environment:
#   IMAGES                  space-separated docker images to test
#   PG_DBMIGRATOR_MUSL_BIN  prebuilt binary to use instead of building one
#
# Running the aarch64 arch on an x86_64 host needs qemu registered first:
#   docker run --privileged --rm tonistiigi/binfmt --install arm64
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

ARCH="${1:-x86_64}"
case "$ARCH" in
    x86_64)  TARGET='x86_64-unknown-linux-musl';  PLATFORM='linux/amd64' ;;
    aarch64) TARGET='aarch64-unknown-linux-musl'; PLATFORM='linux/arm64' ;;
    *) echo "usage: $0 [x86_64|aarch64]" >&2; exit 2 ;;
esac

# The three images cover the three /bin/sh implementations install.sh has to
# survive (dash, bash-as-sh, busybox ash) and the three package managers it
# dispatches to. Alpine deliberately gets no curl, so the busybox wget branch
# is exercised too.
declare -A PREREQ=(
    ["ubuntu:22.04"]='apt-get update -qq && apt-get install -y -qq curl ca-certificates'
    ["rockylinux/rockylinux:9"]='true'
    ["alpine:3"]='true'
)
read -r -a IMAGE_LIST <<<"${IMAGES:-ubuntu:22.04 rockylinux/rockylinux:9 alpine:3}"

# Ubuntu 22.04 ships pg_dump 14 and RHEL 9 ships 13; both refuse a modern
# source. If install.sh ever silently falls back to the distro repo this is the
# assertion that catches it.
MIN_PG_MAJOR=17

VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')"
TAG="v${VERSION}"

WORKDIR=""
HTTP_PID=""
PG_PROBE_NAME=""
cleanup() {
    if [[ -n "$HTTP_PID" ]]; then kill "$HTTP_PID" 2>/dev/null || true; fi
    if [[ -n "$PG_PROBE_NAME" ]]; then docker rm -f "$PG_PROBE_NAME" >/dev/null 2>&1 || true; fi
    if [[ -n "$WORKDIR" ]]; then rm -rf "$WORKDIR"; fi
}
trap cleanup EXIT

# ── 1. binary ───────────────────────────────────────────────────────────────

BIN="${PG_DBMIGRATOR_MUSL_BIN:-}"
if [[ -z "$BIN" ]]; then
    echo "==> building $TARGET"
    if [[ "$ARCH" == "$(uname -m)" ]]; then
        command -v musl-gcc >/dev/null 2>&1 \
            || { echo "musl-gcc missing: sudo apt-get install -y musl-tools" >&2; exit 1; }
        cargo build --release --locked --target "$TARGET" -p pg_dbmigrator --bin pg_dbmigrator
    else
        # ring/aws-lc-rs need a C cross-compiler; cross supplies one per target.
        command -v cross >/dev/null 2>&1 \
            || { echo "cross missing: cargo install cross --locked" >&2; exit 1; }
        cross build --release --locked --target "$TARGET" -p pg_dbmigrator --bin pg_dbmigrator
    fi
    BIN="target/$TARGET/release/pg_dbmigrator"
fi
[[ -f "$BIN" ]] || { echo "binary not found: $BIN" >&2; exit 1; }

echo "==> asserting $BIN is statically linked"
# x86_64 comes out `static-pie linked`, aarch64 `statically linked` -- both are
# fine, a dynamic one is not.
file_out="$(file "$BIN")"
echo "    $file_out"
grep -Eq 'static-pie linked|statically linked' <<<"$file_out" \
    || { echo "FAIL: $BIN is not statically linked" >&2; exit 1; }
if grep -q 'dynamically linked' <<<"$file_out"; then
    echo "FAIL: $BIN is dynamically linked" >&2
    exit 1
fi

# ── 2. stage a release ──────────────────────────────────────────────────────

WORKDIR="$(mktemp -d)"
TARBALL="pg_dbmigrator-${TAG}-${TARGET}.tar.gz"
mkdir -p "$WORKDIR/stage"
cp "$BIN" "$WORKDIR/stage/pg_dbmigrator"
cp LICENSE "$WORKDIR/stage/"
tar -czf "$WORKDIR/$TARBALL" -C "$WORKDIR/stage" pg_dbmigrator LICENSE
rm -rf "$WORKDIR/stage"
( cd "$WORKDIR" && sha256sum "$TARBALL" > checksums.txt )

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')"
( cd "$WORKDIR" && python3 -m http.server "$PORT" --bind 0.0.0.0 >/dev/null 2>&1 ) &
HTTP_PID=$!

for _ in $(seq 1 40); do
    curl -fsS "http://127.0.0.1:$PORT/checksums.txt" >/dev/null 2>&1 && break
    sleep 0.25
done
curl -fsS "http://127.0.0.1:$PORT/checksums.txt" >/dev/null \
    || { echo "FAIL: local release server never came up" >&2; exit 1; }

echo "==> serving $TARBALL on port $PORT"

# ── 3. install and verify, per distro ───────────────────────────────────────

failures=0
# Distro mirrors are external and do go down mid-run (observed: Rocky's aarch64
# BaseOS 404ing for a few minutes). One retry keeps that from turning into a red
# build; a real install.sh bug fails both attempts.
ATTEMPTS=2

for image in "${IMAGE_LIST[@]}"; do
    echo
    echo "==> $image ($ARCH)"
    prereq="${PREREQ[$image]:-true}"
    passed=0

    for attempt in $(seq 1 "$ATTEMPTS"); do
        if docker run --rm --platform "$PLATFORM" --network host \
            -v "$ROOT/install.sh:/install.sh:ro" \
            -e "PG_DBMIGRATOR_BASE_URL=http://127.0.0.1:$PORT" \
            -e "PG_DBMIGRATOR_VERSION=$TAG" \
            -e "EXPECT_VERSION=$VERSION" \
            -e "MIN_PG_MAJOR=$MIN_PG_MAJOR" \
            "$image" sh -c '
                set -e
                '"$prereq"' >/dev/null 2>&1

                sh /install.sh > /tmp/install.log 2>&1 || {
                    echo "install.sh exited $?"; cat /tmp/install.log; exit 1;
                }

                PATH="$HOME/.local/bin:$PATH"; export PATH

                got=$(pg_dbmigrator --version | awk "{print \$2}")
                [ "$got" = "$EXPECT_VERSION" ] || {
                    echo "pg_dbmigrator version: want $EXPECT_VERSION, got $got"; exit 1;
                }

                pg_dump --version >/dev/null
                pg_restore --version >/dev/null

                major=$(pg_dump --version | sed -n "s/.*PostgreSQL) \([0-9]*\).*/\1/p")
                [ -n "$major" ] || { echo "could not parse pg_dump version"; exit 1; }
                [ "$major" -ge "$MIN_PG_MAJOR" ] || {
                    echo "pg_dump major $major < $MIN_PG_MAJOR -- install.sh fell back to the distro repo";
                    exit 1;
                }

                # Second run must be a no-op on the client side, not a reinstall.
                sh /install.sh > /tmp/install2.log 2>&1
                grep -q "already present" /tmp/install2.log || {
                    echo "second run did not detect the existing client"; cat /tmp/install2.log; exit 1;
                }

                echo "ok: pg_dbmigrator $got, pg_dump $major"
            '
        then
            passed=1
            break
        fi
        if (( attempt < ATTEMPTS )); then
            echo "    attempt $attempt failed, retrying once (distro mirrors are external)"
            sleep 5
        fi
    done

    if (( passed == 1 )); then
        echo "PASS: $image ($ARCH)"
    else
        echo "FAIL: $image ($ARCH)" >&2
        failures=$(( failures + 1 ))
    fi
done

echo
if (( failures > 0 )); then
    echo "FAIL: $failures/${#IMAGE_LIST[@]} images failed on $ARCH" >&2
    exit 1
fi
echo "PASS: install.sh works on all ${#IMAGE_LIST[@]} images for $ARCH"

# ── 4. version probe ────────────────────────────────────────────────────────
# Given connection strings, install.sh must install the client matching the
# newer of the two servers rather than simply the newest available. Pointing
# both at one PG 16 server makes the expected answer 16, which is distinct from
# the built-in default, so a regression that ignores the probe is visible.
PROBE_PG=16
PROBE_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')"
PG_PROBE_NAME="pg_dbmigrator_probe_$$"

echo
echo "==> version probe ($ARCH): PG $PROBE_PG on both ends should pick client $PROBE_PG"

docker run -d --name "$PG_PROBE_NAME" --platform "$PLATFORM" --network host \
    -e POSTGRES_PASSWORD=probe "postgres:${PROBE_PG}-alpine" \
    -c "port=$PROBE_PORT" >/dev/null

ready=0
for _ in $(seq 1 90); do
    if docker exec "$PG_PROBE_NAME" pg_isready -q -U postgres -p "$PROBE_PORT" 2>/dev/null; then
        ready=1
        break
    fi
    sleep 1
done
if (( ready == 0 )); then
    echo "FAIL: probe PostgreSQL $PROBE_PG never became ready" >&2
    exit 1
fi

PROBE_URL="postgres://postgres:probe@127.0.0.1:$PROBE_PORT/postgres"
if docker run --rm --platform "$PLATFORM" --network host \
    -v "$ROOT/install.sh:/install.sh:ro" \
    -e "PG_DBMIGRATOR_BASE_URL=http://127.0.0.1:$PORT" \
    -e "PG_DBMIGRATOR_VERSION=$TAG" \
    -e "PG_DBMIGRATOR_SOURCE=$PROBE_URL" \
    -e "PG_DBMIGRATOR_TARGET=$PROBE_URL" \
    -e "WANT_PG_MAJOR=$PROBE_PG" \
    ubuntu:22.04 sh -c '
        set -e
        apt-get update -qq >/dev/null 2>&1
        apt-get install -y -qq curl ca-certificates >/dev/null 2>&1

        sh /install.sh > /tmp/install.log 2>&1 || {
            echo "install.sh exited $?"; cat /tmp/install.log; exit 1;
        }
        grep -q "probed source/target" /tmp/install.log || {
            echo "install.sh did not probe the servers"; cat /tmp/install.log; exit 1;
        }

        PATH="$HOME/.local/bin:$PATH"; export PATH
        major=$(pg_dump --version | sed -n "s/.*PostgreSQL) \([0-9]*\).*/\1/p")
        [ "$major" = "$WANT_PG_MAJOR" ] || {
            echo "installed pg_dump $major, wanted $WANT_PG_MAJOR (probe ignored?)"; exit 1;
        }
        echo "ok: probe selected pg_dump $major"
    '
then
    echo "PASS: version probe ($ARCH)"
else
    echo "FAIL: version probe ($ARCH)" >&2
    exit 1
fi

echo
echo "PASS: install.sh verified on $ARCH (${#IMAGE_LIST[@]} distros + version probe)"
