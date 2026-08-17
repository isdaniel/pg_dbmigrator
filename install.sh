#!/bin/sh
# pg_dbmigrator installer.
#
#   curl -fsSL https://raw.githubusercontent.com/isdaniel/pg_dbmigrator/main/install.sh | sh
#
# Installs two things:
#   1. the pg_dbmigrator binary (static musl, no root needed)
#   2. pg_dump / pg_restore, which pg_dbmigrator shells out to and cannot work without
#
# Configuration is via environment variables only. Under `curl | sh` the script IS
# stdin, so argv and prompts do not survive the pipe -- env vars do.
#
#   PG_DBMIGRATOR_VERSION    git tag to install, e.g. v0.2.1        (default: latest)
#   PG_DBMIGRATOR_BIN_DIR    where the binary goes                  (default: ~/.local/bin)
#   PG_DBMIGRATOR_BASE_URL   override release download base         (mirrors, air-gapped, CI)
#   PG_DBMIGRATOR_SKIP_DEPS  1 = do not touch pg_dump/pg_restore    (default: 0)
#   PG_DBMIGRATOR_SOURCE     source libpq URI -- probed for its major version
#   PG_DBMIGRATOR_TARGET     target libpq URI -- probed for its major version
#   PG_MAJOR                 force a PostgreSQL client major, skipping the probe
#
# When both connection strings are given, the freshly installed binary is asked
# what the two servers actually run (over the wire protocol, so this works
# before any client exists) and the client matching the *newer* of the two is
# installed. That is the floor pg_dump needs, and stopping there rather than
# always taking the newest available avoids a newer pg_restore emitting
# settings an older target does not recognise. Without them, the newest
# client is installed, which is safe for any supported source.
#
# Exit codes:
#   0 ok   2 unsupported CPU arch   3 no way to get root   4 unsupported distro
#   5 pg client install failed      6 pg client too old for the servers
#   1 everything else
#
# The binary install and the pg client install are independent: if the client step
# fails the binary is still installed and usable once a client is present, so that
# step degrades to a warning rather than taking the whole run down.

set -eu

REPO='isdaniel/pg_dbmigrator'
BIN_NAME='pg_dbmigrator'

: "${PG_DBMIGRATOR_VERSION:=latest}"
: "${PG_DBMIGRATOR_BIN_DIR:=${HOME:-/root}/.local/bin}"
: "${PG_DBMIGRATOR_BASE_URL:=}"
: "${PG_DBMIGRATOR_SKIP_DEPS:=0}"
# An explicit PG_MAJOR wins over the source/target probe.
if [ -n "${PG_MAJOR:-}" ]; then PG_MAJOR_EXPLICIT=1; else PG_MAJOR_EXPLICIT=0; fi
: "${PG_MAJOR:=18}"
# It is interpolated into package names handed to the package manager and
# compared with `-ge`, so anything but digits is rejected up front rather than
# turning into a shell error halfway through the install.
case "$PG_MAJOR" in
	*[!0-9]*|'') printf 'error: PG_MAJOR must be a major version number, got "%s"\n' "$PG_MAJOR" >&2; exit 1 ;;
esac

# apt.postgresql.org only publishes these suites. Interpolating $VERSION_CODENAME
# blindly writes a 404 repo into /etc/apt/ and breaks every later apt-get on the box.
# Needs a look roughly twice a year as suites are added and archived.
PGDG_APT_SUITES='bullseye bookworm trixie forky sid jammy noble resolute'
PGDG_APT_URI='https://apt.postgresql.org/pub/repos/apt'
PGDG_APT_KEY_URL='https://www.postgresql.org/media/keys/ACCC4CF8.asc'
PGDG_APT_KEYRING='/usr/share/keyrings/postgresql-pgdg.asc'
PGDG_YUM_REPORPM='https://download.postgresql.org/pub/repos/yum/reporpms'

SUDO=''
TMPDIR_INSTALL=''
# Major of the pg_dump we ended up with, for the "too old" report.
PG_MAJOR_GOT=''

# ---------------------------------------------------------------- output

log()  { printf '%s\n' "$*"; }
info() { printf '\033[32m==>\033[0m %s\n' "$*"; }
# Multi-line reports use notice() on stdout: stdout and stderr are buffered
# independently once the output is piped, so a stderr heading lands in the middle
# of its own stdout body. Only die() is worth splitting off to stderr.
notice() { printf '\033[33mwarning:\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarning:\033[0m %s\n' "$*" >&2; }
die()  { code=$1; shift; printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit "$code"; }

cleanup() { if [ -n "$TMPDIR_INSTALL" ]; then rm -rf "$TMPDIR_INSTALL"; fi; }
# A bare `trap cleanup INT` deletes the temp dir and then *resumes* the script,
# which would leave the pgdg keyring/sources steps reading files that no longer
# exist. Signals have to end the run.
on_signal() { cleanup; trap - EXIT; exit 130; }

# ---------------------------------------------------------------- helpers

have() { command -v "$1" >/dev/null 2>&1; }

# Read one field from /etc/os-release without leaking it into our own scope, and
# without tripping `set -u` on the many distros that simply omit a field (Debian,
# Alpine and Fedora all ship no ID_LIKE at all).
osr() {
	[ -r /etc/os-release ] || return 0
	# shellcheck disable=SC1091
	( . /etc/os-release 2>/dev/null || exit 0
	  eval "printf '%s' \"\${$1:-}\"" )
}

http_get() {
	if have curl; then
		curl -fsSL "$1"
	elif have wget; then
		wget -qO- "$1"
	else
		die 1 'need curl or wget to download anything'
	fi
}

http_get_to() {
	if have curl; then
		curl -fsSL -o "$2" "$1"
	elif have wget; then
		wget -qO "$2" "$1"
	else
		die 1 'need curl or wget to download anything'
	fi
}

# ---------------------------------------------------------------- privileges

# Never `exec sudo "$0"`: under `curl | sh` the value of $0 is literally "sh", and
# the re-exec'd shell inherits the half-consumed pipe -- which has been observed to
# run a mangled tail of this very script as root. Prefix individual commands instead.
pick_sudo() {
	if [ "$(id -u)" -eq 0 ]; then
		# Alpine and Amazon Linux base images ship neither sudo nor doas but run as
		# uid 0, which is exactly where people test installers.
		SUDO=''
		return 0
	fi
	if have sudo && sudo -n true 2>/dev/null; then
		SUDO='sudo'
		return 0
	fi
	if have doas && doas -n true 2>/dev/null; then
		SUDO='doas'
		return 0
	fi
	return 1
}

# `sudo -n` above rules out a password prompt, so every privileged command is
# guaranteed non-interactive. DEBIAN_FRONTEND has to live inside the command string:
# sudo's default env_reset strips it from the surrounding environment, and `sudo -E`
# hard-fails under a narrowed sudoers rule.
as_root() {
	if [ -z "$SUDO" ]; then
		sh -c "$1"
	else
		$SUDO sh -c "$1"
	fi
}

# ---------------------------------------------------------------- binary

detect_target() {
	case "$(uname -s)" in
		Linux) ;;
		*) die 2 "this installer only supports Linux (got $(uname -s)); use \`cargo install $BIN_NAME\`" ;;
	esac
	case "$(uname -m)" in
		x86_64|amd64)   echo 'x86_64-unknown-linux-musl' ;;
		aarch64|arm64)  echo 'aarch64-unknown-linux-musl' ;;
		*) die 2 "no prebuilt binary for $(uname -m); use \`cargo install $BIN_NAME\`" ;;
	esac
}

resolve_version() {
	[ "$PG_DBMIGRATOR_VERSION" != 'latest' ] && { echo "$PG_DBMIGRATOR_VERSION"; return 0; }

	if have curl; then
		# Follow the /releases/latest redirect rather than hitting api.github.com,
		# which rate-limits unauthenticated callers to 60/hour per IP.
		_url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
			"https://github.com/$REPO/releases/latest" 2>/dev/null) || _url=''
		case "$_url" in
			*/releases/tag/*) echo "${_url##*/}"; return 0 ;;
		esac
	fi

	_tag=$(http_get "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null \
		| sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1) || _tag=''
	[ -n "$_tag" ] || die 1 'could not determine the latest release; set PG_DBMIGRATOR_VERSION=vX.Y.Z'
	echo "$_tag"
}

install_binary() {
	target=$(detect_target)
	tag=$(resolve_version)
	base=${PG_DBMIGRATOR_BASE_URL:-https://github.com/$REPO/releases/download/$tag}
	tarball="$BIN_NAME-$tag-$target.tar.gz"

	info "installing $BIN_NAME $tag ($target)"

	TMPDIR_INSTALL=$(mktemp -d)
	trap cleanup EXIT
	trap on_signal INT TERM

	http_get_to "$base/$tarball" "$TMPDIR_INSTALL/$tarball" \
		|| die 1 "download failed: $base/$tarball"
	http_get_to "$base/checksums.txt" "$TMPDIR_INSTALL/checksums.txt" \
		|| die 1 "download failed: $base/checksums.txt"

	expected=$(awk -v f="$tarball" '$2 == f || $2 == "*" f { print $1 }' \
		"$TMPDIR_INSTALL/checksums.txt")
	[ -n "$expected" ] || die 1 "$tarball is not listed in checksums.txt"

	if have sha256sum; then
		actual=$(sha256sum "$TMPDIR_INSTALL/$tarball" | cut -d' ' -f1)
	elif have shasum; then
		actual=$(shasum -a 256 "$TMPDIR_INSTALL/$tarball" | cut -d' ' -f1)
	else
		die 1 'need sha256sum or shasum to verify the download'
	fi
	[ "$expected" = "$actual" ] \
		|| die 1 "checksum mismatch for $tarball (expected $expected, got $actual)"

	tar -xzf "$TMPDIR_INSTALL/$tarball" -C "$TMPDIR_INSTALL"
	[ -f "$TMPDIR_INSTALL/$BIN_NAME" ] || die 1 "$BIN_NAME not found inside $tarball"

	mkdir -p "$PG_DBMIGRATOR_BIN_DIR"
	install -m 0755 "$TMPDIR_INSTALL/$BIN_NAME" "$PG_DBMIGRATOR_BIN_DIR/$BIN_NAME" 2>/dev/null \
		|| { cp "$TMPDIR_INSTALL/$BIN_NAME" "$PG_DBMIGRATOR_BIN_DIR/$BIN_NAME" \
		     && chmod 0755 "$PG_DBMIGRATOR_BIN_DIR/$BIN_NAME"; }

	info "installed $PG_DBMIGRATOR_BIN_DIR/$BIN_NAME"
}

# ---------------------------------------------------------------- pg client

# On Debian and Ubuntu, postgresql-client-common alone installs /usr/bin/pg_dump as a
# symlink to pg_wrapper. `command -v pg_dump` and `[ -x /usr/bin/pg_dump ]` both pass
# while running it just prints "You must install at least one postgresql-client-N
# package" and exits 1. Only the exit code of --version tells the truth.
have_pg_client() {
	pg_dump --version >/dev/null 2>&1 && pg_restore --version >/dev/null 2>&1
}

# Major version of the pg_dump on $PATH, or nothing if there is no usable one.
installed_pg_major() {
	if ! have_pg_client; then
		return 0
	fi
	pg_dump --version 2>/dev/null | sed -n 's/.*PostgreSQL) \([0-9][0-9]*\).*/\1/p'
}

# Ask the just-installed binary what the two servers run and return the newer
# major. Needs both connection strings; stays silent and fails otherwise so the
# caller can fall back to the newest client.
probe_required_major() {
	if [ -z "${PG_DBMIGRATOR_SOURCE:-}" ] || [ -z "${PG_DBMIGRATOR_TARGET:-}" ]; then
		return 1
	fi
	probed=$("$PG_DBMIGRATOR_BIN_DIR/$BIN_NAME" --print-client-major 2>/dev/null) || return 1
	case "$probed" in
		'' | *[!0-9]*) return 1 ;;
	esac
	printf '%s' "$probed"
}

manual_hint() {
	case "$1" in
		debian|ubuntu) echo "sudo apt-get install -y postgresql-client-$PG_MAJOR" ;;
		alpine)        echo 'sudo apk add --no-cache postgresql-client' ;;
		amzn)          echo "sudo dnf install -y postgresql$PG_MAJOR" ;;
		fedora)        echo 'sudo dnf install -y postgresql' ;;
		opensuse*|sles) echo "sudo zypper -n install postgresql$PG_MAJOR" ;;
		rhel|rocky|almalinux|ol|centos) echo "sudo dnf install -y postgresql$PG_MAJOR" ;;
		*)             echo 'install the PostgreSQL client package for your distro' ;;
	esac
}

install_pgdg_apt() {
	codename=$(osr UBUNTU_CODENAME)
	# Ubuntu derivatives (Mint, Pop!_OS, elementary) put their own name in
	# VERSION_CODENAME and the upstream one in UBUNTU_CODENAME. Prefer upstream.
	[ -n "$codename" ] || codename=$(osr VERSION_CODENAME)
	[ -n "$codename" ] || return 1

	found=1
	for s in $PGDG_APT_SUITES; do
		if [ "$s" = "$codename" ]; then
			found=0
			break
		fi
	done
	if [ "$found" -ne 0 ]; then
		warn "$codename is not published on apt.postgresql.org"
		return 1
	fi

	# Docker base images ship an empty /var/lib/apt/lists, so `apt-get install
	# ca-certificates` fails with "Unable to locate package" without this first.
	as_root 'DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get update -qq'
	as_root 'DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get install -y --no-install-recommends ca-certificates' \
		|| return 1

	http_get "$PGDG_APT_KEY_URL" > "$TMPDIR_INSTALL/pgdg.asc" || return 1
	as_root "mkdir -p $(dirname "$PGDG_APT_KEYRING")"
	if [ -z "$SUDO" ]; then
		cp "$TMPDIR_INSTALL/pgdg.asc" "$PGDG_APT_KEYRING"
	else
		$SUDO cp "$TMPDIR_INSTALL/pgdg.asc" "$PGDG_APT_KEYRING"
	fi

	# The setup snippet on postgresql.org hardcodes `Architectures: amd64`; copying
	# it verbatim silently breaks arm64, where PGDG does publish packages.
	arch=$(dpkg --print-architecture 2>/dev/null || echo amd64)
	{
		echo 'Types: deb'
		echo "URIs: $PGDG_APT_URI"
		echo "Suites: $codename-pgdg"
		echo 'Components: main'
		echo "Architectures: $arch"
		echo "Signed-By: $PGDG_APT_KEYRING"
	} > "$TMPDIR_INSTALL/pgdg.sources"
	if [ -z "$SUDO" ]; then
		cp "$TMPDIR_INSTALL/pgdg.sources" /etc/apt/sources.list.d/pgdg.sources
	else
		$SUDO cp "$TMPDIR_INSTALL/pgdg.sources" /etc/apt/sources.list.d/pgdg.sources
	fi

	as_root 'DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get update -qq' || return 1
	as_root "DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a apt-get install -y postgresql-client-$PG_MAJOR"
}

install_pgdg_yum() {
	el=$(osr VERSION_ID)
	el=${el%%.*}   # Rocky/Alma/RHEL report "9.8", not "9"
	case "$el" in
		8|9|10) ;;
		*) warn "no PGDG repo for EL$el"; return 1 ;;
	esac

	# The repo RPM is noarch but lives under an arch-specific path, and the aarch64
	# one ships a differently named GPG key -- let the RPM own the key, never
	# hardcode its path.
	as_root "dnf -y install $PGDG_YUM_REPORPM/EL-$el-$(uname -m)/pgdg-redhat-repo-latest.noarch.rpm" \
		|| return 1

	# Required on EL8, where the stock postgresql module masks the PGDG packages.
	# EL9 does not need it and EL10 ships no modular metadata at all, where this
	# exits 1 and would abort the whole script under `set -e`.
	if [ "$el" = '8' ]; then
		as_root 'dnf -qy module disable postgresql' || true
	fi

	as_root "dnf -y install postgresql$PG_MAJOR"
}

install_amzn() {
	ver=$(osr VERSION_ID)
	case "$ver" in
		2023) ;;
		*) warn "Amazon Linux $ver is EOL and cannot provide a client new enough to dump PostgreSQL 16+"
		   return 1 ;;
	esac

	# The majors all Conflicts: postgresql-any with each other, and a glob install
	# tries them all at once and fails. Walk down explicitly: which majors exist
	# depends on the repo snapshot the AMI is pinned to.
	for v in "$PG_MAJOR" 18 17 16 15; do
		if as_root "dnf install -y postgresql$v" >/dev/null 2>&1; then
			return 0
		fi
	done
	return 1
}

install_alpine() {
	# postgresql-client is a virtual provide resolved to the newest major. Try
	# the pinned package first so a probed major is honoured, but fall back:
	# which majors exist moves between Alpine releases.
	if ! as_root "apk add --no-cache postgresql$PG_MAJOR-client" >/dev/null 2>&1; then
		as_root 'apk add --no-cache postgresql-client' || return 1
	fi
	# If an older postgresqlNN-client was already present, /usr/libexec/postgresql
	# still points at it and pg_dump keeps reporting the old version. Never pass
	# --no-scripts: the /usr/bin symlinks come from a package trigger.
	if have pg_versions; then
		as_root 'pg_versions set-default latest' >/dev/null 2>&1 || true
	fi
	return 0
}

install_pg_client() {
	id=$(osr ID)
	[ -n "$id" ] || { warn 'cannot identify this distro (/etc/os-release missing)'; return 4; }

	# Dispatch on ID before ID_LIKE. Amazon Linux 2023 reports ID_LIKE=fedora and
	# would otherwise be routed into the EL path, where the PGDG repo RPM fails on
	# a missing /etc/redhat-release.
	case "$id" in
		debian|ubuntu|linuxmint|pop|elementary|neon|zorin|raspbian)
			install_pgdg_apt || return 5 ;;
		rhel|rocky|almalinux|ol|centos)
			install_pgdg_yum || return 5 ;;
		amzn)
			install_amzn || return 5 ;;
		alpine)
			install_alpine || return 5 ;;
		fedora)
			as_root 'dnf install -y postgresql' || return 5 ;;
		opensuse*|sles|sled)
			as_root "zypper -n install postgresql$PG_MAJOR" || return 5 ;;
		*)
			return 4 ;;
	esac
	return 0
}

# ---------------------------------------------------------------- reporting

report_pg_client_failure() {
	code=$1
	id=$(osr ID)
	log ''
	if [ "$code" -eq 6 ]; then
		# The package step exited 0 here -- the client it produced is simply
		# behind the servers, so "not available yet" would be misleading.
		notice "$BIN_NAME is installed, but pg_dump is PostgreSQL ${PG_MAJOR_GOT:-unknown}, older than the $PG_MAJOR your servers need."
		log "  This distro's packages do not reach $PG_MAJOR, so the migration would fail later."
	else
		notice "$BIN_NAME is installed, but pg_dump/pg_restore are not available yet."
		if [ "$code" -eq 3 ]; then
			log '  Installing them needs root, and this shell has no passwordless sudo/doas.'
		elif [ "$code" -eq 4 ]; then
			log "  This installer has no package recipe for '${id:-unknown}'."
		else
			log '  The package install did not succeed.'
		fi
	fi
	log '  Run this yourself, then re-run pg_dbmigrator:'
	log ''
	log "      $(manual_hint "${id:-unknown}")"
	log ''
	log "  Detected: ID=${id:-unknown} VERSION_ID=$(osr VERSION_ID) arch=$(uname -m)"
	log "  If that command is wrong for your system, please report it:"
	log "      https://github.com/$REPO/issues"
	log ''
}

report_path() {
	case ":${PATH}:" in
		*":$PG_DBMIGRATOR_BIN_DIR:"*) return 0 ;;
	esac
	log ''
	notice "$PG_DBMIGRATOR_BIN_DIR is not in your PATH."
	log '  Add this to your shell profile:'
	log ''
	log "      export PATH=\"$PG_DBMIGRATOR_BIN_DIR:\$PATH\""
	log ''
	log '  Or install somewhere already on PATH:'
	log ''
	log "      PG_DBMIGRATOR_BIN_DIR=/usr/local/bin  (needs root)"
	log ''
}

# ---------------------------------------------------------------- main

# Everything lives in main() so that a truncated download cannot execute a partial
# script -- half of this one would have already rewritten /etc/apt.
main() {
	install_binary

	pg_status=0
	if [ "$PG_DBMIGRATOR_SKIP_DEPS" = '1' ]; then
		info 'skipping PostgreSQL client install (PG_DBMIGRATOR_SKIP_DEPS=1)'
	else
		# The binary is in place now, so it can tell us what the servers run
		# even though no pg_dump exists yet.
		pg_major_required=0
		if [ "$PG_MAJOR_EXPLICIT" = '0' ]; then
			probed=$(probe_required_major) || probed=''
			if [ -n "$probed" ]; then
				PG_MAJOR="$probed"
				pg_major_required=1
				info "probed source/target: PostgreSQL $PG_MAJOR client is the match"
			elif [ -n "${PG_DBMIGRATOR_SOURCE:-}" ] && [ -n "${PG_DBMIGRATOR_TARGET:-}" ]; then
				# Both URLs were given, so a failure here is worth saying out
				# loud rather than quietly installing the newest client.
				warn "could not read the source/target server versions; falling back to PostgreSQL $PG_MAJOR"
			fi
		fi

		installed=$(installed_pg_major)
		if [ -n "$installed" ] && [ "$installed" -ge "$PG_MAJOR" ]; then
			info "pg_dump already present (PostgreSQL $installed)"
		elif ! pick_sudo; then
			pg_status=3
		else
			if [ -n "$installed" ]; then
				info "pg_dump $installed is older than the required $PG_MAJOR -- upgrading"
			fi
			info "installing PostgreSQL client tools (major $PG_MAJOR)"
			set +e
			install_pg_client
			pg_status=$?
			set -e
			# Package managers exit 0 in situations where the binaries still
			# are not usable (see the pg_wrapper note above), so trust only the
			# probe.
			if [ "$pg_status" -eq 0 ] && ! have_pg_client; then
				pg_status=5
			fi
			# Several branches fall back to whatever major the distro actually
			# carries (Alpine's virtual postgresql-client, the Amazon Linux
			# descending loop, Fedora's unversioned package). That is fine when
			# PG_MAJOR was only our newest-available default, but when it came
			# from the servers it is a hard floor -- reporting "ready" with an
			# older client would just move the failure into the migration.
			if [ "$pg_status" -eq 0 ] && [ "$pg_major_required" = '1' ]; then
				PG_MAJOR_GOT=$(installed_pg_major)
				if [ -z "$PG_MAJOR_GOT" ] || [ "$PG_MAJOR_GOT" -lt "$PG_MAJOR" ]; then
					warn "installed pg_dump ${PG_MAJOR_GOT:-none} is older than the PostgreSQL $PG_MAJOR your servers need"
					pg_status=6
				fi
			fi
		fi
	fi

	if [ "$pg_status" -ne 0 ]; then
		report_pg_client_failure "$pg_status"
	elif [ "$PG_DBMIGRATOR_SKIP_DEPS" != '1' ]; then
		info "pg_dump ready ($(pg_dump --version 2>/dev/null | head -n 1))"
	fi

	report_path

	log ''
	if [ "$pg_status" -eq 6 ]; then
		die "$pg_status" "$BIN_NAME installed, but pg_dump is older than PostgreSQL $PG_MAJOR"
	elif [ "$pg_status" -ne 0 ]; then
		# The binary is installed and usable, but the one-command promise was not
		# kept. Exiting 0 here would let a Dockerfile ship an image that only fails
		# at migration time; fail now instead, loudly and with the fix in hand.
		# Set PG_DBMIGRATOR_SKIP_DEPS=1 if you manage the client yourself.
		die "$pg_status" "$BIN_NAME installed, but pg_dump/pg_restore are missing"
	fi
	info 'done'
	log "  $PG_DBMIGRATOR_BIN_DIR/$BIN_NAME --help"
	log ''
}

main "$@"
