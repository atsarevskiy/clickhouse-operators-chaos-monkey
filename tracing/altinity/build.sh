#!/usr/bin/env bash
# Builds a trace-instrumented Altinity clickhouse-operator image.
#
#   release-<version> ──clone──> temp dir ──git apply release-<version>.patch──> dev/go_build_operator.sh
#        │                                                                               │
#        └── altinity/clickhouse-operator:<version> ──FROM── COPY clickhouse-operator ──> clickhouse-operator:trace-<version>
#
# Usage: build.sh [version]   (default 0.27.4)
# Requires git, go and docker on PATH. GOPROXY, GOFLAGS and GOTOOLCHAIN are honoured if set.
# A local clone with release tags at ${CHOP_MIRROR:-/tmp/chop} is used instead of the network when present.
set -euo pipefail

VERSION="${1:-0.27.4}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH="${SCRIPT_DIR}/release-${VERSION}.patch"
REPO="https://github.com/Altinity/clickhouse-operator"
MIRROR="${CHOP_MIRROR:-/tmp/chop}"
REF="release-${VERSION}"
BASE_IMAGE="altinity/clickhouse-operator:${VERSION}"
TAG="clickhouse-operator:trace-${VERSION}"

export GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-auto}"

if [[ ! -f "${PATCH}" ]]; then
    echo "patch not found: ${PATCH}" >&2
    exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

if [[ -d "${MIRROR}/.git" ]] && git -C "${MIRROR}" rev-parse -q --verify "${REF}^{commit}" >/dev/null; then
    git -c init.defaultBranch=main clone -q --no-checkout "${MIRROR}" "${WORK}/src"
else
    git -c init.defaultBranch=main clone -q --no-checkout --filter=blob:none "${REPO}" "${WORK}/src"
fi
git -C "${WORK}/src" -c advice.detachedHead=false checkout -q --detach "${REF}"
git -C "${WORK}/src" apply "${PATCH}"

docker pull -q "${BASE_IMAGE}" >/dev/null
ARCH="$(docker image inspect "${BASE_IMAGE}" --format '{{.Architecture}}')"
ENTRYPOINT="$(docker image inspect "${BASE_IMAGE}" --format '{{index .Config.Entrypoint 0}}')"

mkdir -p "${WORK}/image"
(
    cd "${WORK}/src"
    OPERATOR_BIN="${WORK}/image/clickhouse-operator" GOARCH="${ARCH}" bash ./dev/go_build_operator.sh
) > "${WORK}/build.log" 2>&1 || {
    tail -n 40 "${WORK}/build.log" >&2
    exit 1
}
grep -q "Build OK" "${WORK}/build.log"

cat > "${WORK}/image/Dockerfile" <<EOF
FROM ${BASE_IMAGE}
COPY clickhouse-operator ${ENTRYPOINT}
EOF

docker build -q -t "${TAG}" "${WORK}/image" >/dev/null
echo "${TAG}"
