#!/usr/bin/env bash
# Builds a trace-instrumented ClickHouse Inc. clickhouse-operator image.
#
#   upstream tag ──clone──> temp dir ──git apply <version>.patch──> go build (static, CGO off)
#        │                                                               │
#        └── ghcr.io/clickhouse/clickhouse-operator:<version> ──FROM── COPY manager ──> clickhouse-operator-official:trace-<version>
#
# Usage: build.sh [version]   (default v0.0.8)
# Requires git, go and docker on PATH. GOPROXY, GOFLAGS and GOTOOLCHAIN are honoured if set.
set -euo pipefail

VERSION="${1:-v0.0.8}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH="${SCRIPT_DIR}/${VERSION}.patch"
REPO="https://github.com/ClickHouse/clickhouse-operator"
BASE_IMAGE="ghcr.io/clickhouse/clickhouse-operator:${VERSION}"
TAG="clickhouse-operator-official:trace-${VERSION}"
VERSION_PKG="github.com/ClickHouse/clickhouse-operator/internal/version"

if [[ ! -f "${PATCH}" ]]; then
    echo "patch not found: ${PATCH}" >&2
    exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

git -c advice.detachedHead=false clone -q --depth 1 --branch "${VERSION}" "${REPO}" "${WORK}/src"
git -C "${WORK}/src" apply "${PATCH}"

docker pull -q "${BASE_IMAGE}" >/dev/null
ARCH="$(docker image inspect "${BASE_IMAGE}" --format '{{.Architecture}}')"
ENTRYPOINT="$(docker image inspect "${BASE_IMAGE}" --format '{{index .Config.Entrypoint 0}}')"

(
    cd "${WORK}/src"
    CGO_ENABLED=0 GOOS=linux GOARCH="${ARCH}" go build \
        -ldflags "-X ${VERSION_PKG}.Version=${VERSION}-chaostrace -X ${VERSION_PKG}.GitCommitHash=$(git rev-parse HEAD) -X ${VERSION_PKG}.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        -o "${WORK}/manager" cmd/main.go
)

cat > "${WORK}/Dockerfile" <<EOF
FROM ${BASE_IMAGE}
COPY manager ${ENTRYPOINT}
EOF

docker build -q -t "${TAG}" "${WORK}" >/dev/null
echo "${TAG}"
