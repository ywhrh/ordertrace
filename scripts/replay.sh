#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
namespace="${1:-replay_$(date -u +%Y%m%d_%H%M%S)}"
java -jar target/ordertrace-1.0.0.jar replay "$namespace"
