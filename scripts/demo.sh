#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
java -jar target/ordertrace-1.0.0.jar publish data/demo.ndjson
