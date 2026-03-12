#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
PREFIX="flyerlink-hosting/ansible/postgres-storage"

cd "$ROOT"
BRANCH=$(git subtree split --prefix="$PREFIX")
git push postgres-storage "$BRANCH":master --force
