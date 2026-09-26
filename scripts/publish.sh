#!/usr/bin/env bash
set -euo pipefail
version=$(python3 - <<'PY'
import re
text = open("mix.exs").read()
match = re.search(r'version: "([0-9.]+)"', text)
if not match:
    raise SystemExit("version missing from mix.exs")
print(match.group(1))
PY
)
tag="${GITHUB_REF_NAME:-}"
if [[ -n "$tag" && "$tag" != "v${version}" ]]; then
  echo "tag ${tag} does not match package version ${version}" >&2
  exit 1
fi
mix local.hex --force
mix deps.get
if [[ "${DRY_RUN:-0}" == 1 ]]; then
  mix hex.build
  exit 0
fi
if [[ -z "${HEX_API_KEY:-}" ]]; then
  echo "HEX_API_KEY is not set" >&2
  exit 1
fi
mix hex.publish --yes
