#!/bin/bash
# Regenerates (re-vendors) the bazel BUILD files for all rust (cargo)
# dependencies using cargo-raze. The generated files under **/cargo/ are
# checked into git so that builds do not depend on cargo-raze.
#
# Run this inside the dev container (see Dockerfile.dev) from the repo root
# after changing any Cargo.toml, then commit and push the regenerated files.
# Note: when the repo is bind-mounted into the container, the regenerated
# files will be owned by root on the host; chown them before committing.
set -euo pipefail
cd "$(dirname "$0")"

# cargo-raze is installed in the dev container for root.
CARGO="${CARGO:-/root/.cargo/bin/cargo}"

# Quirk: our cargo-raze fork expects this directory to exist.
mkdir -p /tmp/cargo-raze/doesnt/exist/

for project in k9db/proxy experiments/lobsters experiments/ownCloud experiments/vote; do
  echo "Vendoring rust dependencies for ${project} ..."
  (cd "${project}" && "${CARGO}" raze)
done

echo "Done. Review, commit, and push the changes under **/cargo/."
