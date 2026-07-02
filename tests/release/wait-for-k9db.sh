#!/bin/bash
# Waits until the k9db proxy in the given container is listening (or fails
# after 60 seconds). Usage: wait-for-k9db.sh <container-name>
set -u
CONTAINER="${1:-k9db}"

for i in $(seq 1 60); do
  if docker logs "${CONTAINER}" 2>&1 | grep -q "Listening"; then
    docker logs "${CONTAINER}"
    exit 0
  fi
  sleep 1
done

echo "k9db did not come up within 60 seconds:"
docker logs "${CONTAINER}"
exit 1
