#!/bin/bash
K9DB_DIR="$(pwd)"
LOG_OUT="${K9DB_DIR}/experiments/scripts/logs/sar/"
PLOT_OUT="${K9DB_DIR}/experiments/scripts/outputs/sar/"

mkdir -p "$LOG_OUT"
mkdir -p "$PLOT_OUT"
mkdir -p /mnt/disks/my-ssd/k9db

echo "Writing plots and final outputs to $PLOT_OUT"
echo "Writing logs and temporary measurements to $LOG_OUT"

# Clean up all DBs and build K9db.
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel build //:k9db --config=opt

# Run K9db in background.
echo "Running K9db in background..."
bazel run //:k9db --config=opt -- --db_path="/mnt/disks/my-ssd/k9db/" \
  > "$LOG_OUT/k9db-server.out" 2>&1 &
pid=$!
sleep 20

# Prime DB.
echo "Prime DB..."
cd experiments/lobsters
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale 2.59 --reqscale 1000 --queries pelton \
  --backend pelton --prime --scale_everything \
  "mysql://root:password@127.0.0.1:10001/lobsters" \
  > "$LOG_OUT/prime.out" 2>&1
cd "$K9DB_DIR"

# Wait for data to be loaded completely.
sleep 10

# Display count of users to make sure all is correct.
mariadb -P10001 \
  -e "SELECT COUNT(*) FROM users" \
  > "$LOG_OUT/users.out" 2>&1
sleep 10

# Execute GET then EXECUTE FORGET
echo "GET TOP 1k USERS..."
echo "GDPR GET 1k (divide by 1k to get avg)" > "$PLOT_OUT/result.out"
(time mariadb -P10001 < experiments/sar/get.sql > "$LOG_OUT/get.out" 2>&1) 2>> "$PLOT_OUT/result.out"

echo "FORGET TOP 1k USERS..."
echo "GDOR FORGET top 1k (divide by 1k to get avg)" >> "$PLOT_OUT/result.out"
(time mariadb -P10001 < experiments/sar/forget.sql > "$LOG_OUT/forget.out" 2>&1) 2>> "$PLOT_OUT/result.out"

# Done!
kill $pid
wait $pid

echo "Terminated"
