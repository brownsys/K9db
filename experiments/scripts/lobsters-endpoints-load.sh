#!/bin/bash
K9DB_DIR="$(pwd)"
LOG_OUT="${K9DB_DIR}/experiments/scripts/logs/lobsters/"
PLOT_OUT="${K9DB_DIR}/experiments/scripts/outputs/lobsters-endpoints/"

mkdir -p "$LOG_OUT"
mkdir -p "$PLOT_OUT"

echo "Writing plots and final outputs to $PLOT_OUT"
echo "Writing logs and temporary measurements to $LOG_OUT"
cd "${K9DB_DIR}/experiments/lobsters"

MEMCACHED_USER=""
if [[ "$(whoami)" == "root" ]]; then
  MEMCACHED_USER="-u memcached"
fi

# Experiment parameters: you need to change the request scale `RS` parameter if
# running on a different configuration.
RT=120
DS=2.59  # Data scale: corresponds to 15k users
RS=1000  # request scale
INFLIGHT=""
# INFLIGHT="--in-flight=15"
TARGET_IP="$1"

#
# Build harness.
#
bazel build -c opt //:lobsters-harness

#
# Baseline
# Run lobsters harness against baseline, find DB size, find memcached memory
# overhead agaisnt DB.
#
echo "Running against baseline..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend rocks-mariadb --prime --scale_everything $INFLIGHT \
  "mysql://k9db:password@$TARGET_IP:3306/lobsters" \
  > "$LOG_OUT/baseline-prime.out" 2>&1

bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend rocks-mariadb --scale_everything \
  "mysql://k9db:password@$TARGET_IP:3306/lobsters" \
  > "$LOG_OUT/baseline.out" 2>&1

# Show DB size of baseline.
mariadb -P3306 --host=$TARGET_IP -u k9db -ppassword \
  -e "SELECT table_schema AS 'Database', SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema" \
  > "$LOG_OUT/baseline-memory.out" 2>&1

# Execute memcached experiment to find out how much memory memcached would take.
echo "Running memcached experiment against baseline..."
cd "$K9DB_DIR/experiments/memcached"

# Run memcached server.
bazel build @memcached//:memcached --config=opt
bazel run @memcached//:memcached --config=opt -- $MEMCACHED_USER -m 1024 -M > "$LOG_OUT/memcached-server.log" 2>&1 &
pid=$!
sleep 30

# Run memcached experiment
bazel build //memcached:memcached --config=opt
bazel run //memcached:memcached --config=opt -- --database=$TARGET_IP \
  > "$LOG_OUT/memcached-memory.out" 2>&1
kill $pid

# Send signal to server to kill mariadb and start k9db.
mariadb -P10001 --host=$TARGET_IP -e "STOP" > /dev/null 2>&1;

# Sleep long enough for K9db to start on remote.
sleep 60

#
# K9db (encrypted)
#

cd "${K9DB_DIR}/experiments/lobsters"
echo "Running against K9db..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --prime --scale_everything $INFLIGHT \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$LOG_OUT/_prime_scale$DS.out" 2>&1

bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$LOG_OUT/scale$DS.out" 2>&1

# Show K9db in-memory cache size.
mariadb -P10001 --host=$TARGET_IP -e "SHOW MEMORY" > "$LOG_OUT/k9db-memory.out" 2>&1

# Send signal to server to kill k9db and start unencrypted k9db.
mariadb -P10001 --host=$TARGET_IP -e "STOP" > /dev/null 2>&1;
sleep 60

#
# K9db (unencrypted)
#

echo "Running against K9db (unencrypted)..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --prime --scale_everything $INFLIGHT \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$LOG_OUT/unencrypted-prime.out" 2>&1

bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$LOG_OUT/unencrypted.out" 2>&1

mariadb -P10001 --host=$TARGET_IP -e "STOP" > /dev/null 2>&1;

#
# Plotting and other formatting.
#
echo "Experiment ran. Now plotting"
cd "${K9DB_DIR}/experiments/scripts/plotting"
. venv/bin/activate
python lobsters-endpoints.py --paper

