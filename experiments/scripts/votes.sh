#!/bin/bash
K9DB_DIR="$(pwd)"
LOG_OUT="${K9DB_DIR}/experiments/scripts/logs/votes/"
PLOT_OUT="${K9DB_DIR}/experiments/scripts/outputs/votes/"

mkdir -p "$LOG_OUT"
mkdir -p "$PLOT_OUT"
mkdir -p /mnt/disks/my-ssd/k9db

echo "Writing plots and final outputs to $PLOT_OUT"
echo "Writing logs and temporary measurements to $LOG_OUT"
cd "${K9DB_DIR}/experiments/vote"

# The database IP is either 127.0.0.1 or LOCAL_IP if running on gcloud.
DB_IP=$LOCAL_IP
if [[ $LOCAL_IP == "" ]]; then
  DB_IP="127.0.0.1"
fi
echo $DB_IP

# Experiment parameters.
articles=10000
distribution="skewed"
target=10000
write=19
runtime=60
warmup=60
prime=30

#
# MariaDB baseline
#
echo "Running against MariaDB..."
sudo service mariadb start

# Baseline mariadb.
echo "  Priming..."
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every 1 \
  --runtime $prime \
  mysql --address "$DB_IP:3306" > $LOG_OUT/baseline-prime.out 2>&1

echo "  Running load..."
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $runtime \
  --no-prime \
  mysql --address "$DB_IP:3306"  > $LOG_OUT/baseline.out 2>&1

#
# Mariadb+memcached
#
echo "Running against MariaDB+memcached..."

# Running memcached server in background.
cd "$K9DB_DIR/experiments/memcached"
bazel run @memcached//:memcached --config=opt -- -m 1024 -M > "$LOG_OUT/memcached-server.log" 2>&1 &
pid=$!
sleep 30

# Running harness.
cd "${K9DB_DIR}/experiments/vote"
echo "  Priming..."
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every 1 \
  --runtime $prime \
  memcached-hybrid --mysql-address "$DB_IP:3306" > $LOG_OUT/hybrid-prime.out 2>&1
  
echo "  Warmup...."
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $warmup \
  --no-prime \
  memcached-hybrid --mysql-address "$DB_IP:3306" > $LOG_OUT/hybrid-warmup.out 2>&1

echo "  Running load..."
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $runtime \
  --no-prime \
  memcached-hybrid --mysql-address "$DB_IP:3306"  > $LOG_OUT/hybrid.out 2>&1

# Kill memcached and mariadb
kill $pid
sudo service mariadb stop

#
# K9db.
#

echo "Running against K9db..."

# Running K9db server in background.
cd ${K9DB_DIR}
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel run //:k9db --config=opt -- --db_path="/mnt/disks/my-ssd/k9db/" \
  > "$LOG_OUT/k9db-server.out" 2>&1 &
sleep 10

# Running harness.
cd "${K9DB_DIR}/experiments/vote"
echo "  Priming"
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every 1 \
  --runtime $prime \
  pelton > $LOG_OUT/k9db-prime.out 2>&1

echo "  Running load..."
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $runtime \
  --no-prime \
  pelton > $LOG_OUT/k9db.out 2>&1

# Kill K9db.
mariadb -P10001 -e "STOP" > /dev/null 2>&1;

#
# Plotting
#
echo "Experiment ran. Now plotting"
cd "${K9DB_DIR}/experiments/scripts/plotting"
. venv/bin/activate
python votes.py --paper

echo "Terminated"
