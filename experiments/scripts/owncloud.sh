#!/bin/bash
K9DB_DIR="$(pwd)"
LOG_OUT="${K9DB_DIR}/experiments/scripts/logs/owncloud/"
PLOT_OUT="${K9DB_DIR}/experiments/scripts/outputs/owncloud/"

mkdir -p "$LOG_OUT"
mkdir -p "$PLOT_OUT"
mkdir -p /mnt/disks/my-ssd/k9db

echo "Writing plots and final outputs to $PLOT_OUT"
echo "Writing logs and temporary measurements to $LOG_OUT"
cd "${K9DB_DIR}/experiments/ownCloud"

# sudo is not available inside docker container
SUDO="sudo "
MEMCACHED_USER=""
if [[ "$(whoami)" == "root" ]]; then
  SUDO=""
  MEMCACHED_USER="-u memcached"
fi

# The database IP is either 127.0.0.1 or LOCAL_IP if running on gcloud.
DB_IP=$LOCAL_IP
if [[ $LOCAL_IP == "" ]]; then
  DB_IP="127.0.0.1"
fi
echo $DB_IP

# Experiment parameters.
user=100000
groups=5
files=3
dshare=3
gshare=2
write_batch_size=1
read_in_size=10
write_every=19
ops=10000
zipf=0.6

#
# Building harness.
#
bazel build :benchmark -c opt

#
# Run against MariaDB baseline.
#
echo "Running against MariaDB..."

# Start MariaDB.
$SUDO service mariadb restart

# Run harness against MariaDB.
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --read_in_size $read_in_size \
  --write_batch_size $write_batch_size \
  --write_every $write_every \
  --operations $ops \
  --zipf $zipf \
  --backend mariadb \
  --db_ip $DB_IP \
  > "$LOG_OUT/mariadb.out" 2>&1

# Show DB size of baseline.
mariadb -P3306 --host=$DB_IP -u k9db -ppassword \
  -e "SELECT table_schema AS 'Database', SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema" \
  > "$LOG_OUT/mariadb-memory.out" 2>&1

#
# Run against memcached + MariaDB.
#
echo "Running against MariaDB+memcached..."
cd "$K9DB_DIR/experiments/memcached"

bazel build @memcached//:memcached --config=opt
bazel run @memcached//:memcached --config=opt -- $MEMCACHED_USER -m 1024 -M > "$LOG_OUT/memcached-server.log" 2>&1 &
pid=$!
sleep 30

# Run harness against memcached+MariaDB.
cd "${K9DB_DIR}/experiments/ownCloud"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --read_in_size $read_in_size \
  --write_batch_size $write_batch_size \
  --write_every $write_every \
  --operations $ops \
  --warmup \
  --zipf $zipf \
  --backend memcached \
  --db_ip $DB_IP \
  > "$LOG_OUT/hybrid.out" 2>&1

# Find memory usage of memcached.
echo "stats" | nc localhost 11211 -q 1 > "$LOG_OUT/memcached-memory.out" 2>&1

# Kill memcached and MariaDB.
kill $pid
$SUDO service mariadb stop

#
# Run against K9db.
#
echo "Running against K9db..."

# Run K9db server.
cd ${K9DB_DIR}
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel build //:k9db --config=opt
bazel run //:k9db --config=opt -- --db_path="/mnt/disks/my-ssd/k9db/" \
  > "$LOG_OUT/k9db-server.out" 2>&1 &
sleep 10

# Run harness against K9db.
cd "${K9DB_DIR}/experiments/ownCloud"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --read_in_size $read_in_size \
  --write_batch_size $write_batch_size \
  --write_every $write_every \
  --operations $ops \
  --zipf $zipf \
  --backend pelton \
  --db_ip 127.0.0.1 \
  > "$LOG_OUT/k9db.out" 2>&1

# Find memory usage of K9db.
mariadb -P10001 -e "SHOW MEMORY" > "$LOG_OUT/k9db-memory.out" 2>&1

# Send signal to server to kill k9db.
mariadb -P10001 -e "STOP" > /dev/null 2>&1;

#
# Plotting
#
echo "Experiment ran. Now plotting"
cd "${K9DB_DIR}/experiments/scripts/plotting"
. venv/bin/activate
python owncloud-comparison.py --paper

echo "Success"
