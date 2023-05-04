#!/bin/bash
K9DB_DIR="$(pwd)"
LOG_OUT="${K9DB_DIR}/experiments/scripts/logs/drilldown/"
PLOT_OUT="${K9DB_DIR}/experiments/scripts/outputs/drilldown/"

mkdir -p "$LOG_OUT"
mkdir -p "$PLOT_OUT"
mkdir -p /mnt/disks/my-ssd/k9db

echo "Writing plots and final outputs to $PLOT_OUT"
echo "Writing logs and temporary measurements to $LOG_OUT"
cd "${K9DB_DIR}/experiments/ownCloud"

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

# Smaller parameters for physical separation
phys_user=1000
phys_ops=200

#
# No views
#
echo "==========================================="
echo "==========================================="
echo "==========================================="
echo "Running against K9db with views disabled..."
echo "==========================================="

# Run K9db server with no views.
cd ${K9DB_DIR}
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel build //:k9db --config=opt --//:views=off
bazel run //:k9db --config=opt --//:views=off -- \
  --db_path="/mnt/disks/my-ssd/k9db/" > "$LOG_OUT/server-noviews.out" 2>&1 &
sleep 10

# Run harness.
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
  > "$LOG_OUT/noviews.out" 2>&1

# Send signal to server to kill k9db.
mariadb -P10001 -e "STOP" > /dev/null 2>&1;

#
# No views + no accessors
#
echo "==========================================="
echo "==========================================="
echo "==========================================="
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo "Running against K9db with views disabled and all owners..."
echo "==========================================="
echo "==========================================="
echo "==========================================="

# Run K9db server with no views.
cd ${K9DB_DIR}
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel build //:k9db --config=opt --//:views=off
bazel run //:k9db --config=opt --//:views=off -- \
  --db_path="/mnt/disks/my-ssd/k9db/" > "$LOG_OUT/server-noaccessors.out" 2>&1 &
sleep 5

# Run harness with all owners.
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
  --all_owners \
  > "$LOG_OUT/noaccessors.out" 2>&1

# Send signal to server to kill k9db.
mariadb -P10001 -e "STOP" > /dev/null 2>&1;

#
# No views + no accessors + physical separation
#
echo "==========================================="
echo "==========================================="
echo "==========================================="
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo "Running against K9db with physical separation..."
echo "==========================================="
echo "==========================================="
echo "==========================================="

# Run K9db server with no views.
cd ${K9DB_DIR}
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel build //:k9db --config=opt --//:views=off --//:physical_separation=on
bazel run //:k9db --config=opt --//:views=off --//:physical_separation=on -- \
  --db_path="/mnt/disks/my-ssd/k9db/" > "$LOG_OUT/server-physical.out" 2>&1 &
sleep 5

# Run harness with all owners.
cd "${K9DB_DIR}/experiments/ownCloud"
bazel run :benchmark -c opt -- \
  --num-users $phys_user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --read_in_size $read_in_size \
  --write_batch_size $write_batch_size \
  --write_every $write_every \
  --operations $phys_ops \
  --zipf $zipf \
  --backend pelton \
  --db_ip 127.0.0.1 \
  --all_owners \
  > "$LOG_OUT/physical.out" 2>&1

# Send signal to server to kill k9db.
mariadb -P10001 -e "STOP" > /dev/null 2>&1;

#
# Plotting
#

echo "==========================================="
echo "==========================================="
echo "==========================================="
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo "Experiment ran. Now plotting"
echo "==========================================="
echo "==========================================="
echo "==========================================="
cd "${K9DB_DIR}/experiments/scripts/plotting"
. venv/bin/activate
python drilldown.py --paper

echo "Success"
