#!/bin/bash
scales=("100" "1000" "10000" "100000" "1000000" "10000000")
# workloads=("reads" "direct-share" "group-share")

# make dir for results, if it doesn't exist
mkdir -p results/scaleusers

echo -e "\n"
echo "Building pelton..."
echo -e "\n"
# save working directory
cwd=$(pwd)
# change to dir with Pelton
cd ../..
# build pelton
bazel build //:pelton --config opt
# change back to dir with benchmark
cd $cwd

echo -e "\n"
echo "Starting pelton backend benchmark..."
echo -e "\n"
for scale_index in "${!scales[@]}"; do
    scale=${scales[$scale_index]}

    # save working directory
    cwd=$(pwd)
    # change to dir with Pelton
    cd ../..
    # run Pelton in background; NOTE: this look ok?
    rm -rf /tmp/pelton/rocksdb
    mkdir -p /tmp/pelton/rocksdb
    ./run.sh opt &
    sleep 3
    # change to dir with benchmark
    cd $cwd

    # run ownCloud harness
    echo -e "\n"
    echo "Generating pelton-${scale}-users.txt..."
    echo -e "\n"
    bazel run :benchmark -c opt -- \
        --num-users $scale \
        --backend pelton \
        --workloads reads \
        --workloads direct-share \
        --workloads group-share \
        --files-per-user 10 \
        --direct-shares-per-file 3 \
        --group-shares-per-file 2 \
        --users-per-group 10 \
        --samples 100 \
        >results/scaleusers/pelton-${scale}-users.txt

    # kill Pelton
    mariadb -P10001 --host=127.0.0.1 -e "STOP"

    echo -e "\n"
    echo "pelton-${scale}-users.txt generated..."
    echo -e "\n"
    sleep 3
done

echo -e "\n"
echo "Starting MySQL backend benchmark..."
echo -e "\n"
for scale_index in "${!scales[@]}"; do
    scale=${scales[$scale_index]}

    # run ownCloud harness
    echo -e "\n"
    echo "Generating mysql-${scale}-users-${workload}.txt..."
    echo -e "\n"
    bazel run benchmark -- \
        --num-users $scale \
        --backend mysql \
        --workloads reads \
        --workloads direct-share \
        --workloads group-share \
        --files-per-user 10 \
        --direct-shares-per-file 3 \
        --group-shares-per-file 2 \
        --users-per-group 10 \
        --samples 100 \
        >results/scaleusers/mysql-${scale}-users-${workload}.txt

    echo -e "\n"
    echo "mysql-${scale}-users-${workload}.txt generated..."
    echo -e "\n"
    sleep 3
done
