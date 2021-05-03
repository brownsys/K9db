#!/bin/bash
ORCHESTRATOR="http://$1:8000"

echo "Worker starting!" 
echo "Orchestrator ip $ORCHESTRATOR"

# Compile our code
bazel build --config=opt ...
echo "Pelton Log: Bazel built!"

# compile GDPRBench
cd experiments/GDPRbench/src/ && mvn package && cd -
echo "Pelton Log: mvn built!"

# Poll the orchestrator for loads to run.
rm -rf orchestrator/worker/worker_load
rm -rf .output

while true
do
  curl -X GET $ORCHESTRATOR/ready -o orchestrator/worker/worker_load
  if [[ -f orchestrator/worker/worker_load ]] && [ $(wc -w orchestrator/worker/worker_load | awk '{print $1}') -ne 0 ]; then
    echo "Pelton Log: Load received!"

    # We have a load.
    PREFIX="../../.."
    LOAD_CONF="../../../orchestrator/worker/worker_load"
    SFILENAME="orchestrator/worker/pelton.sql"
    UFILENAME="orchestrator/worker/mysql.sql"

    # Generate tracefile using GDPRBench.
    cd experiments/GDPRbench/src/ \
      && ./bin/ycsb load tracefile -s -P $LOAD_CONF \
          -p sharded.path=$PREFIX/$SFILENAME \
          -p unsharded.path=$PREFIX/$UFILENAME -p file.append=no \
      && ./bin/ycsb run tracefile -s -P $LOAD_CONF \
          -p sharded.path=$PREFIX/$SFILENAME \
          -p unsharded.path=$PREFIX/$UFILENAME -p file.append=yes \
      && cd -
    echo "Pelton Log: Trace files generated!"

    # Run pelton trace file
    bazel run --config=opt //bin:cli -- --print=no --minloglevel=3 < $SFILENAME > .output 2>&1
    echo "Pelton Log: Pelton job completed!"

    # Get the database size.
    echo "" >> .output
    echo "DB size" >> .output
    mariadb -u root -ppassword --execute="SELECT table_schema AS 'Database', SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema" >> .output

    # Drop all the databases
    ./bin/drop.sh root password
    sleep 5
    echo "" >> .output 2>&1
    echo "" >> .output 2>&1
    echo "" >> .output 2>&1

    # Run vanilla trace file
    bazel run --config=opt //bin:mysql -- --print=no --minloglevel=3 < $UFILENAME >> .output 2>&1
    echo "Pelton Log: Baseline job completed!"

    # Get the database size.
    echo "" >> .output
    echo "DB size" >> .output
    mariadb -u root -ppassword --execute="SELECT table_schema AS 'Database', SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema" >> .output
    
    # Curl the output to orchestrator
    curl -H 'Content-Type: text/plain' -X POST --data-binary @.output $ORCHESTRATOR/done 
    echo "Pelton Log: Result sent to orchestrator!"

    # Drop all the databases
    ./bin/drop.sh root password
    sleep 5
    echo "Pelton Log: Clean up complete!"
  else
    sleep 10
  fi
done