#!/bin/bash
ORCHESTRATOR="http://$1:8000"

echo "Worker starting!" 
echo "Orchestrator ip $ORCHESTRATOR"

# Compile our code
bazel build -c opt ...

# compile GDPRBench
cd experiments/GDPRbench/src/ && mvn package && cd -

# Poll the orchestrator for loads to run.
rm -rf orchestrator/worker/worker_load
rm -rf .output

while true
do
  curl -X GET $ORCHESTRATOR/ready -o orchestrator/worker/worker_load
  if [ $(wc -w orchestrator/worker/worker_load | awk '{print $1}') -ne 0 ]; then
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

    # Run pelton trace file
    bazel run -c opt //bin:cli -- --print=no --minloglevel=3 < $SFILENAME > .output 2>&1

    # Drop all the databases
    ./bin/drop.sh root password
    sleep 5
    echo "" >> .output 2>&1
    echo "" >> .output 2>&1
    echo "" >> .output 2>&1

    # Run vanilla trace file
    bazel run -c opt //bin:mysql -- --print=no --minloglevel=3 < $UFILENAME >> .output 2>&1
    
    # Curl the output to orchestrator
    curl -H 'Content-Type: text/plain' -X POST --data-binary @.output $ORCHESTRATOR/done 

    # Drop all the databases
    ./bin/drop.sh root password
    sleep 5
  else
    sleep 10
  fi
done
