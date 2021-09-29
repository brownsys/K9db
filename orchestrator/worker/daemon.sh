#!/bin/bash
ORCHESTRATOR="http://$1:8000"

echo "Worker starting!"
echo "Orchestrator ip $ORCHESTRATOR"

./bin/drop.sh root password
echo "Pelton Log: Cleaned DB!"

# Compile our code
bazel build --config=opt ...
echo "Pelton Log: Bazel built!"

# compile GDPRBench
cd experiments/GDPRbench/src/ && mvn package && cd -
echo "Pelton Log: mvn built!"

# Poll the orchestrator for loads to run.
echo "Pelton Log: find worker directory"
find orchestrator/worker/

while true
do
  rm -rf orchestrator/worker/worker_load
  rm -rf .output .error

  curl -X GET $ORCHESTRATOR/ready -o orchestrator/worker/worker_load
  if [[ -f orchestrator/worker/worker_load ]] && [ $(wc -w orchestrator/worker/worker_load | awk '{print $1}') -ne 0 ]; then
    echo "Pelton Log: Load received!"
    WITH_ORDER="yes"
    if [[ $(head orchestrator/worker/worker_load -n 1) == "# PELTON NO ORDER" ]]; then
      WITH_ORDER="no"
      echo "Pelton Log: No Order!"
    fi

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
          -p sharded.order=$WITH_ORDER \
      && ./bin/ycsb run tracefile -s -P $LOAD_CONF \
          -p sharded.path=$PREFIX/$SFILENAME \
          -p unsharded.path=$PREFIX/$UFILENAME -p file.append=yes \
          -p sharded.order=$WITH_ORDER \
      && cd -
    echo "Pelton Log: Trace files generated!"

    # Run pelton trace file
    echo "Pelton Log: Running Pelton job!"
    bazel run --config=opt //bin:cli -- --print=no --minloglevel=3 < $SFILENAME > .output 2> .error
    if [ $? -eq 0 ]; then
      echo "Pelton Log: Pelton job completed!"

      # Get the database size
      echo "DB size" >> .output
      mariadb -u root -ppassword --execute="SELECT table_schema AS 'Database',  ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema" >> .output
    else
      echo "Pelton Log: Pelton job errored out!"
      cp .error .output
    fi

    # Drop all the databases
    echo "Pelton Log: Cleaning up!"
    ./bin/drop.sh root password
    sleep 5

    # Run vanilla trace file
    echo "Pelton Log: Running Baseline job!"
    bazel run --config=opt //bin:mysql -- --print=no --minloglevel=3 < $UFILENAME >> .output 2>> .error
    if [ $? -eq 0 ]; then
      echo "Pelton Log: Baseline job completed!"

      # Get the database size
      echo "DB size" >> .output
      mariadb -u root -ppassword --execute="SELECT table_schema AS 'Database',  ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema" >> .output

      # Post the results to orchestrator
      curl -H 'Content-Type: text/plain' -X POST --data-binary @.output $ORCHESTRATOR/done
    else
      echo "Pelton Log: Baseline job errored out!"
      curl -H 'Content-Type: text/plain' -X POST --data-binary @.error $ORCHESTRATOR/done
    fi

    # Drop all the databases
    echo "Pelton Log: Cleaning up!"
    ./bin/drop.sh root password
    sleep 5
    echo "Pelton Log: Ready!"
  else
    sleep 10
  fi
done
