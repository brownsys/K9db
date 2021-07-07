#!/bin/bash
sudo su

cd /pelton
rm -rf noria
git stash
git fetch
git checkout lobsters
git reset --hard origin/lobsters
git submodule add https://github.com/ishanSharma07/noria/
git submodule init && git submodule update
cd noria && git pull origin master && cd -
chmod 777 -R .

# Reset the docker image
docker stop pelton-worker
docker start pelton-worker

## 
# RUN THIS INSIDE DOCKER

cd /home/pelton/noria

# run this only once
apt-get install libclang-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustc --version
cargo --version

cargo b --release --bin lobsters-mysql
# end of once

target/release/lobsters-mysql --runtime 0  --prime  --queries original \
    "mysql://root:password@localhost/lobsters" --scale 1 > populate.sql
    
python3 postprocessing/post_processing.py postprocessing/equivalent_views.sql populate.sql

mv populate.sql mysql-populate.sql
mv transformed_trace.sql pelton-populate.sql

target/release/lobsters-mysql --runtime 60  --queries original \
    "mysql://root:password@localhost:3306/lobsters" --scale 1 > workload.sql
python3 postprocessing/post_processing.py postprocessing/equivalent_views.sql workload.sql

mv workload.sql mysql-workload.sql
mv transformed_trace.sql pelton-workload.sql

# Combine the schemas
cat ../experiments/lobsters/schema-simplified.sql > pelton.sql
cat ../experiments/lobsters/views.sql >> pelton.sql
cat pelton-populate.sql >> pelton.sql
rm pelton-populate.sql
echo "# perf start" >> pelton.sql
cat pelton-workload.sql >> pelton.sql
rm pelton-workload.sql

cat applications/lobsters/mysql/db-schema/original_rocksdb.sql > mysql.sql
cat mysql-populate.sql >> mysql.sql
rm mysql-populate.sql
echo "# perf start" >> mysql.sql
cat mysql-workload.sql >> mysql.sql
rm mysql-workload.sql

# Run via pelton and baseline.
cd ..

bazel run bin:cli --config=opt -- --print=no --minloglevel=3 < noria/pelton.sql

bazel run bin:mysql --config=opt -- --print=no < noria/mysql.sql
