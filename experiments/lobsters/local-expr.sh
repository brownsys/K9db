FLAME=2
BASELINE=1

# lobsters.
if [[ "$BASELINE" == "1" ]]; then
  echo "Baseline..."
  bazel run -c opt //:lobsters-harness -- --runtime 0 --datascale 0.05 --reqscale 100 --queries pelton --backend rocks-mariadb --prime --scale_everything "mysql://root:password@localhost:3306/lobsters" > /home/bab/Desktop/pelton-paper/2023-osdi/graphs/lobsters-endpoints/baseline.txt 2>&1
  bazel run -c opt //:lobsters-harness -- --runtime 60 --datascale 0.05 --reqscale 50 --queries pelton --backend rocks-mariadb --scale_everything --in-flight 3 "mysql://root:password@localhost:3306/lobsters" > /home/bab/Desktop/pelton-paper/2023-osdi/graphs/lobsters-endpoints/baseline.txt 2>&1 
fi

# Run Pelton proxy.
echo "Running proxy..."
cd ../../
./run.sh opt > pelton.log 2>&1 &
cd -
sleep 5

# Prime pelton.
echo "Priming pelton..."
bazel run -c opt //:lobsters-harness -- --runtime 0 --datascale 0.05 --reqscale 100 --queries pelton --backend pelton --prime --scale_everything "mysql://root:password@localhost:10001/lobsters" > /home/bab/Desktop/pelton-paper/2023-osdi/graphs/lobsters-endpoints/pelton.txt 2>&1

# Can run the flamegraph script here.
if [[ "$FLAME" == "1" ]]; then
  echo "Perf monitoring starting..."
  flame &
  pid=$!
fi

# Run pelton workload
echo "Running pelton load..."
bazel run -c opt //:lobsters-harness -- --runtime 60 --datascale 0.05 --reqscale 50 --queries pelton --backend pelton --scale_everything --in-flight 3 "mysql://root:password@localhost:10001/lobsters" > /home/bab/Desktop/pelton-paper/2023-osdi/graphs/lobsters-endpoints/pelton.txt 2>&1

echo "Killing pelton proxy..."
peltonid=$(ps aux | grep proxy/proxy | awk '{print $2}')
echo $peltonid
kill $peltonid

if [[ "$FLAME" == "1" ]]; then
  echo "Waiting for perf..."
  wait $pid
fi

# Plot
echo "Plotting..."
cd /home/bab/Desktop/pelton-paper/2023-osdi/graphs
. venv/bin/activate
python3 lobsters-endpoints.py
xdg-open lobsters-endpoints/lobsters-50th.pdf &
xdg-open lobsters-endpoints/lobsters-95th.pdf &
xdg-open lobsters-endpoints/lobsters-99th.pdf &
cd -

# Show flame graph
if [[ "$FLAME" == "1" ]]; then
  xdg-open graph.svg
fi
