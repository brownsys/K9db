#!/bin/bash
sudo perf record --freq=100000 -p $(ps -ef | grep "pelton/proxy/proxy" -m 1 | awk '{print $2}') -g
sudo perf script > /tmp/out.perf
sudo rm perf.data
~/FlameGraph/stackcollapse-perf.pl /tmp/out.perf > /tmp/out.folded
~/FlameGraph/flamegraph.pl /tmp/out.folded > graph.svg
