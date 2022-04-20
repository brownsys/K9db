#!/bin/bash
sudo perf stat -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations -p $(ps -ef | grep "pelton/proxy/proxy" -m 1 | awk '{print $2}')
