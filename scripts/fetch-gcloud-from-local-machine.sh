#!/bin/bash
IP="$1"
DIR="$2"
echo "Fetching $DIR on $IP"
mkdir -p fetched
scp -i ~/.ssh/pelton "pelton@$IP:/home/pelton/pelton/scripts/$DIR/*.out" fetched/
