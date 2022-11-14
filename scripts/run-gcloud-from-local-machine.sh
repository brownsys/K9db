#!/bin/bash
IP="$1"
SCRIPT="$2"
echo "Running $SCRIPT on $IP"

echo "bash -l -c \"cd pelton/scripts && ./\"$SCRIPT\" ${@:3}\" > \"/home/pelton/pelton/scripts/$SCRIPT.out\" 2>&1 &"
ssh -i ~/.ssh/pelton pelton@$IP "bash -l -c \"cd pelton/scripts && ./\"$SCRIPT\" ${@:3}\" > \"/home/pelton/pelton/scripts/$SCRIPT.out\" 2>&1 &"
ssh -i ~/.ssh/pelton pelton@$IP "tail -f \"/home/pelton/pelton/scripts/$SCRIPT.out\""
