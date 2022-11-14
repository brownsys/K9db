#!/bin/bash
IP="$1"
SCRIPT="$2"
echo "Running $SCRIPT on $IP"
ssh -i ~/.ssh/pelton pelton@$IP "cd pelton/scripts && ./\"$SCRIPT\" > \"$SCRIPT\".out 2>&1 &"
ssh -i ~/.ssh/pelton pelton@$IP "tail -f \"$SCRIPT\".out"
