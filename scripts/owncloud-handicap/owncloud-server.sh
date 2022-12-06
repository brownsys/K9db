#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/owncloud-handicap"
echo "Writing output to $OUT"
cd $PELTONDIR

# Stop mariadb.
sudo service mariadb stop

# Run pelton - no views.
./run.sh opt
echo "Terminated"

# Run pelton - no indices
./run.sh opt
echo "Terminated"

# Run pelton - no accessors
./run.sh opt
echo "Terminated"

# Run pelton - physical separation.
./run.sh physical
echo "Terminated"
