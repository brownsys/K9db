#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/owncloud-handicap"
echo "Writing output to $OUT"
cd $PELTONDIR

# Stop mariadb.
sudo service mariadb stop

# Run pelton.
./run.sh opt
echo "Terminated"

# Run pelton - no views.
./run.sh physical
echo "Terminated"

# Run pelton - no indices
./run.sh physical
echo "Terminated"
