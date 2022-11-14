#!/bin/bash
IP="$1"
ROLE="$2"
echo "Configuring $IP"
ssh-keyscan $IP >> ~/.ssh/known_hosts
scp -i ~/.ssh/pelton setup/gcloud-setup.sh pelton@$IP:/home/pelton/
scp -i ~/.ssh/pelton measurements/flame.sh pelton@$IP:/home/pelton/
scp -i ~/.ssh/pelton ~/.ssh/nsdi pelton@$IP:/home/pelton/.ssh/
ssh -i ~/.ssh/pelton pelton@$IP 'chmod 600 ~/.ssh/nsdi'
ssh -i ~/.ssh/pelton pelton@$IP "./gcloud-setup.sh ${ROLE} > init.log 2>&1 &"
ssh -i ~/.ssh/pelton pelton@$IP "tail -f init.log"
