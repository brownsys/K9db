#!/bin/bash
IP="$1"
echo "Configuring $IP"
ssh-keyscan $IP >> ~/.ssh/known_hosts
scp -i ~/.ssh/pelton gcloud-nsdi-setup.sh pelton@$IP:/home/pelton/
scp -i ~/.ssh/pelton ~/.ssh/nsdi pelton@$IP:/home/pelton/.ssh/
ssh -i ~/.ssh/pelton pelton@$IP 'chmod 600 ~/.ssh/nsdi'
ssh -i ~/.ssh/pelton pelton@$IP './gcloud-nsdi-setup.sh > init.log 2>&1 &'
