#!/bin/bash
IP="$1"
ROLE="$2"
echo "Updating $IP"
ssh -i ~/.ssh/pelton pelton@$IP 'bash -l -c "cd ~/pelton; git fetch origin; git checkout master; git pull origin master"'
ssh -i ~/.ssh/pelton pelton@$IP 'bash -l -c "cd ~/pelton && sudo ./scripts/setup/gcloud-ssd-setup.sh"'
ssh -i ~/.ssh/pelton pelton@$IP 'bash -l -c "sudo service mariadb restart"'
ssh -i ~/.ssh/pelton pelton@$IP "bash -l -c \"cd pelton && ./scripts/setup/gcloud-build.sh $ROLE\""
