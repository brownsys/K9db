#!/bin/bash
IP="$1"
ROLE="$2"
BRANCH="$3"
echo "Updating $IP with role $ROLE to branch $BRANCH"
ssh -i ~/.ssh/pelton pelton@$IP "bash -l -c \"cd ~/pelton; git fetch origin; git checkout $BRANCH; git pull origin $BRANCH\""
ssh -i ~/.ssh/pelton pelton@$IP "bash -l -c \"cd ~/pelton && sudo ./scripts/setup/gcloud-ssd-setup.sh\""
ssh -i ~/.ssh/pelton pelton@$IP "bash -l -c \"sudo service mariadb restart\""
ssh -i ~/.ssh/pelton pelton@$IP "bash -l -c \"cd pelton && ./scripts/setup/gcloud-build.sh $ROLE\""
