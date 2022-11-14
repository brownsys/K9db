#!/bin/bash
IP="$1"
ROLE="$2"
echo "Updating $IP"
ssh -i ~/.ssh/pelton pelton@$IP 'cd pelton && sudo ./scripts/setup/gcloud-ssd-setup.sh'
ssh -i ~/.ssh/pelton pelton@$IP 'sudo service mariadb restart'
ssh -i ~/.ssh/pelton pelton@$IP 'cd pelton && git pull origin master'
ssh -i ~/.ssh/pelton pelton@$IP "cd pelton && ./scripts/setup/gcloud-build.sh $ROLE"
