#!/bin/bash
ROLE="$1"

# Make sure you run as user pelton
if [[ $(whoami) != "pelton" ]]; then
  echo "MUST RUN THIS SCRIPT AS USER PELTON"
  exit 1
fi

# Go to home directory.
cd ~

# Install PERF.
sudo apt-get install -y linux-tools-common linux-tools-generic linux-tools-`uname -r`

# Download flamegraph repo.
git clone https://github.com/brendangregg/FlameGraph
echo '# Add flame alias to generate flamegraphs' >> ~/.bash_profile
echo 'alias flame="~/flame.sh"' >> ~/.bash_profile

# Make sure ssh deploy key is added.
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/nsdi

# Start and stop ssh-agent with the nsdi key on login/logout
echo '# add git deploy key' >> ~/.bash_profile
echo 'eval "$(ssh-agent -s)"' >> ~/.bash_profile
echo 'ssh-add ~/.ssh/nsdi' >> ~/.bash_profile
echo 'if [[ "$SSH_AGENT_PID" != "" ]]; then' >> ~/.bash_logout
echo '  eval $(/usr/bin/ssh-agent -k)' >> ~/.bash_logout
echo 'fi' >> ~/.bash_logout

# Export the local ip as a environment variable
echo 'export LOCAL_IP=$(curl  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip -H "Metadata-Flavor: Google")' >> ~/.bash_profile

# Clones to /home/pelton/pelton
ssh-keyscan github.com >> ~/.ssh/known_hosts
git clone git@github.com:brownsys/pelton.git
cd pelton

# Run the setup scripts
./scripts/setup/setup-user.sh

# Format and load the ssd.
sudo ./scripts/setup/gcloud-ssd-setup.sh

# Configure mariadb to store database in directory
sudo echo "datadir = /mnt/disks/my-ssd/mysql" >> /etc/mysql/mariadb.cnf

# Do this on the server only: https://cloud.google.com/architecture/mysql-remote-access
LOCAL_IP=$(curl  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip -H "Metadata-Flavor: Google")
sudo sed -i "s|bind-address.*|bind-address = $LOCAL_IP|" /etc/mysql/mariadb.conf.d/50-server.cnf
sudo service mariadb restart

./scripts/setup/gcloud-build.sh $ROLE
