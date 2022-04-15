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

# Make sure ssh deploy key is added.
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/nsdi

# Start and stop ssh-agent with the nsdi key on login/logout
echo '# add git deploy key' >> ~/.bashrc
echo 'eval "$(ssh-agent -s)"' >> ~/.bashrc
echo 'ssh-add ~/.ssh/nsdi' >> ~/.bashrc
echo 'if [[ "$SSH_AGENT_PID" != "" ]]; then' >> ~/.bash_logout
echo '  eval $(/usr/bin/ssh-agent -k)' >> ~/.bash_logout
echo 'fi' >> ~/.bash_logout


# Clones to /home/pelton/pelton
ssh-keyscan github.com >> ~/.ssh/known_hosts
git clone git@github.com:brownsys/pelton.git
cd pelton

# Run the setup scripts
./scripts/setup-user.sh

# Do this on the server only: https://cloud.google.com/architecture/mysql-remote-access
LOCAL_IP=$(curl  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip -H "Metadata-Flavor: Google")
sudo sed -i "s|bind-address.*|bind-address = $LOCAL_IP|" /etc/mysql/mariadb.conf.d/50-server.cnf
sudo service mariadb restart

# Do this on the client only
sudo service mariadb stop

# Build ownCloud harness
cd ~/pelton/experiments/ownCloud
bazel build ... -c opt

# Build pelton
cd ~/pelton
if [[ "$ROLE" == "db" ]]; then
  bazel build //:pelton --config opt
else
  cd experiments/lobsters
  bazel build ... -c opt
fi
