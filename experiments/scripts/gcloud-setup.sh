#!/bin/bash

# Our current location is the root dir of the repo.
K9DB_DIR="$(pwd)"

# Export the local ip as a environment variable
echo "Setting up machine ..."
echo 'export LOCAL_IP=$(curl  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip -H "Metadata-Flavor: Google")' >> ~/.bash_profile

# Download and install dependencies.
echo "======================================================================"
echo "Installing K9db dependencies..."
cd $K9DB_DIR
./experiments/scripts/setup/dependencies.sh

# Install and configure mariadb.
echo "======================================================================"
echo "Installing and configuring MariaDB..."
cd $K9DB_DIR
./experiments/scripts/setup/mariadb.sh

# Setup plotting scripts.
echo "======================================================================"
echo "Setup plotting scripts...".
cd $K9DB_DIR
./experiments/scripts/setup/plotting.sh

# Building harnesses and K9db.
echo "======================================================================"
echo "Building harnesses and K9db...".
cd $K9DB_DIR
source ~/.bashrc
./experiments/scripts/setup/build.sh

echo "======================================================================"
echo "Machine setup done!"
echo "Do not forget to run './experiments/scripts/setup/ssd.sh' after any restart!"
cd $K9DB_DIR
