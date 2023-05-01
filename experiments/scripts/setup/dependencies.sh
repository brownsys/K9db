#!/bin/bash

# Our current location is the root dir of the repo.
K9DB_DIR="$(pwd)"

sudo apt-get update
sudo apt-get install -y software-properties-common

# Add ppa for gcc-11
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test

# Install dependencies
echo "installing dependencies..."
sudo apt-get update 
sudo apt-get install -y apt-utils libssl-dev lsb-release openssl vim git \
    build-essential libssl-dev zlib1g-dev libncurses5-dev \
    libncursesw5-dev libreadline-dev libgdbm-dev libdb5.3-dev libbz2-dev \
    libexpat1-dev liblzma-dev tk-dev libffi-dev wget gcc-11 g++-11 unzip \
    openjdk-11-jdk maven python2 valgrind curl libclang-dev flex bison libevent-dev \
    libsnappy-dev

# Make gcc-11/g++-11 default complier.
echo "Configuring gcc-11"
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 90 \
                                 --slave /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-11 \
                                 --slave /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-11 \
                                 --slave /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-11 \
                                 --slave /usr/bin/g++ g++ /usr/bin/g++-11
sudo update-alternatives --set gcc /usr/bin/gcc-11

# Install bazel.
echo "Installing bazel..."
cd /tmp
sudo wget https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh
sudo chmod +x bazel-4.0.0-installer-linux-x86_64.sh
sudo ./bazel-4.0.0-installer-linux-x86_64.sh
echo "source /usr/local/lib/bazel/bin/bazel-complete.bash" >> ~/.bashrc
sudo rm bazel-4.0.0-installer-linux-x86_64.sh

# install rust
echo "Installing rust..."
curl https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
cargo install --version 0.15.0 cargo-raze

# Run cargo raze.
echo "Running cargo raze..."
cd $K9DB_DIR
mkdir -p /tmp/cargo-raze/doesnt/exist/
cd k9db/proxy && cargo raze && cd -
cd experiments/lobsters && cargo raze && cd -
cd experiments/ownCloud && cargo raze && cd -
cd experiments/vote && cargo raze && cd -
