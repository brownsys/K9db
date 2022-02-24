sudo scripts/setup-root.sh

# Give access to pelton db directory
USER=$(whoami)
sudo chown $USER.$USER -R /var/pelton
sudo chown $USER.$USER -R /tmp/pelton

# install rust
curl https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
cargo install cargo-raze

# run cargo raze
cd pelton/proxy && cargo raze && cd -
cd experiments/lobsters && cargo raze && cd -
cd experiments/memcached && cargo raze && cd -
cd experiments/ownCloud && cargo raze && cd -
