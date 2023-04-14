sudo scripts/setup/setup-root.sh

# Give access to pelton db directory
USER=$(whoami)
sudo chown $USER.$USER -R /var/pelton
sudo chown $USER.$USER -R /tmp/pelton

# install rust
curl https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
cargo install --version 0.15.0 cargo-raze

# Run cargo raze.
mkdir -p /tmp/cargo-raze/doesnt/exist/
cd pelton/proxy && cargo raze && cd -
cd experiments/lobsters && cargo raze && cd -
cd experiments/ownCloud && cargo raze && cd -
cd experiments/vote && cargo raze && cd -
