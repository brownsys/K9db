# Give access to pelton db directory
USER=$(whoami)
sudo chown $USER.$USER -R /var/pelton

# install rust
curl https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
cargo install cargo-raze

# run cargo raze
cd mysql_proxy && rm -rf cargo && cargo raze && cd -
