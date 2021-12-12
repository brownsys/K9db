cd mysql_proxy && rm -rf cargo && cargo raze && cd -
USER=$(whoami)
sudo chown $USER.$USER -R /var/pelton
