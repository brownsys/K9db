# Update and add the required apt-get repositories
apt-get update
apt-get upgrade -y
apt-get install -y software-properties-common
add-apt-repository -y ppa:ubuntu-toolchain-r/test  # c++-20

# Install system-wide dependencies
apt-get update
apt-get install -y apt-utils libssl-dev lsb-release openssl vim git \
                   build-essential libssl-dev zlib1g-dev libncurses5-dev \
                   libncursesw5-dev libreadline-dev libgdbm-dev libdb5.3-dev libbz2-dev \
                   libexpat1-dev liblzma-dev tk-dev libffi-dev wget gcc-11 g++-11 unzip \
                   openjdk-11-jdk maven python2 valgrind curl libclang-dev flex bison libevent-dev \
                   libsnappy-dev

# GCC-11 is default.
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 90 \
                         --slave /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-11 \
                         --slave /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-11 \
                         --slave /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-11 \
                         --slave /usr/bin/g++ g++ /usr/bin/g++-11
update-alternatives --set gcc /usr/bin/gcc-11

# install bazel using installer to get appropriate version 4.0
cd /tmp \
    && wget https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh \
    && chmod +x bazel-4.0.0-installer-linux-x86_64.sh \
    && ./bazel-4.0.0-installer-linux-x86_64.sh \
    && echo "source /usr/local/lib/bazel/bin/bazel-complete.bash" >> ~/.bashrc \
    && rm bazel-4.0.0-installer-linux-x86_64.sh \
    && cd -

# install rust
curl https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
cargo install cargo-raze

# install mariadb
apt-get remove -y --purge mysql*
curl -LsS -O https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
bash mariadb_repo_setup --mariadb-server-version=10.6
rm mariadb_repo_setup
apt-get install -y mariadb-server-10.6 mariadb-client-10.6 mariadb-plugin-rocksdb
echo "[mysqld]" >> /etc/mysql/mariadb.cnf
echo "table_open_cache_instances = 1" >> /etc/mysql/mariadb.cnf
echo "table_open_cache = 1000000" >> /etc/mysql/mariadb.cnf
echo "table_definition_cache = 1000000" >> /etc/mysql/mariadb.cnf
echo "open_files_limit = 1000000" >> /etc/mysql/mariadb.cnf

# ulimits.
echo "* hard nofile 1024000" >> /etc/security/limits.conf
echo "* soft nofile 1024000" >> /etc/security/limits.conf
echo "mysql hard nofile 1024000" >> /etc/security/limits.conf
echo "mysql soft nofile 1024000" >> /etc/security/limits.conf
echo "root hard nofile 1024000" >> /etc/security/limits.conf
echo "root soft nofile 1024000" >> /etc/security/limits.conf
echo "* soft nofile 1024000" >> /etc/security/limits.d/90-nproc.conf
echo "* hard nofile 1024000" >> /etc/security/limits.d/90-nproc.conf
echo "* soft nproc 10240" >> /etc/security/limits.d/90-nproc.conf
echo "* hard nproc 10240" >> /etc/security/limits.d/90-nproc.conf
echo "root soft nproc unlimited" >> /etc/security/limits.d/90-nproc.conf

# mariadb connector
apt-get install -y libmariadb3 libmariadb-dev
cd /tmp
wget https://dlm.mariadb.com/1601342/Connectors/cpp/connector-cpp-1.0.0/mariadb-connector-cpp-1.0.0-ubuntu-groovy-amd64.tar.gz
tar -xvzf mariadb-connector-cpp-1.0.0-*.tar.gz
rm mariadb-connector-cpp-1.0.0-*.tar.gz
cd -

cd /tmp/mariadb-connector-cpp-1.0.0-*
install -d /usr/include/mariadb/conncpp
install -d /usr/include/mariadb/conncpp/compat
install -v include/mariadb/*.hpp /usr/include/mariadb/
install -v include/mariadb/conncpp/*.hpp /usr/include/mariadb/conncpp
install -v include/mariadb/conncpp/compat/* /usr/include/mariadb/conncpp/compat
install -d /usr/lib/mariadb
install -d /usr/lib/mariadb/plugin
install -v lib64/mariadb/libmariadbcpp.so /usr/lib
install -v lib64/mariadb/plugin/* /usr/lib/mariadb/plugin
cd -

# clear tmp folder
rm -rf /tmp/mariadb*

# Generate docker.bazelrc
echo "test:asan --test_env LSAN_OPTIONS=suppressions=/home/pelton/.lsan_jvm_suppress.txt" > /home/docker.bazelrc
echo "test:tsan --test_env LSAN_OPTIONS=suppressions=/home/pelton/.lsan_jvm_suppress.txt" >> /home/docker.bazelrc
echo "test:tsan --test_env TSAN_OPTIONS=suppressions=/home/pelton/.tsan_jvm_suppress.txt" >> /home/docker.bazelrc

# for GDPRBench, replace python with python2
ln -s /usr/bin/python2 /usr/bin/python

# configure mariadb on startup and run mysqld in the background
mariadb -u root < docker/configure_db.sql

useradd memcached
mkdir -p /tmp/pelton
