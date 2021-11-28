# Docker image to build pelton incl. all dependencies
# based on Ubuntu 2004
FROM ubuntu:focal-20211006

MAINTAINER Pelton team "http://cs.brown.edu/people/malte/research/pbc/"

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG C.UTF-8
ENV PATH /usr/local/bin:$PATH

# Add apt-add-repository
RUN apt-get update && apt-get upgrade -y && apt-get install -y software-properties-common

# Add ppa for gcc-11
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test

# Install dependencies
RUN apt-get update && apt-get install -y \
    apt-utils libssl-dev lsb-release openssl vim git \
    build-essential libssl-dev zlib1g-dev libncurses5-dev \
    libncursesw5-dev libreadline-dev libgdbm-dev libdb5.3-dev libbz2-dev \
    libexpat1-dev liblzma-dev tk-dev libffi-dev wget gcc-11 g++-11 unzip \
    openjdk-11-jdk maven python2 valgrind curl libclang-dev

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 90 \
                                 --slave /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-11 \
                                 --slave /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-11 \
                                 --slave /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-11 \
                                 --slave /usr/bin/g++ g++ /usr/bin/g++-11
RUN update-alternatives --set gcc /usr/bin/gcc-11

# install bazel using installer to get appropriate version 4.0
RUN cd /tmp \
    && wget https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh \
    && chmod +x bazel-4.0.0-installer-linux-x86_64.sh \
    && ./bazel-4.0.0-installer-linux-x86_64.sh \
    && echo "source /usr/local/lib/bazel/bin/bazel-complete.bash" >> ~/.bashrc \
    && rm bazel-4.0.0-installer-linux-x86_64.sh

# install rust
RUN curl https://sh.rustup.rs | sh -s -- -y
RUN /root/.cargo/bin/cargo install cargo-raze

# install mariadb
RUN apt-get remove -y --purge mysql*
RUN apt-get install -y mariadb-server
RUN apt-get install -y mariadb-plugin-rocksdb
RUN echo "[mariadb]" >> /etc/mysql/mariadb.cnf \
    && echo "plugin_load_add = ha_rocksdb" >> /etc/mysql/mariadb.cnf \
    && echo "[mysqld]" >> /etc/mysql/mariadb.cnf \
    && echo "table_open_cache_instances = 1" >> /etc/mysql/mariadb.cnf \
    && echo "table_open_cache = 1000000" >> /etc/mysql/mariadb.cnf \
    && echo "table_definition_cache = 1000000" >> /etc/mysql/mariadb.cnf \
    && echo "open_files_limit = 1000000" >> /etc/mysql/mariadb.cnf

RUN echo "* hard nofile 1024000" >> /etc/security/limits.conf \
    && echo "* soft nofile 1024000" >> /etc/security/limits.conf \
    && echo "mysql hard nofile 1024000" >> /etc/security/limits.conf \
    && echo "mysql soft nofile 1024000" >> /etc/security/limits.conf \
    && echo "root hard nofile 1024000" >> /etc/security/limits.conf \
    && echo "root soft nofile 1024000" >> /etc/security/limits.conf

RUN echo "* soft nofile 1024000" >> /etc/security/limits.d/90-nproc.conf \
    && echo "* hard nofile 1024000" >> /etc/security/limits.d/90-nproc.conf \
    && echo "* soft nproc 10240" >> /etc/security/limits.d/90-nproc.conf \
    && echo "* hard nproc 10240" >> /etc/security/limits.d/90-nproc.conf \
    && echo "root soft nproc unlimited" >> /etc/security/limits.d/90-nproc.conf

# install mariadb connector
RUN apt-get install -y libmariadb3 libmariadb-dev
RUN cd /tmp \
    && wget https://dlm.mariadb.com/1601342/Connectors/cpp/connector-cpp-1.0.0/mariadb-connector-cpp-1.0.0-ubuntu-groovy-amd64.tar.gz \
    && tar -xvzf mariadb-connector-cpp-1.0.0-*.tar.gz

RUN cd /tmp/mariadb-connector-cpp-1.0.0-* \
    && install -d /usr/include/mariadb/conncpp \
    && install -d /usr/include/mariadb/conncpp/compat \
    && install -v include/mariadb/*.hpp /usr/include/mariadb/ \
    && install -v include/mariadb/conncpp/*.hpp /usr/include/mariadb/conncpp \
    && install -v include/mariadb/conncpp/compat/* /usr/include/mariadb/conncpp/compat

RUN cd /tmp/mariadb-connector-cpp-1.0.0-* \
    && install -d /usr/lib/mariadb \
    && install -d /usr/lib/mariadb/plugin \
    && install -v lib64/mariadb/libmariadbcpp.so /usr/lib \
    && install -v lib64/mariadb/plugin/* /usr/lib/mariadb/plugin

# configure mariadb user
RUN chown -R mysql:root /var/lib/mysql
RUN mkdir -p /run/mysqld
RUN chown -R mysql:root /run/mysqld/

# clear tmp folder
RUN rm -rf /tmp/*

# Do not copy, instead bind mount during docker run
RUN mkdir /home/pelton

# Generate docker.bazelrc
RUN echo "test:asan --test_env LSAN_OPTIONS=suppressions=/home/pelton/.lsan_jvm_suppress.txt" > /home/docker.bazelrc
RUN echo "test:tsan --test_env LSAN_OPTIONS=suppressions=/home/pelton/.lsan_jvm_suppress.txt" >> /home/docker.bazelrc
RUN echo "test:tsan --test_env TSAN_OPTIONS=suppressions=/home/pelton/.tsan_jvm_suppress.txt" >> /home/docker.bazelrc

# for GDPRBench, replace python with python2
RUN ln -s /usr/bin/python2 /usr/bin/python

# configure mariadb on startup and run mysqld in the background
ADD docker/configure_db.sql /home/configure_db.sql
ADD docker/configure_db.sh /home/configure_db.sh
RUN chmod 750 /home/configure_db.sh

ENTRYPOINT ["/bin/bash", "./home/configure_db.sh"]


# Run with docker run -v .:/home/pelton
