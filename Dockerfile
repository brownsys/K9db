# Docker image to build k9db incl. all dependencies
# based on Ubuntu 2004
FROM ubuntu:focal-20211006

MAINTAINER K9db team "http://cs.brown.edu/people/malte/research/pbc/"

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
    openjdk-11-jdk maven python2 valgrind curl libclang-dev flex bison libevent-dev \
    libsnappy-dev netcat

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

# install mariadb (for baselines only)
RUN apt-get remove -y --purge mysql*
RUN curl -LsS -O https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
RUN bash mariadb_repo_setup --mariadb-server-version=10.6
RUN rm mariadb_repo_setup
RUN apt-get update
RUN apt-get install -y mariadb-server-10.6 mariadb-client-10.6
RUN apt-get install -y mariadb-plugin-rocksdb

# install mariadb connector (for memcached)
RUN apt-get update
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

# configure mariadb and memcached users
RUN chown -R mysql:root /var/lib/mysql
RUN mkdir -p /run/mysqld
RUN chown -R mysql:root /run/mysqld/
RUN useradd memcached

# configure mariadb datadir
RUN echo '[mysqld]' >> /etc/mysql/mariadb.cnf
RUN echo 'datadir = /var/lib/mysql' >> /etc/mysql/mariadb.cnf

# Do not copy, instead bind mount during docker run
RUN mkdir /home/k9db

# Generate docker.bazelrc
RUN echo "test:asan --test_env LSAN_OPTIONS=suppressions=/home/k9db/.lsan_jvm_suppress.txt" > /home/docker.bazelrc
RUN echo "test:tsan --test_env LSAN_OPTIONS=suppressions=/home/k9db/.lsan_jvm_suppress.txt" >> /home/docker.bazelrc
RUN echo "test:tsan --test_env TSAN_OPTIONS=suppressions=/home/k9db/.tsan_jvm_suppress.txt" >> /home/docker.bazelrc

# for GDPRBench, replace python with python2
RUN ln -s /usr/bin/python2 /usr/bin/python

# Latex for plotting
RUN apt-get update && apt-get install --no-install-recommends -y \
  texlive-latex-base texlive-latex-extra \
  texlive-fonts-recommended texlive-fonts-extra \
  dvipng cm-super
RUN apt-get update && apt-get install -y python3-pip python3.8-venv

# configure mariadb on startup and run mysqld in the background
ADD configure_db.sql /home/configure_db.sql
ADD configure_docker.sh /home/configure_docker.sh
RUN chmod 750 /home/configure_docker.sh

# Install 

RUN curl https://sh.rustup.rs | sh -s -- -y
RUN . /root/.cargo/env && rustup default nightly-2023-12-06

# Install cargo raze
RUN git clone https://github.com/KinanBab/cargo-raze.git /home/cargo-raze
RUN . /root/.cargo/env && cd /home/cargo-raze/impl && cargo install --locked --path .

EXPOSE 10001/tcp
EXPOSE 3306/tcp

ENTRYPOINT /bin/bash ./home/configure_docker.sh && /bin/bash

# Run with docker run --mount type=bind,source=$(pwd),target=/home/k9db --name k9db -d -p 3306:3306 -p 10001:10001 -t k9db/latest
