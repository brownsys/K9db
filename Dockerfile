# Docker image to build pelton incl. all dependencies
# based on Ubuntu 2004
FROM ubuntu:groovy-20210225

MAINTAINER Pelton team "http://cs.brown.edu/people/malte/research/pbc/"

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG C.UTF-8
ENV PATH /usr/local/bin:$PATH

# Copy pelton directory
COPY . /home/pelton

# Install dependencies
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    apt-utils libssl-dev lsb-release openssl vim git python3 python3-pip \
    python-is-python3 build-essential libssl-dev zlib1g-dev libncurses5-dev \
    libncursesw5-dev libreadline-dev libgdbm-dev libdb5.3-dev libbz2-dev \
    libexpat1-dev liblzma-dev tk-dev libffi-dev wget gcc-9 g++-9 unzip \
    openjdk-11-jdk maven python2 valgrind

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 \
                                 --slave /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-9 \
                                 --slave /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-9 \
                                 --slave /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-9 \
                                 --slave /usr/bin/g++ g++ /usr/bin/g++-9
RUN update-alternatives --set gcc /usr/bin/gcc-9

# install bazel using installer to get appropriate version 4.0
RUN cd /tmp \
    && wget https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh \
    && chmod +x bazel-4.0.0-installer-linux-x86_64.sh \
    && ./bazel-4.0.0-installer-linux-x86_64.sh \
    && echo "source /usr/local/lib/bazel/bin/bazel-complete.bash" >> ~/.bashrc \
    && rm bazel-4.0.0-installer-linux-x86_64.sh

# install mariadb
RUN apt-get remove -y --purge mysql*
RUN apt-get install -y mariadb-server
RUN apt-get install -y mariadb-plugin-rocksdb
RUN echo "[mariadb]" >> /etc/mysql/mariadb.cnf \
    && echo "plugin_load_add = ha_rocksdb" >> /etc/mysql/mariadb.cnf

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

# for GDPRBench, replace python with python2
RUN sed -i 's|#!/usr/bin/env python|#!/usr/bin/env python2|' /home/pelton/experiments/GDPRbench/src/bin/ycsb

# clear tmp folder
RUN rm -rf /tmp/*

# configure mariadb on startup and run mysqld in the background
RUN cp /home/pelton/docker/configure_db.sql /home/configure_db.sql
RUN cp /home/pelton/docker/configure_db.sh /home/configure_db.sh
RUN chmod 750 /home/configure_db.sh
ENTRYPOINT ["/bin/bash", "./home/configure_db.sh"]
