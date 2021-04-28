# Docker image to build pelton incl. all dependencies
# based on Ubuntu 2004
FROM ubuntu:groovy-20210225

MAINTAINER Pelton team "http://cs.brown.edu/people/malte/research/pbc/"

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG C.UTF-8
ENV PATH /usr/local/bin:$PATH

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    apt-utils libssl-dev lsb-release openssl vim git python3 python3-pip python-is-python3 build-essential libssl-dev zlib1g-dev libncurses5-dev \
    libncursesw5-dev libreadline-dev libsqlite3-dev libgdbm-dev libdb5.3-dev \
     libbz2-dev libexpat1-dev liblzma-dev tk-dev libffi-dev wget gcc-9 g++-9 unzip libsqlite3-dev openjdk-11-jdk clang-10 maven python2

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 \
                                 --slave /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-9 \
                                 --slave /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-9 \
                                 --slave /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-9 \
                                 --slave /usr/bin/g++ g++ /usr/bin/g++-9
RUN update-alternatives --set gcc /usr/bin/gcc-9

# update clang-10 to point to clang
ADD update-alternatives-clang.sh /tmp/update-alternatives-clang.sh
RUN bash /tmp/update-alternatives-clang.sh 10 100

# install bazel using installer to get appropriate version 4.0
RUN cd /tmp && wget https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh \
    && chmod +x bazel-4.0.0-installer-linux-x86_64.sh && ./bazel-4.0.0-installer-linux-x86_64.sh && echo "source /usr/local/lib/bazel/bin/bazel-complete.bash" >> ~/.bashrc && rm bazel-4.0.0-installer-linux-x86_64.sh

# install mysql 8-2 connector
RUN cd /tmp && wget https://dev.mysql.com/get/mysql-apt-config_0.8.16-1_all.deb && dpkg -i mysql-apt-config_0.8.16-1_all.deb && apt-get update && apt-get install -y mysql-server libmysqlcppconn8-2 libmysqlcppconn-dev




# add repokey
RUN mkdir /home/pelton
ADD deploykey.pem /root/.ssh/id_rsa
ADD sshconfig.txt /root/.ssh/config

# clone
RUN git clone --recurse-submodules -j4 git@github.com:brownsys/pelton.git /home/pelton

# now run the bazel test ...  command within /home/Pelton
RUN cd /home/pelton && bazel test ...

# for GDPRBench, replace python with python2
RUN sed -i 's|#!/usr/bin/env python|#!/usr/bin/env python2|' /home/pelton/experiments/GDPRbench/src/bin/ycsb

# clear tmp folder
RUN rm -rf /tmp/*

# for git: repokey necessary
# git clone --recurse-submodules -j4 ...
