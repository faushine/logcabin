FROM ubuntu:latest
RUN apt-get update \
    && apt-get install -y \
                        python3 python3-pip python3-setuptools python3-wheel \
                        vim \
                        udev \
                        pkg-config \
                        wget \
                        cmake \
                        autoconf automake libtool curl make g++ unzip \
                        git \
                        scons \
                        crypto++ \
                        libgtest-dev \
    && cd /usr/local/bin \
    && ln -s /usr/bin/python3 python \
    && pip3 install --upgrade pip
RUN pip3 install pytest

# install protobuf
WORKDIR /protobuf
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz \
    && tar xvzf protobuf-2.6.1.tar.gz
WORKDIR /protobuf/protobuf-2.6.1
RUN ./configure \
    && make -j4 \
    && make check -j4 \
    && make install -j4 \
    && ldconfig # refresh shared library cache.

WORKDIR /logcabin
