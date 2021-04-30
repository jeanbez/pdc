#!/usr/bin/env bash

set -eu -o pipefail

sudo apt-get update
sudo apt-get install libopenmpi-dev

# libfabric
git clone https://github.com/ofiwg/libfabric.git
cd libfabric
./autogen.sh
./configure --disable-usnic --disable-mrail --disable-rstream --disable-perf --disable-efa --disable-psm2 --disable-psm --disable-verbs --disable-shm --disable-static --disable-silent-rules
make -j2 && sudo make install
# make check
cd ..

# Mercury
git clone --recursive https://github.com/mercury-hpc/mercury.git
cd mercury
mkdir build && cd build
cmake ../  -DCMAKE_C_COMPILER=gcc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF
make -j2 && sudo make install
# ctest
