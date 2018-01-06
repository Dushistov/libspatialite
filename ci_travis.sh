#!/bin/bash -e

TCI_NUMTHREADS=2
if [[ -f /sys/devices/system/cpu/online ]]; then
	# Calculates 1.5 times physical threads
	TCI_NUMTHREADS=$(( ( $(cut -f 2 -d '-' /sys/devices/system/cpu/online) + 1 ) * 15 / 10  ))
fi

run_make()
{
    [ $TCI_NUMTHREADS -gt 0 ] && make -j $TCI_NUMTHREADS || make 
}

if [ "$LIBSPATIALITE_BUILD_TOOL" == "autotools" ]; then
    ./configure --prefix=/usr --enable-libxml2 --disable-freexl
    run_make
    make check
else
    rm -fr travis_cmake_build
    mkdir travis_cmake_build
    cd travis_cmake_build
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DSPATIALITE_ENABLE_TESTS=On ..
    run_make
    ctest -V --output-on-failure
fi
