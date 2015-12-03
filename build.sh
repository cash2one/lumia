#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################

WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

# boost
if [ -f "boost_1_57_0.tar.gz" ]
then
    echo "boost exist"
else
    wget http://superb-dca2.dl.sourceforge.net/project/boost/boost/1.57.0/boost_1_57_0.tar.gz
    tar zxf boost_1_57_0.tar.gz
    rm -rf ${DEPS_PREFIX}/boost_1_57_0
    mv boost_1_57_0 ${DEPS_PREFIX}
fi

if [ -d "rapidjson" ]
then
    echo "rapid json exist"
else
    # rapidjson
    git clone https://github.com/miloyip/rapidjson.git
    rm -rf ${DEPS_PREFIX}/rapidjson
    cp -rf rapidjson ${DEPS_PREFIX}
fi

if [ -d "protobuf" ]
then
    echo "protobuf exist"
else
    # protobuf
    # wget --no-check-certificate https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz
    git clone --depth=1 https://github.com/00k/protobuf
    mv protobuf/protobuf-2.6.1.tar.gz .
    tar zxf protobuf-2.6.1.tar.gz
    cd protobuf-2.6.1
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

if [ -d "snappy" ]
then
    echo "snappy exist"
else
    # snappy
    # wget --no-check-certificate https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
    git clone --depth=1 https://github.com/00k/snappy
    mv snappy/snappy-1.1.1.tar.gz .
    tar zxf snappy-1.1.1.tar.gz
    cd snappy-1.1.1
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

if [ -f "sofa-pbrpc-1.0.0.tar.gz" ]
then
    echo "sofa exist"
else
    # sofa-pbrpc
    wget --no-check-certificate -O sofa-pbrpc-1.0.0.tar.gz https://github.com/BaiduPS/sofa-pbrpc/archive/v1.0.0.tar.gz
    tar zxf sofa-pbrpc-1.0.0.tar.gz
    cd sofa-pbrpc-1.0.0
    sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
    sed -i '/PROTOBUF_DIR=/ d' depends.mk
    sed -i '/SNAPPY_DIR=/ d' depends.mk
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    cd src
    PROTOBUF_DIR=${DEPS_PREFIX} sh compile_proto.sh
    cd ..
    make -j4
    make install
    cd ..
fi

if [ -f "zookeeper-3.4.6.tar.gz" ]
then
    echo "zookeeper-3.4.6.tar.gz exist"
else
    # zookeeper
    wget http://www.us.apache.org/dist/zookeeper/stable/zookeeper-3.4.6.tar.gz
    tar zxf zookeeper-3.4.6.tar.gz
    cd zookeeper-3.4.6/src/c
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

if [ -f "CMake-3.2.1.tar.gz" ]
then
    echo "CMake-3.2.1.tar.gz exist"
else
    # cmake for gflags
    wget --no-check-certificate -O CMake-3.2.1.tar.gz https://github.com/Kitware/CMake/archive/v3.2.1.tar.gz
    tar zxf CMake-3.2.1.tar.gz
    cd CMake-3.2.1
    ./configure --prefix=${DEPS_PREFIX}
    make -j4
    make install
    cd -
fi

if [ -f "gflags-2.1.1.tar.gz" ]
then
    echo "gflags-2.1.1.tar.gz exist"
else
    # gflags
    wget --no-check-certificate -O gflags-2.1.1.tar.gz https://github.com/schuhschuh/gflags/archive/v2.1.1.tar.gz
    tar zxf gflags-2.1.1.tar.gz
    cd gflags-2.1.1
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
    make -j4
    make install
    cd -
fi

if [ -f "glog-0.3.3.tar.gz" ] 
then
    echo "glog-0.3.3.tar.gz exist"
else
    # glog
    wget --no-check-certificate -O glog-0.3.3.tar.gz https://github.com/google/glog/archive/v0.3.3.tar.gz
    tar zxf glog-0.3.3.tar.gz
    cd glog-0.3.3
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
    cd -
fi

if [ -d "gtest_archive" ]
then
    echo "gtest_archive exist"
else

    # gtest
    # wget --no-check-certificate https://googletest.googlecode.com/files/gtest-1.7.0.zip
    git clone --depth=1 https://github.com/xupeilin/gtest_archive
    mv gtest_archive/gtest-1.7.0.zip .
    unzip gtest-1.7.0.zip
    cd gtest-1.7.0
    ./configure ${DEPS_CONFIG}
    make
    cp -a lib/.libs/* ${DEPS_PREFIX}/lib
    cp -a include/gtest ${DEPS_PREFIX}/include
    cd -
fi

if [ -f "libunwind-0.99-beta.tar.gz" ]
then
    echo "libunwind-0.99-beta.tar.gz exist"
else
    # libunwind for gperftools
    wget http://download.savannah.gnu.org/releases/libunwind/libunwind-0.99-beta.tar.gz
    tar zxf libunwind-0.99-beta.tar.gz
    cd libunwind-0.99-beta
    ./configure ${DEPS_CONFIG}
    make CFLAGS=-fPIC -j4
    make CFLAGS=-fPIC install
    cd -
fi

if [ -d "gperftools" ]
then
    echo "gperftools exist"
else

    # gperftools (tcmalloc)
    # wget --no-check-certificate https://googledrive.com/host/0B6NtGsLhIcf7MWxMMF9JdTN3UVk/gperftools-2.2.1.tar.gz
    git clone --depth=1 https://github.com/00k/gperftools
    mv gperftools/gperftools-2.2.1.tar.gz .
    tar zxf gperftools-2.2.1.tar.gz
    cd gperftools-2.2.1
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
    cd -
fi

if [ -d "leveldb" ]
then
    echo "leveldb exist"
else

    # leveldb
    git clone https://github.com/imotai/leveldb.git
    cd leveldb
    make -j8 
    cp -rf include/* ${DEPS_PREFIX}/include
    cp libleveldb.a ${DEPS_PREFIX}/lib
    cd -
fi


if [ -d "ins" ]
then
    echo "ins exist"
else
    # ins
    git clone https://github.com/fxsjy/ins
    cd ins
    set -i 's/^SNAPPY_PATH=.*/SNAPPY_PATH=..\/..\/thirdparty/' Makefile
    set -i 's/^PROTOBUF_PATH=.*/PROTOBUF_PATH=..\/..\/thirdparty/' Makefile
    set -i 's/^PROTOC_PATH=.*/PROTOC_PATH=..\/..\/thirdparty\/bin/' Makefile
    set -i 's/^PROTOC=.*/PROTOC=..\/..\/thirdparty\/bin\/protoc/' Makefile
    set -i 's/\./thirdparty\/sofa-pbrpc\/output\//\/..\/thirdparty/' Makefile
    set -i 's/\./thirdparty\/boost\//..\/..\/thirdparty\/boost_1_57_0/' Makefile
    sed -i "s|^PREFIX=.*|PREFIX=${DEPS_PREFIX}|" Makefile
    sed -i "s|^PROTOC=.*|PROTOC=${DEPS_PREFIX}/bin/protoc|" Makefile
    BOOST_PATH=${DEPS_PREFIX}/boost_1_57_0 make install_sdk
    make -j4 ins && make -j4 install_sdk
    mkdir -p output/bin && cp ins output/bin
    cd -
fi

if [ -d "tera" ]
then 
    echo "tera exist"
else

    # tera
    git clone https://github.com/baidu/tera
    cd tera
    sed -i 's/^SOFA_PBRPC_PREFIX=.*/SOFA_PBRPC_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^PROTOBUF_PREFIX=.*/PROTOBUF_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^SNAPPY_PREFIX=.*/SNAPPY_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^ZOOKEEPER_PREFIX=.*/ZOOKEEPER_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GFLAGS_PREFIX=.*/GFLAGS_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GLOG_PREFIX=.*/GLOG_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GTEST_PREFIX=.*/GTEST_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GPERFTOOLS_PREFIX=.*/GPERFTOOLS_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^BOOST_INCDIR=.*/BOOST_INCDIR=..\/..\/thirdparty\/boost_1_57_0/' depends.mk
    sed -i 's/^INS_PREFIX=.*/INS_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -e '$ c -lgtest_main -lgtest -lglog -lgflags -ltcmalloc_minimal -lunwind' depends.mk > depends.mk.new
    mv depends.mk.new depends.mk
    make -j8
    cp -a build/lib/*.a ${DEPS_PREFIX}/lib
    cp -a build/include/*.h ${DEPS_PREFIX}/include
    cd -
fi

if [ -d "mdt" ]
then
    echo "mdt exist"
else 
    # mdt
    git clone -b zip-log https://github.com/imotai/mdt.git 
    cd mdt
    sed -i 's/^SOFA_PBRPC_PREFIX=.*/SOFA_PBRPC_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^PROTOBUF_PREFIX=.*/PROTOBUF_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^SNAPPY_PREFIX=.*/SNAPPY_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^ZOOKEEPER_PREFIX=.*/ZOOKEEPER_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GFLAGS_PREFIX=.*/GFLAGS_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GLOG_PREFIX=.*/GLOG_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GTEST_PREFIX=.*/GTEST_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^GPERFTOOLS_PREFIX=.*/GPERFTOOLS_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^BOOST_INCDIR=.*/BOOST_INCDIR=..\/..\/thirdparty\/boost_1_57_0/' depends.mk
    sed -i 's/^INS_PREFIX=.*/INS_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i 's/^TERA_PREFIX=.*/TERA_PREFIX=..\/..\/thirdparty/' depends.mk
    sed -i '/-lgtest_main/c -lgtest_main -lgtest -lglog -lgflags -ltcmalloc_minimal -lunwind' depends.mk
    make -j8
    cp src/ftrace/collector/logger.h ${DEPS_PREFIX}/include
    cp libftrace.a ${DEPS_PREFIX}/lib
    cd -
fi

if [ -f "curl-7.46.0.tar.gz" ]
then
    echo "curl exist"
else
    wget https://github.com/bagder/curl/releases/download/curl-7_46_0/curl-7.46.0.tar.gz
    tar -zxvf curl-7.46.0.tar.gz
    cd curl-7.46.0
    cmake .
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
fi

# common
git clone https://github.com/baidu/common.git
cd common
sed -i 's/^INCLUDE_PATH=.*/INCLUDE_PATH=-Iinclude -I..\/..\/thirdparty\/boost_1_57_0/' Makefile
make -j8
cp -rf include/* ${DEPS_PREFIX}/include
cp -rf libcommon.a ${DEPS_PREFIX}/lib
cd -
cd ${WORK_DIR}

########################################
# build galaxy
########################################

#make -j4

