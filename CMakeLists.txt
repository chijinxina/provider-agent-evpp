cmake_minimum_required(VERSION 3.7)
project(provider-agent-folly)
set(CMAKE_CXX_STANDARD 11)
#set(CMAKE_BUILD_TYPE "Release")

set (CXX_FLAGS
        -g
        -fPIC
        -Wall
        -Wno-unused-parameter
        -Wno-unused-function
        -Wunused-variable
        -Wunused-value
        -Wshadow
        -Wcast-qual
        -Wcast-align
        -Wwrite-strings
        -Wsign-compare
        -Winvalid-pch
        -fms-extensions
        -Wfloat-equal
        -Wextra
        -std=c++11
        )

find_library(CPPREST_LIB NAMES cpprest)
find_path(CPPREST_INCLUDE_DIR NAMES cpprest/http_client.h)
#查找Boost库
find_package(Boost REQUIRED COMPONENTS system thread locale random)


include_directories (${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIR} ${CPPREST_INCLUDE_DIR})

SET(CMAKE_CXX_FLAGS_DEBUG   "-O0 -g -ggdb -D_DEBUG -DGOOGLE_STRIP_LOG=0 ")
SET(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -ggdb -DNDEBUG -DGOOGLE_STRIP_LOG=1 ")
SET(DEPENDENT_LIBRARIES event glog gflags pthread)

set (EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set (LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

message (STATUS "This is BINARY dir " ${PROJECT_BINARY_DIR})
message (STATUS "This is SOURCE dir " ${PROJECT_SOURCE_DIR})
message (STATUS "CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS})
message (STATUS "EXECUTABLE_OUTPUT_PATH " ${EXECUTABLE_OUTPUT_PATH})
message (STATUS "LIBRARY_OUTPUT_PATH " ${LIBRARY_OUTPUT_PATH})
message (STATUS "THRIFT_COMPILER " ${THRIFT_COMPILER})
message (STATUS "THRIFT_INCLUDE_DIR " ${THRIFT_INCLUDE_DIR})
message (STATUS "THRIFT_LIBRARY " ${THRIFT_LIBRARY})

add_subdirectory (evpp)

set (CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
include (utils)
include (packages)

include (CPack)
include (CTest)

add_executable(provider-agent-folly main.cpp)
target_link_libraries(provider-agent-folly evpp_static
        ${DEPENDENT_LIBRARIES} ${Boost_LIBRARIES}
        etcd-cpp-api cpprest ssl crypto protobuf grpc++)
