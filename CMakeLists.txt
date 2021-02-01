cmake_minimum_required(VERSION 3.15)
project(Pelton VERSION 0.0.1 DESCRIPTION "Dataflow w. privacy project")

find_package(GTest CONFIG)
find_package(gflags REQUIRED)
find_package(Glog REQUIRED)

# enable c++14
set(CMAKE_CXX_STANDARD 14)

include(GoogleTest)
add_subdirectory(dataflow)

add_executable(testpelton src/main.cc)
target_link_libraries(testpelton dataflow
        ${GTest_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        glog::glog)

gtest_add_tests(TARGET testpelton TEST_PREFIX "")