cmake_minimum_required(VERSION 3.16)
project(TelemetrySystem LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(TBB QUIET)

add_executable(telemetry_system main.cpp)
target_link_libraries(telemetry_system PRIVATE Threads::Threads)

if(TBB_FOUND)
    target_link_libraries(telemetry_system PRIVATE TBB::tbb)
    message(STATUS "TBB found — parallel execution policies enabled")
else()
    message(STATUS "TBB not found — installing...")
endif()

target_compile_options(telemetry_system PRIVATE -O2 -march=native)