cmake_minimum_required(VERSION 3.16)
project(DroneMessagingSystem LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)

# Сервер
add_executable(server_drone server_drone.cpp)
target_link_libraries(server_drone PRIVATE Threads::Threads Boost::system)

# Клиент
add_executable(client_drone client_drone.cpp)
target_link_libraries(client_drone PRIVATE Threads::Threads Boost::system)

# Windows: подключаем WinSock2 и Mswsock
if(WIN32)
    target_link_libraries(server_drone PRIVATE ws2_32 mswsock)
    target_link_libraries(client_drone PRIVATE ws2_32 mswsock)
    target_compile_definitions(server_drone PRIVATE _WIN32_WINNT=0x0601)
    target_compile_definitions(client_drone PRIVATE _WIN32_WINNT=0x0601)
endif()