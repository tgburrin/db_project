cmake_minimum_required(VERSION 3.24)
project(db_server C)

set(CMAKE_C_STANDARD 23)

find_library(cjson cjson)
find_library(uuid uuid)
find_library(z z)

set(DB_MEM ../db_mem)
include_directories(${DB_MEM})
link_directories(AFTER ${DB_MEM}/cmake-build-debug/)

add_executable(db_server main.c)
target_link_libraries(db_server db_mem cjson uuid z)