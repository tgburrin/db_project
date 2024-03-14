cmake_minimum_required(VERSION 3.24)
project(db_dictionary C)

set(CMAKE_C_STANDARD 23)

set(DB_MEM ../db_mem)
include_directories(${DB_MEM})
link_directories(AFTER ${DB_MEM}/cmake-build-debug/)

add_executable(db_dictionary main.c)
target_link_libraries(db_dictionary db_mem)