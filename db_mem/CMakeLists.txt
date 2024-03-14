cmake_minimum_required(VERSION 3.24)
project(db_mem C)

set(CMAKE_C_STANDARD 23)

find_library(CJSON cjson)
find_library(ZLIB z)
find_library(UUID uuid)

add_library(db_mem SHARED
        data_dictionary.c
        db_interface.c
        index_tools.c
        journal_tools.c
        server_tools.c
        table_tools.c
        utils.c)

target_link_libraries(db_mem PRIVATE "${CJSON}" "${ZLIB}" "${UUID}")
