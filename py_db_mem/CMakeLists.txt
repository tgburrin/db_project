cmake_minimum_required(VERSION 3.24)
project(py_db_mem C)

set(CMAKE_C_STANDARD 23)

set(PYTHON /usr/include/python3.11/)
set(DB_MEM ../db_mem)
include_directories(${PYTHON})
include_directories(${DB_MEM})

link_directories(AFTER ${DB_MEM})

add_library(py_db_mem SHARED py_data_dictionary.c
        py_db_mem.c
        py_dd_table.c
        py_dd_table_record.c
        py_error.c)
set_target_properties(py_db_mem PROPERTIES PREFIX "")
target_link_libraries(py_db_mem db_mem)