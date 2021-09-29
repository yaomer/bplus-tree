cmake_minimum_required (VERSION 2.8)

project (bplus-tree)

set (CMAKE_CXX_FLAGS "-std=c++17 -Wall -O2")

set (SRC "${PROJECT_SOURCE_DIR}/src")

set (DB
    ${SRC}/db.cc
    ${SRC}/disk.cc
    ${SRC}/page.cc
)

set (HEADERS
    ${SRC}/db.h
    ${SRC}/disk.h
    ${SRC}/page.h
    ${SRC}/codec.h
)

add_library (bpdb STATIC ${DB})

install(TARGETS bpdb
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib)

install(FILES ${HEADERS} DESTINATION include/bpdb)
