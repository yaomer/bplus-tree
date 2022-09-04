cmake_minimum_required (VERSION 2.8)

project (bplus-tree)

set (CMAKE_CXX_FLAGS "-std=c++17 -Wall -O2")

include (CheckCXXSymbolExists)

CHECK_CXX_SYMBOL_EXISTS(fdatasync "unistd.h" HAVE_FDATASYNC)
CHECK_CXX_SYMBOL_EXISTS(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)

set (SRC "${PROJECT_SOURCE_DIR}/bpdb")

configure_file (
    "${SRC}/config.h.in"
    "${SRC}/config.h"
)

set (DB
    ${SRC}/db.cc
    ${SRC}/db_iter.cc
    ${SRC}/disk.cc
    ${SRC}/page.cc
    ${SRC}/log.cc
    ${SRC}/transaction.cc
    ${SRC}/transaction_lock.cc
    ${SRC}/version.cc
    ${SRC}/util.cc
)

set (HEADERS
    ${SRC}/db.h
    ${SRC}/disk.h
    ${SRC}/page.h
    ${SRC}/codec.h
    ${SRC}/common.h
    ${SRC}/log.h
    ${SRC}/transaction.h
    ${SRC}/transaction_lock.h
    ${SRC}/version.h
)

add_library (bpdb STATIC ${DB})

install(TARGETS bpdb
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib)

install(FILES ${HEADERS} DESTINATION include/bpdb)

