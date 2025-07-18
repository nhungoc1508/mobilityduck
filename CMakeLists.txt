cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME mobilityduck)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

add_library(meos_lib STATIC
  src/meos/meos_internal.c
  src/meos/meos_catalog.c
  src/meos/type_util.c
)
target_include_directories(meos_lib
  PUBLIC src/meos
)
install(TARGETS meos_lib EXPORT "${DUCKDB_EXPORT_SET}" ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

set(EXTENSION_SOURCES
    src/mobilityduck_extension.cpp
    src/functions.cpp
    src/types.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} meos_lib OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} meos_lib OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
