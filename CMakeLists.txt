cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME mobilityduck)

# Set the module path for custom Find modules
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src/cmake" ${CMAKE_MODULE_PATH})

option(MEOS
  "Set MEOS (default=ON)
  "
  ON
)

project(${TARGET_NAME})

set(MOBILITYDB_VERSION "${PROJECT_VERSION}")
set(MOBILITYDB_VERSION_STRING "${CMAKE_PROJECT_NAME} ${PROJECT_VERSION}${PROJECT_VERSION_DEV}")
add_definitions(-DMOBILITYDB_VERSION_STRING="${MOBILITYDB_VERSION_STRING}")
message(STATUS "MOBILITYDB_VERSION_STRING = '${MOBILITYDB_VERSION_STRING}'")

# Check machine architecture: big endian vs. little endian
# Needed for WKB support in MobilityDB and PostGIS (file postgis_config.h.in)
include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(IS_BIG_ENDIAN)
  message(STATUS "BIG_ENDIAN")
  add_definitions(-DMEOS_IS_BIG_ENDIAN=1)
  set(DEF_WORDS_BIGENDIAN ON)
else()
  message(STATUS "LITTLE_ENDIAN")
  add_definitions(-DMEOS_IS_BIG_ENDIAN=0)
  unset(DEF_WORDS_BIGENDIAN)
endif()

# list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
# GEOS geometry library
find_package(GEOS REQUIRED)
include_directories(SYSTEM ${GEOS_INCLUDE_DIR})
math(EXPR POSTGIS_GEOS_VERSION "${GEOS_VERSION_MAJOR} * 10000 + ${GEOS_VERSION_MINOR} * 100 + ${GEOS_VERSION_MICRO}")
message(STATUS "POSTGIS_GEOS_VERSION: ${POSTGIS_GEOS_VERSION}")

# Proj reprojection library
find_package(PROJ REQUIRED)
include_directories(SYSTEM ${PROJ_INCLUDE_DIRS})
math(EXPR POSTGIS_PROJ_VERSION "${PROJ_VERSION_MAJOR} * 10 + ${PROJ_VERSION_MINOR}")
message(STATUS "POSTGIS_PROJ_VERSION: ${POSTGIS_PROJ_VERSION}")

# JSON-C library (used for MF-JSON input/output)
find_package(JSON-C REQUIRED)
include_directories(SYSTEM ${JSON-C_INCLUDE_DIRS})
message(STATUS "JSON-C_INCLUDE_DIRS: ${JSON-C_INCLUDE_DIRS}")
message(STATUS "JSON-C_LIBRARIES: ${JSON-C_LIBRARIES}")

# GSL (GNU Scientific Library) (used for skiplists)
find_package(GSL REQUIRED)
include_directories(SYSTEM ${GSL_INCLUDE_DIRS})
message(STATUS "GSL_VERSION: ${GSL_VERSION}")
add_definitions(-DGSL_VERSION_STRING="${GSL_VERSION}")

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)
configure_file(./src/postgis/liblwgeom/liblwgeom.h.in ./src/postgis/liblwgeom/liblwgeom.h)
configure_file(./src/postgis/postgis_config.h.in ./src/postgis/postgis_config.h)
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/src/postgis/liblwgeom")
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/src/postgis/libpgcommon")
include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/src/postgis")
include_directories(SYSTEM "${CMAKE_CURRENT_BINARY_DIR}/src/postgis/liblwgeom")
include_directories(SYSTEM "${CMAKE_CURRENT_BINARY_DIR}/src/postgis")
# add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/src/postgis" "postgis")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/meos/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/meos/postgres")
# add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/src/meos/src" "meos_src")
include_directories(SYSTEM /usr/include/json-c)
include_directories("${CMAKE_BINARY_DIR}/extension/mobilityduck/meos/postgres")
include_directories("${CMAKE_BINARY_DIR}/extension/mobilityduck/src/meos/postgres")


# --- Add MEOS as a subdirectory ---
add_subdirectory(src/meos)

set(EXTENSION_SOURCES
    src/mobilityduck_extension.cpp
    src/functions.cpp
    src/temporal/tint.cpp
    src/temporal/tbool.cpp
    src/temporal/temporal_types.cpp
    src/temporal/temporal_functions.cpp
    src/span.cpp
    src/tgeometry.cpp
    src/tgeompoint.cpp
    src/set.cpp
    src/geomset.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# --- Link against MEOS library (adjust name if needed) ---
target_link_libraries(${EXTENSION_NAME} meos OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} meos OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

install(TARGETS meos
    EXPORT DuckDBExports
    DESTINATION lib
)