cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME mobilityduck)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set the module path for custom Find modules
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src/cmake" ${CMAKE_MODULE_PATH})

# Detect architecture endianness (needed by PostGIS WKB)
include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(IS_BIG_ENDIAN)
  message(STATUS "BIG_ENDIAN")
  add_definitions(-DMEOS_IS_BIG_ENDIAN=1)
else()
  message(STATUS "LITTLE_ENDIAN")
  add_definitions(-DMEOS_IS_BIG_ENDIAN=0)
endif()

# Dependency: GEOS
find_package(GEOS REQUIRED)
include_directories(SYSTEM ${GEOS_INCLUDE_DIR})
math(EXPR POSTGIS_GEOS_VERSION "${GEOS_VERSION_MAJOR} * 10000 + ${GEOS_VERSION_MINOR} * 100 + ${GEOS_VERSION_MINOR}")
message(STATUS "POSTGIS_GEOS_VERSION: ${POSTGIS_GEOS_VERSION}")

# Dependency: PROJ
find_package(PROJ REQUIRED)
include_directories(SYSTEM ${PROJ_INCLUDE_DIRS})
math(EXPR POSTGIS_PROJ_VERSION "${PROJ_VERSION_MAJOR} * 10 + ${PROJ_VERSION_MINOR}")
message(STATUS "POSTGIS_PROJ_VERSION: ${POSTGIS_PROJ_VERSION}")

# Dependency: JSON-C
find_package(JSON-C REQUIRED)
include_directories(SYSTEM ${JSON-C_INCLUDE_DIRS})
message(STATUS "JSON-C_INCLUDE_DIRS: ${JSON-C_INCLUDE_DIRS}")

# Dependency: GSL
find_package(GSL REQUIRED)
include_directories(SYSTEM ${GSL_INCLUDE_DIRS})
add_definitions(-DGSL_VERSION_STRING="${GSL_VERSION}")
message(STATUS "GSL_VERSION: ${GSL_VERSION}")

# Dependency: OpenSSL (used by DuckDB build system)
find_package(OpenSSL REQUIRED)

# -----------------------------
# Use externally installed MEOS
# -----------------------------
include_directories(/usr/local/include)

find_library(MEOS_LIBRARY meos PATHS /usr/local/lib)

if(NOT MEOS_LIBRARY)
  message(FATAL_ERROR "libmeos.so not found in /usr/local/lib")
endif()

message(STATUS "Using MEOS library at: ${MEOS_LIBRARY}")

# -----------------------------
# DuckDB Extension Logic
# -----------------------------

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)

# Spatial/PostGIS compatibility headers (if needed by functions)
# configure_file(./src/postgis/liblwgeom/liblwgeom.h.in ./src/postgis/liblwgeom/liblwgeom.h)
# configure_file(./src/postgis/postgis_config.h.in ./src/postgis/postgis_config.h)
# include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/src/postgis/liblwgeom")
# include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/src/postgis/libpgcommon")
# include_directories(SYSTEM "${CMAKE_CURRENT_SOURCE_DIR}/src/postgis")
# include_directories(SYSTEM "${CMAKE_CURRENT_BINARY_DIR}/src/postgis/liblwgeom")
# include_directories(SYSTEM "${CMAKE_CURRENT_BINARY_DIR}/src/postgis")

set(EXTENSION_SOURCES
    src/mobilityduck_extension.cpp
    # src/temporal/tint.cpp
    # src/temporal/tbool.cpp
    # src/temporal/temporal_types.cpp
    src/temporal/temporal.cpp
    src/temporal/temporal_functions.cpp
    # src/temporal/tbox.cpp
    # src/temporal/tbox_functions.cpp
    src/gen/tbox_temp.cpp
    src/gen/tbox_temp_functions.cpp
    src/set.cpp    
    src/span.cpp
    src/geoset.cpp
    src/spanset.cpp
    src/tgeo/tgeometry.cpp
    src/tgeo/tgeometry_in_out.cpp
    src/tgeo/tgeompoint.cpp
    )

# Build extension targets
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link external MEOS + dependencies
target_link_libraries(${EXTENSION_NAME} ${MEOS_LIBRARY} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${MEOS_LIBRARY} OpenSSL::SSL OpenSSL::Crypto)

# Install
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
