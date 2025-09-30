cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME mobilityduck)
project(${TARGET_NAME} LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/src/cmake" ${CMAKE_MODULE_PATH})

include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(IS_BIG_ENDIAN)
  add_definitions(-DMEOS_IS_BIG_ENDIAN=1)
else()
  add_definitions(-DMEOS_IS_BIG_ENDIAN=0)
endif()

find_package(GEOS CONFIG REQUIRED)        
find_package(PROJ CONFIG REQUIRED)        
find_package(GSL REQUIRED)                
find_package(OpenSSL REQUIRED)            
find_package(json-c CONFIG QUIET)
if(NOT json-c_FOUND)
  find_package(JSON-C REQUIRED)           
endif()

find_package(MEOS CONFIG REQUIRED)        

if(TARGET GEOS::geos_c)
  set(GEOS_TGT GEOS::geos_c)
elseif(TARGET GEOS::geos)
  set(GEOS_TGT GEOS::geos)
else()
  message(FATAL_ERROR "Neither GEOS::geos_c nor GEOS::geos was found.")
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/mobilityduck_extension.cpp
    src/temporal/temporal.cpp
    src/temporal/temporal_functions.cpp
    src/temporal/tbox.cpp
    src/temporal/tbox_functions.cpp
    src/geo/stbox.cpp
    src/geo/stbox_functions.cpp
    src/geo/tgeompoint.cpp
    src/geo/tgeompoint_functions.cpp
    src/temporal/set.cpp
    src/temporal/span.cpp
    src/geo/geoset.cpp
    src/temporal/spanset.cpp
    src/geo/tgeometry.cpp
    src/geo/tgeometry_in_out.cpp
    src/index/rtree_module.cpp
    src/index/rtree_index_create_physical.cpp
    src/index/rtree_index_scan.cpp
    src/index/rtree_optimize_scan.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} "" ${EXTENSION_SOURCES})

# ----- SRID CSV autodetect -----
set(MOBILITYDUCK_SRID_CSV "" CACHE FILEPATH "Path to spatial_ref_sys.csv (optional override)")

function(_pick_first_existing OUT_VAR)
  foreach(_p IN LISTS ARGN)
    if(EXISTS "${_p}")
      set(${OUT_VAR} "${_p}" PARENT_SCOPE)
      return()
    endif()
  endforeach()
  set(${OUT_VAR} "" PARENT_SCOPE)
endfunction()

set(_md_triplets
  "${VCPKG_TARGET_TRIPLET}"
  x64-linux-release
  x64-linux-musl-release
  arm64-linux-release
  arm64-linux-musl-release
  x64-osx-release
  arm64-osx-release
  x64-windows-release
)

set(_srid_candidates)
if(MOBILITYDUCK_SRID_CSV)
  list(APPEND _srid_candidates "${MOBILITYDUCK_SRID_CSV}")
endif()

foreach(t ${_md_triplets})
  if(t)
    list(APPEND _srid_candidates
      "${CMAKE_BINARY_DIR}/vcpkg_installed/${t}/share/spatial_ref_sys.csv")
  endif()
endforeach()

if(DEFINED VCPKG_INSTALLED_DIR)
  foreach(t ${_md_triplets})
    if(t)
      list(APPEND _srid_candidates
        "${VCPKG_INSTALLED_DIR}/${t}/share/spatial_ref_sys.csv")
    endif()
  endforeach()
endif()

list(APPEND _srid_candidates
  "/usr/share/spatial_ref_sys.csv"
  "/usr/share/proj/spatial_ref_sys.csv"
  "/usr/local/share/spatial_ref_sys.csv"
  "/usr/local/share/proj/spatial_ref_sys.csv"
  "${CMAKE_SOURCE_DIR}/data/spatial_ref_sys.csv"
)

_pick_first_existing(_srid_csv ${_srid_candidates})

if(NOT _srid_csv)
  message(FATAL_ERROR "Could not locate spatial_ref_sys.csv")
endif()

message(STATUS "[mobilityduck] SRID CSV: ${_srid_csv}")

foreach(tgt IN ITEMS ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_compile_definitions(${tgt} PRIVATE
    MDUCK_DEFAULT_SRID_CSV="${_srid_csv}"
    MDUCK_SRID_ENV_NAME="SPATIAL_REF_SYS"
  )
endforeach()

if (TARGET json-c::json-c)
  set(JSONC_TGT json-c::json-c)
else()
  target_include_directories(${EXTENSION_NAME} PRIVATE ${JSON-C_INCLUDE_DIRS})
  target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${JSON-C_INCLUDE_DIRS})
  set(JSONC_TGT ${JSON-C_LIBRARIES})
endif()

target_link_libraries(${EXTENSION_NAME}
  MEOS::meos
  ${GEOS_TGT}
  PROJ::proj
  GSL::gsl
  GSL::gslcblas
  ${JSONC_TGT}
  OpenSSL::SSL
  OpenSSL::Crypto
)

target_link_libraries(${LOADABLE_EXTENSION_NAME}
  MEOS::meos
  ${GEOS_TGT}
  PROJ::proj
  GSL::gsl
  GSL::gslcblas
  ${JSONC_TGT}
  OpenSSL::SSL
  OpenSSL::Crypto
)

if (WIN32)  
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)

install(
  TARGETS ${LOADABLE_EXTENSION_NAME}
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
  RUNTIME DESTINATION "${INSTALL_LIB_DIR}"
)
