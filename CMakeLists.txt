cmake_minimum_required(VERSION 3.9)

include(CheckIncludeFiles)
include(CheckSymbolExists)
include(CheckCSourceCompiles)

file(READ "config.h.in" AUTOMAKE_CONFIG_H_IN)
string(REPLACE "#undef VERSION" "#cmakedefine VERSION \"@VERSION@\"" CMAKE_CONFIG_H_IN
  "${AUTOMAKE_CONFIG_H_IN}")
string(REPLACE "#undef TARGET_CPU" "#cmakedefine TARGET_CPU \"@TARGET_CPU@\"" CMAKE_CONFIG_H_IN
  "${CMAKE_CONFIG_H_IN}")
string(REPLACE "#undef _FILE_OFFSET_BITS" "#cmakedefine _FILE_OFFSET_BITS @_FILE_OFFSET_BITS@" CMAKE_CONFIG_H_IN
  "${CMAKE_CONFIG_H_IN}")
string(CONCAT CMAKE_CONFIG_H_IN "${CMAKE_CONFIG_H_IN}" "#cmakedefine DISABLE_LOADABLE_EXTENSION 1")
string(REPLACE  "#undef" "#cmakedefine" CMAKE_CONFIG_H_IN
  "${CMAKE_CONFIG_H_IN}")
string(REGEX REPLACE  "([A-Z0-9])\n" "\\1 1\n" CMAKE_CONFIG_H_IN
  "${CMAKE_CONFIG_H_IN}")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/config.h.in" "${CMAKE_CONFIG_H_IN}")

if (CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(spatialite LANGUAGES C)
endif()

set(VERSION "4.5.0-devel")
set(TARGET_CPU "${CMAKE_SYSTEM_PROCESSOR}")
set(GEOS_ADVANCED 1)
set(GEOS_REENTRANT 1)
set(OMIT_EPSG 1)
set(OMIT_FREEXL 1)
set(OMIT_ICONV 1)
set(OMIT_KNN 1)
set(OMIT_PROJ 1)

if (NOT SQLITE_INCLUDE_DIR)
  find_path(SQLITE3_INCLUDE_DIR NAMES sqlite3.h)
  if (NOT SQLITE3_INCLUDE_DIR)
    message(FATAL_ERROR "Can not find sqlite3.h")
  endif ()
  include_directories(${SQLITE3_INCLUDE_DIR})
  set(HAVE_SQLITE3_H 1)
endif ()

if (NOT SQLITE3_LIBRARY)
  find_library(SQLITE3_LIBRARY NAMES sqlite)
  if (NOT SQLITE3_LIBRARY)
    find_library(SQLITE3_LIBRARY NAMES sqlite3)
      if (NOT SQLITE3_LIBRARY)
	message(FATAL_ERROR "Can not find sqlite library")
      endif ()
  endif ()
  set(HAVE_LIBSQLITE3 1)
endif ()

if (NOT GEOS_C_INCLUDE_DIR)
  find_path(GEOS_C_INCLUDE_DIR NAMES geos_c.h)
  if (NOT GEOS_C_INCLUDE_DIR)
    message(FATAL_ERROR "Can not find geos_c.h")
  endif ()
  set(HAVE_GEOS_C_H 1)
endif ()

include_directories(${GEOS_C_INCLUDE_DIR})

if (NOT GEOS_C_LIBRARY)
  find_library(GEOS_C_LIBRARY NAMES geos_c)
  if (NOT GEOS_C_LIBRARY)
    message(FATAL_ERROR "Can not find geos_c library")
  endif ()
endif ()

check_symbol_exists(SQLITE_INDEX_CONSTRAINT_LIKE sqlite3.h HAVE_DECL_SQLITE_INDEX_CONSTRAINT_LIKE)
check_include_files(dlfcn.h HAVE_DLFCN_H)
check_include_files(fcntl.h HAVE_FCNTL_H)
check_symbol_exists(fdatasync unistd.h HAVE_FDATASYNC)
check_include_files(float.h HAVE_FLOAT_H)
check_symbol_exists(ftruncate unistd.h HAVE_FTRUNCATE)
check_symbol_exists(getcwd unistd.h HAVE_GETCWD)
check_symbol_exists(gettimeofday sys/time.h HAVE_GETTIMEOFDAY)
check_include_files(inttypes.h HAVE_INTTYPES_H)
check_symbol_exists(localtime_r time.h HAVE_LOCALTIME_R)
check_include_files(math.h HAVE_MATH_H)
check_symbol_exists(memmove string.h HAVE_MEMMOVE)
check_include_files(memory.h HAVE_MEMORY_H)
check_symbol_exists(memset string.h HAVE_MEMSET)
check_include_files(sqlite3ext.h HAVE_SQLITE3EXT_H)
check_symbol_exists(sqrt math.h HAVE_SQRT)
check_include_files(stddef.h  HAVE_STDDEF_H)
check_include_files(stdint.h HAVE_STDINT_H)
check_include_files(stdio.h HAVE_STDIO_H)
check_include_files(stdlib.h HAVE_STDLIB_H)
check_symbol_exists(strcasecmp strings.h HAVE_STRCASECMP)
check_symbol_exists(strerror string.h HAVE_STRERROR)
check_symbol_exists(strftime time.h HAVE_STRFTIME)
check_include_files(strings.h HAVE_STRINGS_H)
check_include_files(string.h HAVE_STRING_H)
check_symbol_exists(strncasecmp strings.h HAVE_STRNCASECMP)
check_symbol_exists(strstr string.h HAVE_STRSTR)
check_include_files(sys/stat.h HAVE_SYS_STAT_H)
check_include_files(sys/time.h HAVE_SYS_TIME_H)
check_include_files(sys/types.h HAVE_SYS_TYPES_H)
check_include_files(unistd.h HAVE_UNISTD_H)
check_include_files(zlib.h HAVE_ZLIB_H)
check_c_source_compiles("\
#include <sys/time.h>\n \
#include <time.h>\n \
int main() { return 0; }\n \
" TIME_WITH_SYS_TIME)
check_c_source_compiles("\
#include <sys/time.h>\n \
int main() { struct tm x; return 0; }\n \
" TM_IN_SYS_TIME)
set(_FILE_OFFSET_BITS 64)
set(_LARGEFILE_SOURCE 1)
set(_LARGE_FILE 1)
check_symbol_exists(off_t sys/types.h HAVE_OFF_T)
if (NOT HAVE_OFF_T)
  set(HAVE_OFF_T "long int")
endif ()
check_symbol_exists(size_t stddef.h HAVE_SIZE_T)
if (NOT HAVE_SIZE_T)
  set(HAVE_SIZE_T "unsigned int")
endif ()
set(DISABLE_LOADABLE_EXTENSION 1)
configure_file("${CMAKE_CURRENT_BINARY_DIR}/config.h.in" "${CMAKE_CURRENT_BINARY_DIR}/config.h")

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/headers)

file(GLOB spatialite_MATCH_SRC
  "src/spatialite/*.h"
  "src/spatialite/*.c"
  "src/gaiaaux/*.c"
  "src/gaiaaux/*.h"
  "src/md5/*.h"
  "src/md5/*.c"
  "src/gaiaexif/*.c"
  "src/gaiaexif/*.h"
  "src/dxf/*.h"
  "src/dxf/*.c"
  "src/shapefiles/*.c"
  "src/shapefiles/*.h"
  "src/connection_cache/*.h"
  "src/connection_cache/*.c"
  "src/srsinit/*.h"
  "src/srsinit/*.c"
  "src/stored_procedures/*.c"
  "src/stored_procedures/*.h"
  "src/cutter/*.c"
  "src/cutter/*.h"
  )

add_library(spatialite STATIC
  ${spatialite_MATCH_SRC}
  src/gaiageo/gg_advanced.c
  src/gaiageo/gg_endian.c
  src/gaiageo/gg_geodesic.c
  src/gaiageo/gg_geometries.c
  src/gaiageo/gg_geoscvt.c
  src/gaiageo/gg_relations.c
  src/gaiageo/gg_relations_ext.c
  src/gaiageo/gg_extras.c
  src/gaiageo/gg_shape.c
  src/gaiageo/gg_transform.c
  src/gaiageo/gg_wkb.c
  src/gaiageo/gg_wkt.c
  src/gaiageo/gg_vanuatu.c
  src/gaiageo/gg_ewkt.c
  src/gaiageo/gg_geoJSON.c
  src/gaiageo/gg_kml.c
  src/gaiageo/gg_gml.c
  src/gaiageo/gg_voronoj.c
  src/gaiageo/gg_xml.c
  src/gaiageo/gg_matrix.c
  src/versioninfo/version.c
  )
target_link_libraries(spatialite ${SQLITE3_LIBRARY} ${GEOS_C_LIBRARY})
install(TARGETS spatialite DESTINATION ".")

option(SPATIALITE_ENABLE_TESTS
  "Set to OFF|ON (default) to control build of spatialite tests" OFF)

if (SPATIALITE_ENABLE_TESTS)
  enable_testing()
  add_subdirectory(test)
endif ()
