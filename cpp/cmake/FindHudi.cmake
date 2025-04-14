# FindHudi.cmake - Find Apache Hudi C++ library
# Defines:
#  HUDI_FOUND - Found the Hudi library
#  HUDI_INCLUDE_DIRS - Include directories
#  HUDI_LIBRARIES - Libraries to link against
#  HUDI_VERSION - Version of the library

# Find package config first
find_package(Hudi CONFIG QUIET)
if(Hudi_FOUND)
  # We found a package config file, use it
  set(HUDI_FOUND ${Hudi_FOUND})
  set(HUDI_INCLUDE_DIRS ${Hudi_INCLUDE_DIRS})
  set(HUDI_LIBRARIES ${Hudi_LIBRARIES})
  set(HUDI_VERSION ${Hudi_VERSION})
  return()
endif()

# Find the library - search common locations
find_library(HUDI_LIBRARY
  NAMES hudi
  PATHS
    /usr/local/lib
    /usr/lib
    /usr/lib64
    $ENV{HUDI_HOME}/lib
  DOC "The Hudi library"
)

# Find include directories
find_path(HUDI_INCLUDE_DIR
  NAMES hudi/hudi.h
  PATHS
    /usr/local/include
    /usr/include
    $ENV{HUDI_HOME}/include
  DOC "The Hudi include directory"
)

# Find the generated cxx bridge headers directory
find_path(HUDI_CXX_BRIDGE_DIR
  NAMES src/lib.rs.h
  PATHS
    /usr/local/include
    /usr/include
    ${HUDI_INCLUDE_DIR}
    $ENV{HUDI_HOME}/include
  DOC "The Hudi cxx-rs bridge header directory"
)

# Get version from the library
if(HUDI_INCLUDE_DIR)
  # Try to extract version from Cargo.toml
  file(STRINGS "${HUDI_INCLUDE_DIR}/../Cargo.toml" HUDI_VERSION_LINE
       REGEX "^version[ \t]*=[ \t]*\"[0-9]+\\.[0-9]+\\.[0-9]+\"$")
  if(HUDI_VERSION_LINE)
    string(REGEX REPLACE "^version[ \t]*=[ \t]*\"([0-9]+\\.[0-9]+\\.[0-9]+)\"$" "\\1"
           HUDI_VERSION "${HUDI_VERSION_LINE}")
  else()
    # Default version if we couldn't extract it
    set(HUDI_VERSION "0.4.0")
  endif()
endif()

# Set results
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Hudi
  REQUIRED_VARS HUDI_LIBRARY HUDI_INCLUDE_DIR HUDI_CXX_BRIDGE_DIR
  VERSION_VAR HUDI_VERSION
)

if(HUDI_FOUND)
  set(HUDI_LIBRARIES ${HUDI_LIBRARY})
  set(HUDI_INCLUDE_DIRS ${HUDI_INCLUDE_DIR} ${HUDI_CXX_BRIDGE_DIR})

  # Create imported target
  if(NOT TARGET Hudi::Hudi)
    add_library(Hudi::Hudi INTERFACE IMPORTED)
    set_target_properties(Hudi::Hudi PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${HUDI_INCLUDE_DIRS}"
    )

    # Create imported implementation library for linkage
    add_library(Hudi::HudiLib SHARED IMPORTED)
    set_target_properties(Hudi::HudiLib PROPERTIES
      IMPORTED_LOCATION "${HUDI_LIBRARY}"
    )

    # Make Hudi::Hudi depend on the actual library
    set_target_properties(Hudi::Hudi PROPERTIES
      INTERFACE_LINK_LIBRARIES Hudi::HudiLib
    )
  endif()
endif()

mark_as_advanced(
  HUDI_INCLUDE_DIR
  HUDI_LIBRARY
  HUDI_CXX_BRIDGE_DIR
)