# HudiConfig.cmake - Config file for Hudi C++ library
# Exports:
#  Hudi::Hudi - Imported target for the Hudi library

@PACKAGE_INIT@

# Define the library path
set(HUDI_LIBRARY_PATH "@CMAKE_INSTALL_PREFIX@/@CMAKE_INSTALL_LIBDIR@/libhudi@CMAKE_SHARED_LIBRARY_SUFFIX@")

# Include the exported targets file
include("${CMAKE_CURRENT_LIST_DIR}/HudiTargets.cmake")

# Check if the imported target exists
if(NOT TARGET Hudi::Hudi)
  message(FATAL_ERROR "Hudi::Hudi target not found in HudiTargets.cmake")
endif()

# Get the include directories
get_target_property(HUDI_INCLUDE_DIRS Hudi::Hudi INTERFACE_INCLUDE_DIRECTORIES)

# Set other required variables
set(HUDI_LIBRARIES Hudi::Hudi)
set(HUDI_LIBRARY ${HUDI_LIBRARY_PATH})
set(HUDI_FOUND TRUE)
set(Hudi_FOUND TRUE)
set(HUDI_VERSION "@PROJECT_VERSION@")
set(Hudi_VERSION "@PROJECT_VERSION@")

# Check all required components
check_required_components(Hudi)