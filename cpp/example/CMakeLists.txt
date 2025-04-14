cmake_minimum_required(VERSION 3.15)
project(hudi_cpp_examples)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the installed hudi library
find_library(HUDI_LIBRARY hudi REQUIRED)

# Set the include directories for Hudi
set(HUDI_INCLUDE_DIR "/usr/local/include")

# Add the example executable
add_executable(hudi_cpp_example
  read_file_slice.cpp
  ${HUDI_INCLUDE_DIR}/hudi/src/lib.rs.cc
)

# Setup include directories
target_include_directories(hudi_cpp_example PRIVATE
  ${HUDI_INCLUDE_DIR}/hudi
  ${HUDI_INCLUDE_DIR}/rust
)

# Link against libraries
target_link_libraries(hudi_cpp_example
  ${HUDI_LIBRARY}
)

# Output configuration
message(STATUS "Using HUDI_LIBRARY: ${HUDI_LIBRARY}")