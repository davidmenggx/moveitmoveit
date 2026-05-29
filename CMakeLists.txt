cmake_minimum_required(VERSION 3.18...3.27)
project(moveitmoveit LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(nanobind CONFIG REQUIRED)

add_subdirectory(src/ext)

if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(tests)
endif()

