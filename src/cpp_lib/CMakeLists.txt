nanobind_add_module(moveitmoveit_ext
  utils/calculate_bin.hpp
  utils/rng.hpp
  utils/valid_shm_name.hpp
  bindings.cpp
  deque.hpp
  deque.cpp
)

target_include_directories(moveitmoveit_ext SYSTEM PRIVATE ${nanobind_INCLUDE_DIR} ${Python_INCLUDE_DIRS})

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(moveitmoveit_ext PRIVATE -std=c++20 -Wall -Wextra -Wpedantic -Werror -march=native)
endif()

install(TARGETS moveitmoveit_ext DESTINATION moveitmoveit)

add_custom_command(
    TARGET moveitmoveit_ext POST_BUILD
    COMMAND ${Python_EXECUTABLE} -m nanobind.stubgen 
            -m moveitmoveit_ext 
            -O ${CMAKE_SOURCE_DIR}/src/moveitmoveit
            COMMENT "Generating Python stub file (.pyi)"
)

