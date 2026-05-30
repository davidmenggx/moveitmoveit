nanobind_add_module(moveitmoveit_ext
  utils/calculate_bin.hpp
  utils/rng.hpp
  utils/valid_shm_name.hpp
  bindings.cpp
  deque.hpp
  deque.cpp
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

install(TARGETS moveitmoveit_ext DESTINATION moveitmoveit)

