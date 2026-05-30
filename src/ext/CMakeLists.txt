nanobind_add_module(moveitmoveit_ext
  utils/calculate_bin.hpp
  utils/rng.hpp
  utils/valid_shm_name.hpp
  bindings.cpp
  deque.hpp
  deque.cpp
)

install(TARGETS moveitmoveit_ext DESTINATION moveitmoveit)

