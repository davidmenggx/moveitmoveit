nanobind_add_module(moveitmoveit_ext
  bindings.cpp
  deque.cpp
)

install(TARGETS moveitmoveit_ext DESTINATION moveitmoveit)

