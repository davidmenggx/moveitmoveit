add_test(
  NAME pytest_suite
  COMMAND "${Python_EXECUTABLE}" -m pytest "${CMAKE_CURRENT_SOURCE_DIR}"
)

set_tests_properties(pytest_suite PROPERTIES 
  ENVIRONMENT "PYTHONPATH=$<TARGET_FILE_DIR:moveitmoveit_ext>"
)

