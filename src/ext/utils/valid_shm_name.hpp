#pragma once

#include <string>

[[nodiscard]] inline std::string valid_shm_name(const std::string &input) {
  std::string valid_name = "/";

  for (char c : input) {
    if (c == '/')
      continue;

    if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' ||
        c == '.')
      valid_name += c;
  }

  if (valid_name == "/") {
    valid_name = "/moveitmoveit";
  }

  const size_t MAX_SHM_LEN = 254;
  if (valid_name.length() > MAX_SHM_LEN)
    valid_name = valid_name.substr(0, MAX_SHM_LEN);

  return valid_name;
}
