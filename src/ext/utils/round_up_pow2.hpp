#pragma once

#include <bit>
#include <concepts>
#include <limits>

template <std::unsigned_integral T> constexpr T round_up_pow2(T val) noexcept {
  if (val <= 1)
    return 1;

  return T(1) << (std::numeric_limits<T>::digits - std::countl_zero(val - 1));
}
