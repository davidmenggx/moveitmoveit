#pragma once

#include <cstdint>

// Calculates the tightest bin size in the segregated free list whose block size
// is at least `capacity` bytes. Returns -1 when `capacity` exceeds the maximum
// bin size.
[[nodiscard]] inline int
calculate_bin(uint64_t capacity, const int SMALLEST_BIN, const int NUM_BINS) {
  if (capacity == 0) [[unlikely]]
    return 0;

#if defined(__GNUC__) || defined(__clang__)
  const int floor_log =
      63 - __builtin_clzll(static_cast<unsigned long long>(capacity));

#elif defined(_MSC_VER) && (defined(_WIN64) || defined(_M_ARM64))
  unsigned long floor_log_ul{0};
  _BitScanReverse64(&floor_log_ul, static_cast<unsigned __int64>(capacity));
  const int floor_log{static_cast<int>(floor_log_ul)};

#else
  int floor_log{0};
  for (uint64_t v{capacity >> 1}; v != 0; v >>= 1)
    ++floor_log;
#endif

  const bool is_pow2{(capacity & (capacity - 1)) == 0};
  const int ceil_log{floor_log + (is_pow2 ? 0 : 1)};

  const int bin{ceil_log - SMALLEST_BIN};
  if (bin < 0)
    return 0;
  if (bin >= NUM_BINS)
    return -1;
  return bin;
}
