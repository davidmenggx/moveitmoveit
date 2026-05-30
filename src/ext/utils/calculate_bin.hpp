#pragma once

#include <bit>
#include <cstdint>

// Calculates the tightest bin size in the segregated free list whose block size
// is at least `capacity` bytes. Returns -1 when `capacity` exceeds the maximum
// bin size.
[[nodiscard]] inline int calculate_bin(uint64_t capacity,
                                       const int MIN_FREE_LIST_BIN_CAP,
                                       const int NUM_FREE_LIST_BINS) {
  if (capacity == 0) [[unlikely]]
    return 0;

  const int ceil_log{std::bit_width(capacity - 1)};

  const int bin{ceil_log - MIN_FREE_LIST_BIN_CAP};
  if (bin < 0)
    return 0;
  if (bin >= NUM_FREE_LIST_BINS)
    return -1;
  return bin;
}
