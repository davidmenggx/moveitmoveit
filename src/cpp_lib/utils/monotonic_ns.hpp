#pragma once

#include <cstdint>
#include <ctime>

// Granularity a bit off but it's ok for a basic second-level process check.
[[nodiscard]] inline uint64_t monotonic_ns() noexcept {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(ts.tv_nsec);
}
