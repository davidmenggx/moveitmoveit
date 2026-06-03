#pragma once

#include <chrono>
#include <concepts>
#include <cstdint>
#include <type_traits>

class FastRNG {
private:
  uint64_t state_;

  constexpr uint64_t next64() noexcept {
    uint64_t z = (state_ += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }

public:
  FastRNG() noexcept { seed(); }

  constexpr void seed(uint64_t s) noexcept {
    state_ = s;
    next64();
  }

  void seed() noexcept {
    uint64_t time_seed = static_cast<uint64_t>(
        std::chrono::high_resolution_clock::now().time_since_epoch().count());
    uint64_t addr_seed = reinterpret_cast<uint64_t>(this);
    seed(time_seed ^ addr_seed);
  }

  template <std::integral T> constexpr T random(T min, T max) noexcept {
    if (min >= max)
      return min;

    using UnsignedT = std::make_unsigned_t<T>;
    UnsignedT range = static_cast<UnsignedT>(max - min) + 1;

    if constexpr (sizeof(T) <= 4) {
      uint64_t random32 = static_cast<uint32_t>(next64());
      uint64_t multiresult = random32 * range;
      uint32_t leftover = static_cast<uint32_t>(multiresult);

      if (leftover < range) {
        uint32_t threshold = -range % range;
        while (leftover < threshold) {
          random32 = static_cast<uint32_t>(next64());
          multiresult = random32 * range;
          leftover = static_cast<uint32_t>(multiresult);
        }
      }
      return min + static_cast<T>(multiresult >> 32);
    } else {
      UnsignedT threshold = -range % range;
      while (true) {
        UnsignedT r = static_cast<UnsignedT>(next64());
        if (r >= threshold)
          return min + static_cast<T>(r % range);
      }
    }
  }
};
