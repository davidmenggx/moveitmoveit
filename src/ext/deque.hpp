#pragma once

#include "utils/rng.hpp"
#include "utils/round_up_pow2.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

namespace moveitmoveit {
#if defined(__cpp_lib_hardware_interference_size)
inline constexpr std::size_t CACHE_LINE_SIZE =
    std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t CACHE_LINE_SIZE = 64;
#endif

enum class ObjectStatus : uint32_t { Ok, Empty, Abort };
// Metadata for deque objects to support type-agnostic-ness.
struct ObjectDescriptor {
  uint64_t offset_{0};
  uint32_t size_{0};
  uint32_t padding_{0};
  ObjectStatus status_{ObjectStatus::Ok};
};

// TODO: Consider a more performant implementation for single type.

// Deque state constants
inline constexpr int64_t MAX_BUFFERS = 64;
inline constexpr ObjectDescriptor EMPTY{0, 0, 0, ObjectStatus::Empty};
inline constexpr ObjectDescriptor ABORT{0, 0, 0, ObjectStatus::Abort};

// Structure and algorithms inspired by David Chase and Yossi Lev, 2005.
struct alignas(CACHE_LINE_SIZE) Buffer {
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> top_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> bottom_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> buffer_capacity_{
      0}; // Must be a power of 2
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> buffer_offset_{0};
};

// Memory allocation constants
inline constexpr int NULL_OFFSET{0};
inline constexpr int MIN_FREE_LIST_BIN_CAP{6}; // Buckets store 2^6 = 64 objects
inline constexpr int NUM_FREE_LIST_BINS{25};

// Metadata on the shared memory state.
struct Registry {
  std::atomic<int64_t> num_active_processes_{0};
  std::atomic<uint64_t> global_segement_size_{0};
  std::atomic<uint64_t> unallocated_memory_top_{
      round_up_pow2(sizeof(Registry))}; // Bump allocator

  struct alignas(8) TaggedOffset {
    uint32_t offset_;
    uint32_t aba_tag_;
  };
  static_assert(std::atomic<TaggedOffset>::is_always_lock_free,
                "TaggedOffset must be strictly lock-free");

  struct alignas(64) PaddedFreeList {
    std::atomic<TaggedOffset> head_{TaggedOffset{0, 0}};
  };

  // Segregated free list
  PaddedFreeList free_lists_[NUM_FREE_LIST_BINS];

  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> buffer_offsets_[MAX_BUFFERS];
  std::atomic<uint32_t> initialized_flag_{0};
};

// Constructor retry constants
inline constexpr int MAX_FTRUNCATE_TRIES{50};
inline constexpr int SLEEP_US{1'000};

class Deque {
public:
  // TODO: Consider how to resize the total memory block given. Right now we're
  // assigning 10GB (which Linux allocates lazily).
  Deque(std::string group_id, std::size_t default_size_mb = 10'000);
  ~Deque() noexcept;

  Deque(const Deque &other) = delete;
  Deque &operator=(const Deque &other) = delete;
  Deque(Deque &&other) = delete;
  Deque &operator=(Deque &&other) = delete;

  // Structure and algorithms inspired by David Chase and Yossi Lev, 2005.

  bool push(const char *serialized_data, std::size_t size) noexcept;

  // Never resizes the buffer, returns false if capacity is reached.
  bool try_push(const char *serialized_data, std::size_t size) noexcept;

  [[nodiscard]] ObjectDescriptor pop() noexcept;
  [[nodiscard]] ObjectDescriptor steal() noexcept;

  // TODO: Perhaps a targetted steal opereation that accepts a deque ID to try
  // and steal from.

private:
  // Helpers for accessing and writing raw ObjectDescriptors.
  [[nodiscard]] ObjectDescriptor write(const char *serialized_data,
                                       std::size_t size) noexcept;

  // Resizes the buffer up to a multiple times `current_capacity`.
  [[nodiscard]] bool grow(int64_t current_capacity) noexcept;

  // Returns the byte offset of a memory block large enough for `capacity`
  // bytes, or NULL_OFFSET (0) if out of memory or unsupported capacity.
  [[nodiscard]] uint64_t allocate(uint64_t capacity);

  // TODO: Shrink buffers down at low usage (requires more sophisticated
  // reclamation techniques without GC).

  // Pointer arithmetic helpers
  template <typename T>
  [[nodiscard]] inline T *offset_to_ptr(uint64_t offset) noexcept {
    if (offset == 0)
      return nullptr;
    // char * moves 1 byte
    return reinterpret_cast<T *>(static_cast<char *>(base_address_) + offset);
  }

  template <typename T>
  [[nodiscard]] inline uint64_t ptr_to_offset(T *ptr) noexcept {
    if (!ptr)
      return 0;
    return static_cast<uint64_t>(reinterpret_cast<char *>(ptr) -
                                 static_cast<char *>(base_address_));
  }

  void cleanup() noexcept;

  std::string shm_name_{};
  int shm_fd_{-1};
  std::size_t segment_size_{0}; // Bytes
  void *base_address_{nullptr};

  // Process local offsets
  Registry *registry_{nullptr};
  Buffer *my_buffer_{nullptr};
  int64_t queue_idx_{-1};
  bool is_creator_{false};

  FastRNG rng_{};
};
} // namespace moveitmoveit
