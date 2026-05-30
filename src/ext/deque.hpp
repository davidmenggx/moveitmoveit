#pragma once

#include "utils/rng.hpp"
#include "utils/round_up_pow2.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace moveitmoveit {
inline constexpr std::size_t CACHE_LINE_SIZE = 64;

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
// NOTE: We currently only support 64 total queue lifetimes (NOT concurrent
// connections), perhaps re-use the queue indices in the Registry.

inline constexpr ObjectDescriptor EMPTY{0, 0, 0, ObjectStatus::Empty};
inline constexpr ObjectDescriptor ABORT{0, 0, 0, ObjectStatus::Abort};

// Structure and algorithms inspired by David Chase and Yossi Lev, 2005.
struct alignas(CACHE_LINE_SIZE) Buffer {
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> top_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> bottom_{0};

  // TODO: Is there a race condition in the separate atomic reads of the buffer
  // capacity and offset? I could fix this with an atomic struct wrapper, but
  // make sure that it is always lock free.
  // As of right now we are silently truncating for memory greater than 4 GB.
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> buffer_capacity_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> buffer_offset_{0};
};

// Memory allocation constants
inline constexpr uint64_t NULL_OFFSET{0};
inline constexpr int MIN_FREE_LIST_BIN_CAP{6}; // Buckets store 2^6 = 64 objects
inline constexpr int NUM_FREE_LIST_BINS{25};

// Metadata on the shared memory state.
struct Registry {
  std::atomic<int64_t> num_active_processes_{0};
  std::atomic<int64_t> next_queue_idx_{0};
  std::atomic<uint64_t> live_mask_{0};
  std::atomic<uint64_t> global_segement_size_{0};

  std::atomic<uint64_t> unallocated_memory_top_{
      round_up_pow2(sizeof(Registry))}; // Bump allocator

  // TODO: the range of uint32_t limits the size of the memory segment we can
  // allocate. Check if two uint64_t's is atomic or do something like a double
  // CAS, and fall back to the uint32_t offset if not possible.
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

  // Epoch based reclamation state
  static constexpr uint64_t EBR_UNPINNED = std::numeric_limits<uint64_t>::max();
  std::atomic<uint64_t> global_epoch_{0};

  struct alignas(CACHE_LINE_SIZE) PinnedEpoch {
    std::atomic<uint64_t> value_{EBR_UNPINNED};
  };
  PinnedEpoch pinned_epochs_[MAX_BUFFERS];
};

// Constructor retry constants
inline constexpr int MAX_FTRUNCATE_RETRIES{1'000};
inline constexpr int MAX_REGISTRY_RETRIES{1'000};
inline constexpr int SLEEP_US{1'000};

class Deque {
public:
  // TODO: Consider how to resize the total memory block given. Right now we're
  // assigning 3GB (which Linux allocates lazily).
  Deque(std::string group_id, std::size_t default_size_mb = 3'000);
  ~Deque() noexcept;

  Deque(const Deque &other) = delete;
  Deque &operator=(const Deque &other) = delete;
  Deque(Deque &&other) = delete;
  Deque &operator=(Deque &&other) = delete;

  // Structure and algorithms inspired by David Chase and Yossi Lev, 2005.

  bool put(const char *serialized_data, std::size_t size);

  // Never resizes the buffer, returns false if capacity is reached.
  bool try_put(const char *serialized_data, std::size_t size);

  [[nodiscard]] ObjectDescriptor get();
  [[nodiscard]] ObjectDescriptor steal();

  // TODO: Perhaps a targetted steal opereation that accepts a deque ID to try
  // and steal from.

  [[nodiscard]] std::size_t qsize();
  [[nodiscard]] bool empty();
  [[nodiscard]] bool full();

private:
  // --- Deque state helpers ---

  // Resizes the buffer up to a multiple times `current_capacity`.
  [[nodiscard]] bool grow(uint64_t current_capacity);

  // TODO: Shrink buffers down at low usage (requires more sophisticated
  // reclamation techniques without GC).

  void cleanup();

  // --- Data access helpers ---
  [[nodiscard]] ObjectDescriptor write(const char *serialized_data,
                                       std::size_t size);

  // --- Memory management helpers ---

  // Returns the byte offset of a memory block large enough for `capacity`
  // bytes, or NULL_OFFSET (0) if out of memory or unsupported capacity.
  [[nodiscard]] uint64_t allocate(uint64_t capacity);

  // IMPORTANT: For the individual items removed (not the buffers) via `get` or
  // `steal`, `free` needs to be called safely after the memory is read. Perhaps
  // I'll use a Python `memoryview` and only call `free` in the binding. For now
  // we are not calling `free` in `get` or `steal`.
  void free(uint64_t offset, uint64_t capacity);

  // --- Pointer arithmetic helpers ---
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

  // --- Shared memory state ---
  std::string shm_name_{};
  int shm_fd_{-1};
  std::size_t segment_size_{0}; // Bytes
  void *base_address_{nullptr};

  // --- Process local offsets ---
  Registry *registry_{nullptr};
  Buffer *my_buffer_{nullptr};
  int64_t queue_idx_{-1};
  bool is_creator_{false};

  FastRNG rng_{};

  // --- Epoch based reclamation state ---
  static constexpr int EBR_EPOCHS = 3;
  struct RetiredBuffer {
    uint64_t offset_;
    uint64_t size_bytes_;
  };
  std::array<std::vector<RetiredBuffer>, EBR_EPOCHS> retire_lists_{};
  static constexpr int EBR_CYCLE_FORCE_ADVANCE = 128;
  int ebr_cycles_since_advance_{0};

  // --- Epoch based reclamation helpers ---
  void ebr_pin();
  void ebr_unpin();
  void ebr_retire(uint64_t offset, uint64_t size_bytes);
  bool ebr_try_advance();
  void ebr_reclaim();
};
} // namespace moveitmoveit
