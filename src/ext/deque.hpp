#pragma once

#include "utils/rng.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace moveitmoveit {
inline constexpr std::size_t CACHE_LINE_SIZE = 64;

// Note: We only support 64 concurrent processes.
inline constexpr int64_t MAX_BUFFERS = 64;

enum class ObjectStatus : uint32_t { Ok, Empty, Abort };
// Metadata for deque objects to support type-agnostic-ness.
struct alignas(16) ObjectDescriptor {
  uint64_t offset_{0};
  uint32_t size_{0}; // Maximum object size is u32 max, ~4GB.
  ObjectStatus status_{ObjectStatus::Ok};

  bool operator==(const ObjectDescriptor &other) const noexcept = default;
};

inline constexpr ObjectDescriptor EMPTY{
    .offset_ = 0, .size_ = 0, .status_ = ObjectStatus::Empty};
inline constexpr ObjectDescriptor ABORT{
    .offset_ = 0, .size_ = 0, .status_ = ObjectStatus::Abort};

static_assert(std::atomic<ObjectDescriptor>::is_always_lock_free,
              "CRITICAL: ObjectDescriptor (128 bits) must be lock free");

// TODO: Consider a more performant implementation for single type, such as
// inlining the ObjectDescriptors.

// Metadata for where the ring buffer is located in shared memory.
struct alignas(16) RingDescriptor {
  uint64_t offset_{0};
  uint64_t capacity_{0};

  bool operator==(const RingDescriptor &other) const noexcept = default;
};

static_assert(std::atomic<RingDescriptor>::is_always_lock_free,
              "CRITICAL: RingDescriptor (128 bits) must be lock free");

// Structure and algorithms inspired by David Chase and Yossi Lev, 2005.
struct alignas(CACHE_LINE_SIZE) Buffer {
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> top_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> bottom_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<RingDescriptor> descriptor_{};
};

// Memory allocation constants
inline constexpr uint64_t NULL_OFFSET{0};
inline constexpr int MIN_FREE_LIST_BIN_CAP{6}; // Buckets store 2^6 = 64 objects
inline constexpr int NUM_FREE_LIST_BINS{25};

// Metadata to prevent race conditions in the segregated free list.
struct alignas(16) FreeListHead {
  uint64_t offset_{};
  uint64_t tag_{};

  bool operator==(const FreeListHead &other) const noexcept = default;
};

static_assert(std::atomic<FreeListHead>::is_always_lock_free,
              "CRITICAL: FreeListHead (128 bits) must be lock free");

// Metadata on the shared memory state.
struct Registry {
  std::atomic<int64_t> num_active_processes_{0};
  std::atomic<uint64_t> live_mask_{0};
  std::atomic<uint64_t> global_segement_size_{0};

  // For the bump allocator
  std::atomic<uint64_t> unallocated_memory_top_{
      (sizeof(Registry) + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1)};

  struct alignas(CACHE_LINE_SIZE) PaddedFreeList {
    std::atomic<FreeListHead> head_{{.offset_ = NULL_OFFSET, .tag_ = 0}};
  };

  // Segregated free list
  PaddedFreeList free_lists_[NUM_FREE_LIST_BINS];

  struct alignas(CACHE_LINE_SIZE) PaddedOffset {
    std::atomic<uint64_t> value_{NULL_OFFSET};
  };

  PaddedOffset buffer_offsets_[MAX_BUFFERS];

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
  Deque(std::string group_id, std::size_t default_size_mb = 16'384);
  ~Deque() noexcept;

  Deque(const Deque &other) = delete;
  Deque &operator=(const Deque &other) = delete;
  Deque(Deque &&other) = delete;
  Deque &operator=(Deque &&other) = delete;

  // Structure and algorithms inspired by David Chase and Yossi Lev, 2005.

  // If the current buffer is full, try resize. Throws `bad_alloc` on failure.
  void put(const char *serialized_data, std::size_t size);

  // Never resizes the buffer, returns false if capacity is reached.
  [[nodiscard]] bool try_put(const char *serialized_data, std::size_t size);

  [[nodiscard]] ObjectDescriptor get();
  // TODO: Implement the optional params
  [[nodiscard]] ObjectDescriptor
      steal(/*int64_t target_victim, bool target_longest*/);

  // Results unreliable
  [[nodiscard]] std::size_t qsize() const;
  [[nodiscard]] bool empty() const;
  [[nodiscard]] bool full() const;

  // CRITICAL IMPORTANT: This should be called after the returned Python object
  // is destructed. This must be called on the same `Deque` instance which it
  // was retrieved from. This is not thread safe. This must not be called on the
  // same `ObjectDescriptor` twice.
  void release(ObjectDescriptor &desc) noexcept;

private:
  // --- Deque state helpers ---
  [[nodiscard]] bool grow(uint64_t current_capacity);

  // TODO: Shrink buffers at low usage

  void cleanup() noexcept;

  // --- Data access helpers ---
  [[nodiscard]] ObjectDescriptor write(const char *serialized_data,
                                       std::size_t size);
  [[nodiscard]] const char *
  payload_ptr(const ObjectDescriptor &d) const noexcept;

  // --- Memory management helpers ---
  // Returns the byte offset of a memory block large enough for `capacity`
  // bytes, or NULL_OFFSET (0) if out of memory or unsupported capacity.
  [[nodiscard]] uint64_t allocate(uint64_t capacity);

  void free(uint64_t offset, uint64_t capacity);

  // --- Pointer arithmetic helpers ---
  template <typename T>
  [[nodiscard]] inline T *offset_to_ptr(uint64_t offset) const noexcept {
    if (offset == 0)
      return nullptr;
    return reinterpret_cast<T *>(static_cast<char *>(base_address_) +
                                 offset); // char * moves one byte
  }

  template <typename T>
  [[nodiscard]] inline uint64_t ptr_to_offset(T *ptr) const noexcept {
    if (!ptr)
      return 0;
    return static_cast<uint64_t>(reinterpret_cast<char *>(ptr) -
                                 static_cast<char *>(base_address_));
  }

  // --- Shared memory state ---
  std::string shm_name_{};
  int shm_fd_{-1};
  std::size_t segment_size_{0};
  void *base_address_{nullptr};

  // --- Process local offsets ---
  Registry *registry_{nullptr};
  Buffer *my_buffer_{nullptr};
  int64_t queue_idx_{-1};
  bool is_creator_{false};
  bool process_count_incremented_{false};

  FastRNG rng_{};

  // --- Epoch based reclamation state ---
  static constexpr int EBR_EPOCHS = 3;
  struct RetiredBuffer {
    uint64_t offset_;
    uint64_t size_bytes_;
  };

  // TODO: There is a memory leak here when a process drops out, as its vector
  // and array are cleared but the underlying buffer is lost. We should replace
  // this heap-local array<vector> to shared memory.
  std::array<std::vector<RetiredBuffer>, EBR_EPOCHS> retire_lists_{};
  static constexpr int EBR_CYCLE_FORCE_ADVANCE = 128;
  int ebr_cycles_since_advance_{0};

  // --- Epoch based reclamation helpers ---

  // Call prior to entering the critical section.
  void ebr_pin() noexcept;

  // Call upon leaving the critical section so the application can progress.
  void ebr_unpin() noexcept;

  void ebr_retire(uint64_t offset, uint64_t size_bytes) noexcept;

  // Returns `false` and fails if a process is in an older epoch.
  bool ebr_try_advance() noexcept;
  void ebr_reclaim() noexcept;
};
} // namespace moveitmoveit
