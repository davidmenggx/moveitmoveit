#pragma once

#include "utils/rng.hpp"

#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "CRITICAL: std::atomic<uint64_t> must be lock-free");
static_assert(std::atomic<int64_t>::is_always_lock_free,
              "CRITICAL: std::atomic<int64_t> must be lock-free");

// TODO: If 128 bits are atomic (e.g. __uint128_t), we should prefer that
// instead of the current bit packing tricks.

namespace moveitmoveit {
inline constexpr std::size_t CACHE_LINE_SIZE = 64;

// Note: Due to the need for atomic operations, we pack metadata into 64 bits.
// This limits the amount of addressable shared memory to 1 TB.
inline constexpr uint64_t SYSTEM_OFFSET_MASK = 0x000000FFFFFFFFFFULL;

enum class ObjectStatus : uint32_t { Ok, Empty, Abort };

// Metadata for deque objects to support type-agnostic-ness.
struct ObjectDescriptor {
  uint64_t offset_{0};
  uint32_t size_{0}; // Maximum object size is u32 max, ~4GB.
  ObjectStatus status_{ObjectStatus::Ok};
};

// TODO: Consider a more performant implementation for single type, such as
// inlining the ObjectDescriptors.

struct alignas(8) RingSlot {
  std::atomic<uint64_t> data_{0};

  void store(const ObjectDescriptor &d, std::memory_order mo) noexcept {
    const uint64_t safe_offset{d.offset_ & SYSTEM_OFFSET_MASK};
    const uint64_t safe_status{static_cast<uint64_t>(d.status_) & 0x3ULL};

    data_.store((safe_offset << 2) | safe_status, mo);
  }

  [[nodiscard]] ObjectDescriptor load(std::memory_order mo) const noexcept {
    const uint64_t packed{data_.load(mo)};

    return ObjectDescriptor{.offset_ = (packed >> 2) & SYSTEM_OFFSET_MASK,
                            .size_ =
                                0, // Note: populate later with recover_size()
                            .status_ = static_cast<ObjectStatus>(packed & 0x3)};
  }
};

static_assert(sizeof(RingSlot) == 8);

// Deque state constants
// Note: We only support 64 concurrent processes.
inline constexpr int64_t MAX_BUFFERS = 64;

inline constexpr ObjectDescriptor EMPTY{0, 0, ObjectStatus::Empty};
inline constexpr ObjectDescriptor ABORT{0, 0, ObjectStatus::Abort};

// Pack capacity (log2) in lowest 6 bits, since allocations are 64 bit aligned.
struct PackedRingDescriptor {
  static constexpr uint64_t TAG_MASK = 0x3FULL;
  static constexpr uint64_t OFFSET_MASK = SYSTEM_OFFSET_MASK & ~TAG_MASK;
  uint64_t data_{0};

  constexpr PackedRingDescriptor() noexcept = default;
  constexpr PackedRingDescriptor(uint64_t offset, uint64_t capacity) noexcept {
    pack(offset, capacity);
  }

  constexpr void pack(uint64_t offset, uint64_t capacity) noexcept {
    const uint64_t cap_log2{static_cast<uint64_t>(std::countr_zero(capacity))};
    data_ = (offset & OFFSET_MASK) | (cap_log2 & TAG_MASK);
  }

  [[nodiscard]] inline uint64_t get_offset() const noexcept {
    return data_ & OFFSET_MASK;
  }

  [[nodiscard]] inline uint64_t get_capacity() const noexcept {
    const uint64_t cap_log2{data_ & TAG_MASK};
    return 1ULL << cap_log2;
  }
};

static_assert(std::atomic<PackedRingDescriptor>::is_always_lock_free,
              "CRITICAL: atomic<PackedRingDescriptor> must be lock-free");

// Structure and algorithms inspired by David Chase and Yossi Lev, 2005.
struct alignas(CACHE_LINE_SIZE) Buffer {
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> top_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> bottom_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<PackedRingDescriptor> descriptor_{};
};

// Memory allocation constants
inline constexpr uint64_t NULL_OFFSET{0};
inline constexpr int MIN_FREE_LIST_BIN_CAP{6}; // Buckets store 2^6 = 64 objects
inline constexpr int NUM_FREE_LIST_BINS{25};

// Metadata on the shared memory state.
struct Registry {
  std::atomic<int64_t> num_active_processes_{0};
  std::atomic<uint64_t> live_mask_{0};
  std::atomic<uint64_t> global_segement_size_{0};

  std::atomic<uint64_t> unallocated_memory_top_{
      (sizeof(Registry) + CACHE_LINE_SIZE - 1) &
      ~(CACHE_LINE_SIZE - 1)}; // Bump allocator

  struct alignas(CACHE_LINE_SIZE) PaddedFreeList {
    std::atomic<uint64_t> head_{NULL_OFFSET};
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

private:
  // --- Deque state helpers ---
  [[nodiscard]] bool grow(uint64_t current_capacity);

  // TODO: Shrink buffers at low usage
  //
  void cleanup() noexcept;

  // --- Data access helpers ---
  [[nodiscard]] ObjectDescriptor write(const char *serialized_data,
                                       std::size_t size);
  // We are embedding the size attribute of the ObjectDescriptor (inside each
  // RingSlot) into the beginning of the allocated block where the objected is
  // stored.
  void recover_size(ObjectDescriptor &desc) const noexcept;
  [[nodiscard]] const char *
  payload_ptr(const ObjectDescriptor &d) const noexcept;

  // --- Memory management helpers ---
  static constexpr uint64_t FREE_LIST_OFFSET_MASK = SYSTEM_OFFSET_MASK;
  static constexpr int ABA_TAG_SHIFT = 40;

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

  // CRITICAL IMPORTANT: For the individual items removed (not the buffers) via
  // `get` or `steal`, memory needs to be freed. We cannot directly call `free`.
  // Perhaps I can call `ebr_retire` in the constructor of the Python object or
  // `memoryview` (expose this functionality in the bindings).
  void ebr_retire(uint64_t offset, uint64_t size_bytes) noexcept;

  // Returns `false` and fails if a process is in an older epoch.
  bool ebr_try_advance() noexcept;
  void ebr_reclaim() noexcept;
};
} // namespace moveitmoveit
