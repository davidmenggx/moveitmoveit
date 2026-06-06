#pragma once

#include "utils/rng.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

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
static_assert(sizeof(ObjectDescriptor) == 16,
              "Double width CAS compares bitwise, no padding is acceptable");

// TODO: Consider a more performant implementation for single type, such as
// inlining the ObjectDescriptors.

// Metadata for where the ring buffer is located in shared memory.
struct alignas(16) RingDescriptor {
  std::atomic<uint64_t> offset_{0};
  std::atomic<uint64_t> capacity_{0};

  bool operator==(const RingDescriptor &other) const noexcept = default;

  RingDescriptor &operator=(RingDescriptor &&other) {
    if (this != &other) {
      offset_.store(other.offset_.load(std::memory_order_relaxed));
      capacity_.store(other.capacity_.load(std::memory_order_relaxed));
    }
    return *this;
  }
};

// Structure and algorithms inspired by David Chase and Yossi Lev, 2005.
struct alignas(CACHE_LINE_SIZE) Buffer {
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> top_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> bottom_{0};

  // The RingDescriptor is only changed in grow, which is only called by a
  // single thread. Therefore we can use a seqlock instead of a 128 bit atomic
  // for better performance.
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> desc_seq_{0};
  std::atomic<uint64_t> desc_offset_{0};
  std::atomic<uint64_t> desc_capacity_{0};
};

// Memory allocation constants
inline constexpr uint64_t NULL_OFFSET{0};
inline constexpr int MIN_FREE_LIST_BIN_CAP{6}; // Buckets store 2^6 = 64 objects
inline constexpr int NUM_FREE_LIST_BINS{27};

// Metadata to prevent race conditions in the segregated free list.
struct alignas(16) FreeListHead {
  uint64_t offset_{};
  uint64_t tag_{};

  bool operator==(const FreeListHead &other) const noexcept = default;
};

static_assert(std::atomic<FreeListHead>::is_always_lock_free,
              "CRITICAL: FreeListHead (128 bits) must be lock free");
static_assert(sizeof(FreeListHead) == 16,
              "Double width CAS compares bitwise, no padding is acceptable");

// Metadata for tracking a retired buffer within shared memory.
struct RetireNode {
  uint64_t offset_{0};
  uint64_t size_bytes_{0};
  uint64_t next_{0};
  uint64_t retire_epoch_{0};
};

// Metadata on the shared memory state.
struct Registry {
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> num_active_processes_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> live_mask_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> global_segement_size_{0};

  // For the bump allocator
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> unallocated_memory_top_{
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

  // Prevent EBR from getting stuck when a process crashes.
  static constexpr uint64_t EBR_PIN_TIMEOUT_NS{120'000'000'000ULL}; // 120s
  struct alignas(CACHE_LINE_SIZE) Heartbeat {
    std::atomic<uint64_t> last_seen_ns_{0};
  };
  Heartbeat heartbeats_[MAX_BUFFERS];

  // In shared memory
  struct alignas(CACHE_LINE_SIZE) ProcessRetireHeads {
    std::atomic<uint64_t> epoch_lists_[3]{NULL_OFFSET, NULL_OFFSET,
                                          NULL_OFFSET};
  };
  ProcessRetireHeads retire_heads_[MAX_BUFFERS];

  struct alignas(CACHE_LINE_SIZE) OwnerPid {
    std::atomic<uint64_t> value_{0}; // 0 == unclaimed / mid-registration
  };
  OwnerPid owner_pids_[MAX_BUFFERS];
};

// Constructor retry constants
inline constexpr int MAX_FTRUNCATE_RETRIES{1'000};
inline constexpr int MAX_REGISTRY_RETRIES{1'000};
inline constexpr int SLEEP_US{1'000};

class Deque {
public:
  Deque(std::string group_id, std::size_t total_memory_capacity_mb = 16'384);
  ~Deque() noexcept;

  Deque(const Deque &other) = delete;
  Deque &operator=(const Deque &other) = delete;
  Deque(Deque &&other) = delete;
  Deque &operator=(Deque &&other) = delete;

  // Structure and algorithms inspired by David Chase and Yossi Lev, 2005.

  /**
   * @brief Attempts to add the specified item to the buffer. If the current
   * buffer is full, try to resize up. Not thread safe.
   *
   * @param[in] serialized_data A pointer to the serialized object.
   *
   * @param[in] size The size of the object.
   *
   * @throws std::bad_alloc if out of memory, either for the specified item or
   * when resizing the buffer up.
   */
  void put(const char *serialized_data, std::size_t size);

  // Never resizes the buffer, returns false if capacity is reached.

  /**
   * @brief Attempts to add the specified item to the buffer. If the current
   * buffer is full, returns false. Not thread safe.
   *
   * @param[in] serialized_data A pointer to the serialized object.
   *
   * @param[in] size The size of the object.
   *
   * @return bool True if the object was successfully pushed to the deque, false
   * if the deque was empty and the object was not added.
   *
   * @throws std::bad_alloc if out of memory for the specified item.
   */
  [[nodiscard]] bool try_put(const char *serialized_data, std::size_t size);

  /**
   * @brief Attempts to remove and returns the newest item from this deque. Not
   * thread safe.
   *
   * @return An `ObjectDescriptor` containing the removed newest item from this
   * deque, and `EMPTY` if this deque is empty.
   */
  [[nodiscard]] ObjectDescriptor get();

  /**
   * @brief Attempts to remove and returns the oldest item from another deque.
   *
   * @param[in] target_longest [optional] If `true`, attempts to steal from the
   * deque with the most items. If `false`, chooses a random deque uniformly,
   * not necessarily nonempty. Largest size result unreliable. (Default: false)
   *
   * param[in] target_first [optional] If `true`, attempts to steal from the
   * first deque, based on an internal ordering approximately equal to the order
   * in which the deques were initialized. Since this ordering is unreliable,
   * this parameter should primarily considered as a performance optimization.
   * Overrides the value of `target_longest`. (Default: false)
   *
   * @return An `ObjectDescriptor` containing the removed oldest item from the
   * selected deque, `EMPTY` if there are no valid deques or the selected deque
   * contains no elements, and `ABORT` on failure. Users should retry after
   * `ABORT`.
   */
  [[nodiscard]] ObjectDescriptor steal(bool target_longest = false,
                                       bool target_first = false);

  // Returns the approximate size of this deque. Results unreliable.
  [[nodiscard]] std::size_t qsize() const;

  // Returns `true` if this deque is empty. Results unreliable.
  [[nodiscard]] bool empty() const;

  // Returns `true` if this deque is full. Results unreliable.
  [[nodiscard]] bool full() const;

  // CRITICAL IMPORTANT: This should be called after the returned Python object
  // is destructed. This must be called on the same `Deque` instance which it
  // was retrieved from. This is not thread safe. This must not be called on the
  // same `ObjectDescriptor` twice.
  void release(ObjectDescriptor &desc) noexcept;

  // Python binding data access helper.
  [[nodiscard]] const char *get_data_ptr(uint64_t offset) const noexcept;

private:
  // --- Deque state helpers ---

  [[nodiscard]] bool grow(uint64_t current_capacity);

  // TODO: Shrink buffers at low usage

  void cleanup() noexcept;

  // --- Data access helpers ---

  [[nodiscard]] ObjectDescriptor write(const char *serialized_data,
                                       std::size_t size);

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

  // --- Initialization progress ---
  bool process_count_incremented_{false};
  bool fully_initialized_{false};

  FastRNG rng_{};

  // --- Epoch based reclamation state ---
  static constexpr int EBR_EPOCHS = 3;
  static constexpr int EBR_CYCLE_FORCE_ADVANCE = 128;
  int ebr_cycles_since_advance_{0};

  // --- Epoch based reclamation helpers ---

  void ebr_pin() noexcept;

  void ebr_unpin() noexcept;

  void ebr_retire(uint64_t offset, uint64_t size_bytes) noexcept;

  // Returns `false` and fails if a process is in an older epoch.
  bool ebr_try_advance() noexcept;

  void ebr_reclaim() noexcept;

  // --- Dead process reclamation helpers ---
  // TODO: These rely on PID. PID re-use could be problematic, perhaps also
  // store the time of creation to be ultra conservative.
  [[nodiscard]] ObjectDescriptor make_descriptor(uint64_t block) const noexcept;

  void beat() noexcept;

  void ebr_periodic() noexcept;

  [[nodiscard]] bool slot_owner_alive(int idx, uint64_t now) noexcept;

  void reclaim_dead_slot(int idx) noexcept;

  void ebr_reap_dead_slots(uint64_t now) noexcept;
};
} // namespace moveitmoveit
