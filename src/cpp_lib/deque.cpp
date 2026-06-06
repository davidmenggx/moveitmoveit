#include "deque.hpp"
#include "utils/calculate_bin.hpp"
#include "utils/monotonic_ns.hpp"
#include "utils/valid_shm_name.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <signal.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

namespace moveitmoveit {
// --- Seqlock helpers ---
namespace {
inline RingDescriptor load_descriptor(const Buffer *b) noexcept {
  constexpr int MAX_SEQ_SPINS{1 << 16};
  for (int spins{0}; spins < MAX_SEQ_SPINS; ++spins) {
    const uint64_t s0{b->desc_seq_.load(std::memory_order_acquire)};

    if (s0 & 1)
      continue;

    const uint64_t off{b->desc_offset_.load(std::memory_order_relaxed)};
    const uint64_t cap{b->desc_capacity_.load(std::memory_order_relaxed)};
    std::atomic_thread_fence(std::memory_order_acquire);

    if (b->desc_seq_.load(std::memory_order_relaxed) == s0)
      return RingDescriptor{.offset_ = off, .capacity_ = cap};
  }
  return RingDescriptor{.offset_ = NULL_OFFSET, .capacity_ = 0}; // give up
}

inline void store_descriptor(Buffer *b, const RingDescriptor &d) noexcept {
  const uint64_t s{b->desc_seq_.load(std::memory_order_relaxed) + 1};
  b->desc_seq_.store(s, std::memory_order_relaxed); // Begin

  std::atomic_thread_fence(std::memory_order_release);

  b->desc_offset_.store(d.offset_, std::memory_order_relaxed);
  b->desc_capacity_.store(d.capacity_, std::memory_order_relaxed);
  b->desc_seq_.store(s + 1, std::memory_order_release); // Publish
}
} // namespace

Deque::Deque(std::string group_id, std::size_t total_memory_capacity_mb)
    : shm_name_{valid_shm_name(group_id)} {
  shm_fd_ = shm_open(shm_name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);

  // The first deque in the group needs to open shared memory and establish
  // Registry. Detect with flag O_EXCL.
  if (shm_fd_ != -1) {
    is_creator_ = true;
    segment_size_ = total_memory_capacity_mb * 1024 * 1024;
    if (ftruncate(shm_fd_, segment_size_) == -1) {
      cleanup();
      throw std::runtime_error("CRITICAL: ftruncate failed: " +
                               std::string(std::strerror(errno)) + ".");
    }
  } else if (errno == EEXIST) {
    is_creator_ = false;
    shm_fd_ = shm_open(shm_name_.c_str(), O_RDWR, 0666);
    if (shm_fd_ == -1) {
      cleanup();
      throw std::runtime_error(
          "CRITICAL: Failed to open existing shared memory.");
    }

    // Wait for the creator to set the memory using ftruncate.
    struct stat statbuf{};
    for (int i{0}; statbuf.st_size == 0; ++i) {
      if (i >= MAX_FTRUNCATE_RETRIES) {
        close(shm_fd_);
        throw std::runtime_error(
            "CRITICAL: Timed out waiting for creator to size the segment.");
      }
      std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_US));
      fstat(shm_fd_, &statbuf);
    }

    segment_size_ = statbuf.st_size;
  } else {
    throw std::runtime_error("CRITICAL: Error opening shared memory.");
  }

  base_address_ = mmap(nullptr, segment_size_, PROT_READ | PROT_WRITE,
                       MAP_SHARED, shm_fd_, 0);
  if (base_address_ == MAP_FAILED) {
    cleanup();
    throw std::runtime_error("CRITICAL: mmap failed: " +
                             std::string(std::strerror(errno)));
  }

  // Creator sets the Registry metadata block, non-creators register themselves.
  if (is_creator_) {
    registry_ = new (base_address_) Registry();
    registry_->global_segement_size_.store(segment_size_,
                                           std::memory_order_relaxed);
    queue_idx_ = 0;
    registry_->live_mask_.store(1ULL << queue_idx_, std::memory_order_release);
    registry_->initialized_flag_.store(1, std::memory_order_seq_cst);
  } else {
    Registry *temp_reg{reinterpret_cast<Registry *>(base_address_)};
    for (int i{0};
         temp_reg->initialized_flag_.load(std::memory_order_acquire) == 0;
         ++i) {
      if (i >= MAX_REGISTRY_RETRIES) {
        cleanup();
        throw std::runtime_error(
            "CRITICAL: Timed out waiting for creator to open Registry");
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    registry_ = temp_reg;

    // Loop to try and find an index to claim.
    uint64_t current_mask{
        registry_->live_mask_.load(std::memory_order_acquire)};
    while (true) {
      int free_idx{std::countr_zero(~current_mask)};
      if (free_idx >= MAX_BUFFERS) {
        cleanup();
        throw std::runtime_error("CRITICAL: Deque group is full (" +
                                 std::to_string(MAX_BUFFERS) + " max).");
      }

      uint64_t new_mask{current_mask | (1ULL << free_idx)};
      if (registry_->live_mask_.compare_exchange_weak(
              current_mask, new_mask, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        queue_idx_ = free_idx;
        break;
      }
    }
  }

  registry_->owner_pids_[queue_idx_].value_.store(
      static_cast<uint64_t>(getpid()), std::memory_order_release);
  beat();

  const uint64_t buf_offset{registry_->unallocated_memory_top_.fetch_add(
      (sizeof(Buffer) + CACHE_LINE_SIZE - 1) &
          ~(CACHE_LINE_SIZE - 1), // Align up
      std::memory_order_relaxed)};
  my_buffer_ = new (offset_to_ptr<void>(buf_offset)) Buffer();
  registry_->buffer_offsets_[queue_idx_].value_.store(
      buf_offset, std::memory_order_release);

  const uint64_t initial_capacity{1 << MIN_FREE_LIST_BIN_CAP};
  const uint64_t initial_buffer_offset{
      allocate(initial_capacity * sizeof(std::atomic<ObjectDescriptor>))};

  if (initial_buffer_offset == NULL_OFFSET) {
    cleanup();
    throw std::runtime_error(
        "CRITICAL: Initial buffer allocation failed, out of memory.");
  }
  RingDescriptor initial_descriptor{.offset_ = initial_buffer_offset,
                                    .capacity_ = initial_capacity};
  store_descriptor(my_buffer_, initial_descriptor);

  registry_->num_active_processes_.fetch_add(1, std::memory_order_relaxed);
  process_count_incremented_ = true;

  rng_.seed(getpid());
  fully_initialized_ = true;
}

Deque::~Deque() noexcept {
  ebr_unpin();
  cleanup();
}

[[nodiscard]] ObjectDescriptor Deque::write(const char *serialized_data,
                                            std::size_t size) {
  if (size == 0 || size > std::numeric_limits<uint32_t>::max()) [[unlikely]]
    return ABORT;

  const uint64_t alloc_offset{allocate(size)};
  if (alloc_offset == NULL_OFFSET) [[unlikely]]
    return ABORT;

  char *base{offset_to_ptr<char>(alloc_offset)};
  std::memcpy(base, serialized_data, size);

  return ObjectDescriptor{.offset_ = alloc_offset,
                          .size_ = static_cast<uint32_t>(size),
                          .status_ = ObjectStatus::Ok};
}

void Deque::put(const char *serialized_data, std::size_t size) {
  const int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  const int64_t top{my_buffer_->top_.load(
      std::memory_order_relaxed)}; // This was acquire but switched to
                                   // relax since a stale result just results in
                                   // an unneeded grow, which is still correct.
  RingDescriptor buf_desc{load_descriptor(my_buffer_)};
  uint64_t capacity{buf_desc.capacity_};

  // Attempt to resize up if at capacity.
  if (bottom - top >= static_cast<int64_t>(capacity)) {
    if (!grow(capacity))
      throw std::bad_alloc();
    buf_desc = load_descriptor(my_buffer_);
    capacity = buf_desc.capacity_;
  }

  ObjectDescriptor obj_desc{write(serialized_data, size)};
  if (obj_desc.status_ == ObjectStatus::Abort)
    throw std::bad_alloc();

  auto *ring_buffer =
      offset_to_ptr<std::atomic<ObjectDescriptor>>(buf_desc.offset_);

  ring_buffer[bottom & (capacity - 1)].store(obj_desc,
                                             std::memory_order_relaxed);

  my_buffer_->bottom_.store(bottom + 1, std::memory_order_release);

  ebr_periodic();
}

[[nodiscard]] bool Deque::try_put(const char *serialized_data,
                                  std::size_t size) {
  const int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  const int64_t top{my_buffer_->top_.load(std::memory_order_acquire)};
  RingDescriptor buf_desc{load_descriptor(my_buffer_)};
  const uint64_t capacity{buf_desc.capacity_};

  // Do not resize up if at capacity, return false.
  if (bottom - top >= static_cast<int64_t>(capacity))
    return false;

  ObjectDescriptor obj_desc{write(serialized_data, size)};
  if (obj_desc.status_ == ObjectStatus::Abort)
    throw std::bad_alloc();

  auto *ring_buffer =
      offset_to_ptr<std::atomic<ObjectDescriptor>>(buf_desc.offset_);

  ring_buffer[bottom & (capacity - 1)].store(obj_desc,
                                             std::memory_order_relaxed);

  my_buffer_->bottom_.store(bottom + 1, std::memory_order_release);

  ebr_periodic();

  return true;
}

[[nodiscard]] ObjectDescriptor Deque::get() {
  int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  --bottom;
  my_buffer_->bottom_.store(bottom, std::memory_order_release);

  std::atomic_thread_fence(std::memory_order_seq_cst);

  int64_t top{my_buffer_->top_.load(std::memory_order_seq_cst)};

  ebr_periodic();

  if (top <= bottom) {
    // Deque is nonempty
    RingDescriptor desc{load_descriptor(my_buffer_)};
    const uint64_t capacity{desc.capacity_};
    auto *ring_buffer =
        offset_to_ptr<std::atomic<ObjectDescriptor>>(desc.offset_);

    ObjectDescriptor item{
        ring_buffer[bottom & (capacity - 1)].load(std::memory_order_relaxed)};

    if (top != bottom)
      return item;

    // Compete with possible thieves over the last item
    if (!my_buffer_->top_.compare_exchange_strong(
            top, top + 1, std::memory_order_seq_cst, std::memory_order_relaxed))
      item = EMPTY;

    my_buffer_->bottom_.store(bottom + 1, std::memory_order_relaxed);
    return item;
  } else {
    // Deque is empty
    my_buffer_->bottom_.store(bottom + 1, std::memory_order_relaxed);
    return EMPTY;
  }
}

[[nodiscard]] ObjectDescriptor Deque::steal(bool target_longest,
                                            bool target_first) {
  uint64_t candidates{registry_->live_mask_.load(std::memory_order_acquire) &
                      ~(1ULL << queue_idx_)};
  if (candidates == 0)
    return EMPTY;

  int victim_idx{-1};

  if (target_first) {
    uint64_t temp_candidates{candidates};

    while (temp_candidates != 0) {
      // Get the lowest active bit
      int idx{std::countr_zero(temp_candidates)};
      const uint64_t candidate_buf_offset{
          registry_->buffer_offsets_[idx].value_.load(
              std::memory_order_acquire)};

      if (candidate_buf_offset != NULL_OFFSET) {
        auto *candidate_buffer = offset_to_ptr<Buffer>(candidate_buf_offset);

        const int64_t top{
            candidate_buffer->top_.load(std::memory_order_relaxed)};
        const int64_t bottom{
            candidate_buffer->bottom_.load(std::memory_order_relaxed)};

        if (bottom > top) {
          victim_idx = idx;
          break;
        }
      }

      temp_candidates &= temp_candidates - 1;
    }

    if (victim_idx == -1)
      return EMPTY;

  } else if (target_longest) {
    int64_t maximum_size{-1};
    for (int idx{0}; idx < MAX_BUFFERS; ++idx) {
      if ((candidates & (1ULL << idx)) == 0)
        continue;

      const uint64_t candidate_buf_offset{
          registry_->buffer_offsets_[idx].value_.load(
              std::memory_order_acquire)};
      if (candidate_buf_offset == NULL_OFFSET)
        continue;

      auto *candidate_buffer = offset_to_ptr<Buffer>(candidate_buf_offset);

      const int64_t top{candidate_buffer->top_.load(std::memory_order_relaxed)};
      const int64_t bottom{
          candidate_buffer->bottom_.load(std::memory_order_relaxed)};
      const int64_t current_size{bottom - top};
      if (current_size > maximum_size) {
        maximum_size = current_size;
        victim_idx = idx;
      }
    }

    if (victim_idx == -1)
      return EMPTY;

  } else {
    const int num_candidates{std::popcount(candidates)};
    const int pick{rng_.random<int>(0, num_candidates - 1)};
    uint64_t temp{candidates};
    for (int i{0}; i < pick; ++i)
      temp &= temp - 1;
    victim_idx = std::countr_zero(temp);
  }

  const uint64_t victim_buf_offset{
      registry_->buffer_offsets_[victim_idx].value_.load(
          std::memory_order_acquire)};
  if (victim_buf_offset == NULL_OFFSET)
    return EMPTY;

  auto *victim_buffer = offset_to_ptr<Buffer>(victim_buf_offset);

  // Unpinned pre-check: Buffer is never retired so top_/bottom_ are always
  // safe to load without a pin. This avoids pinning for clearly empty victims.
  {
    int64_t top{victim_buffer->top_.load(std::memory_order_acquire)};
    std::atomic_thread_fence(std::memory_order_seq_cst);
    int64_t bottom{victim_buffer->bottom_.load(std::memory_order_acquire)};
    if (top >= bottom)
      return EMPTY;
  }

  ebr_pin();

  // Re-read under the pin. The pre-check window is unprotected, so top or
  // bottom may have changed (a concurrent steal could have drained the queue).
  int64_t top{victim_buffer->top_.load(std::memory_order_acquire)};
  std::atomic_thread_fence(std::memory_order_seq_cst);
  int64_t bottom{victim_buffer->bottom_.load(std::memory_order_acquire)};

  if (top >= bottom) {
    ebr_unpin();
    return EMPTY;
  }

  const RingDescriptor desc{load_descriptor(victim_buffer)};

  if (desc.capacity_ == 0) {
    ebr_unpin();
    return ABORT;
  }

  const uint64_t capacity{desc.capacity_};
  auto *ring_buffer =
      offset_to_ptr<std::atomic<ObjectDescriptor>>(desc.offset_);

  ObjectDescriptor item{
      ring_buffer[top & (capacity - 1)].load(std::memory_order_relaxed)};

  const bool cas_ok{victim_buffer->top_.compare_exchange_strong(
      top, top + 1, std::memory_order_seq_cst, std::memory_order_relaxed)};

  ebr_unpin();

  if (cas_ok && ebr_try_advance())
    ebr_reclaim();

  if (!cas_ok)
    return ABORT;

  return item;
}

[[nodiscard]] std::size_t Deque::qsize() const {
  const int64_t top{my_buffer_->top_.load(std::memory_order_relaxed)};
  const int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  const int64_t current_size{bottom - top};
  return current_size < 0 ? 0 : static_cast<std::size_t>(current_size);
}

[[nodiscard]] bool Deque::empty() const { return qsize() == 0; }

[[nodiscard]] bool Deque::full() const {
  RingDescriptor desc{load_descriptor(my_buffer_)};
  return qsize() >= desc.capacity_;
}

// TODO: Consider memory fragmentation issues
[[nodiscard]] uint64_t Deque::allocate(uint64_t capacity) {
  const int bin_idx{
      calculate_bin(capacity, MIN_FREE_LIST_BIN_CAP, NUM_FREE_LIST_BINS)};
  if (bin_idx < 0) [[unlikely]]
    return NULL_OFFSET;

  FreeListHead head{
      registry_->free_lists_[bin_idx].head_.load(std::memory_order_acquire)};

  while (true) {
    if (head.offset_ == NULL_OFFSET)
      break; // Fallback to bump allocator

    auto *next_offset_ptr = offset_to_ptr<uint64_t>(head.offset_);
    const uint64_t next_offset{std::atomic_ref<uint64_t>(*next_offset_ptr)
                                   .load(std::memory_order_relaxed)};
    FreeListHead new_head{next_offset, head.tag_};

    if (registry_->free_lists_[bin_idx].head_.compare_exchange_weak(
            head, new_head, std::memory_order_acquire,
            std::memory_order_acquire))
      return head.offset_;
  }

  // Fallback to bump allocation. Allocate the entire bin (not just capacity) so
  // it can be recycled.
  const uint64_t bytes_needed{1ULL << (MIN_FREE_LIST_BIN_CAP + bin_idx)};

  uint64_t old_top{
      registry_->unallocated_memory_top_.load(std::memory_order_relaxed)};

  while (true) {
    const uint64_t aligned_top{(old_top + CACHE_LINE_SIZE - 1) &
                               ~(CACHE_LINE_SIZE - 1)}; // Align up
    const uint64_t new_top{aligned_top + bytes_needed};

    if (new_top > registry_->global_segement_size_.load(
                      std::memory_order_relaxed)) [[unlikely]]
      return NULL_OFFSET; // Out of memory

    if (registry_->unallocated_memory_top_.compare_exchange_weak(
            old_top, new_top, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      return aligned_top;
    }
  }
}

void Deque::free(uint64_t offset, uint64_t capacity) {
  if (offset == NULL_OFFSET) [[unlikely]]
    return;

  const int bin_idx{
      calculate_bin(capacity, MIN_FREE_LIST_BIN_CAP, NUM_FREE_LIST_BINS)};

  if (bin_idx < 0) [[unlikely]]
    return;

  auto *next_ptr = offset_to_ptr<uint64_t>(offset);

  FreeListHead head{
      registry_->free_lists_[bin_idx].head_.load(std::memory_order_relaxed)};

  while (true) {
    std::atomic_ref<uint64_t>(*next_ptr).store(head.offset_,
                                               std::memory_order_relaxed);
    FreeListHead new_head{.offset_ = offset, .tag_ = head.tag_ + 1};

    if (registry_->free_lists_[bin_idx].head_.compare_exchange_weak(
            head, new_head, std::memory_order_release,
            std::memory_order_relaxed))
      break;
  }
}

void Deque::release(ObjectDescriptor &desc) noexcept {
  if (desc.status_ != ObjectStatus::Ok || desc.offset_ == NULL_OFFSET)
    return;

  free(desc.offset_, static_cast<uint64_t>(desc.size_));

  desc.status_ = ObjectStatus::Empty;
  desc.offset_ = NULL_OFFSET;
}

[[nodiscard]] const char *Deque::get_data_ptr(uint64_t offset) const noexcept {
  return offset_to_ptr<const char>(offset);
}

[[nodiscard]] bool Deque::grow(uint64_t current_capacity) {
  const RingDescriptor desc{load_descriptor(my_buffer_)};
  const uint64_t old_offset{desc.offset_};
  const uint64_t old_size{current_capacity *
                          sizeof(std::atomic<ObjectDescriptor>)};

  const uint64_t new_capacity{current_capacity * 2};
  const uint64_t new_offset{
      allocate(new_capacity * sizeof(std::atomic<ObjectDescriptor>))};

  if (new_offset == NULL_OFFSET) [[unlikely]]
    return false;

  auto *old_ring = offset_to_ptr<std::atomic<ObjectDescriptor>>(old_offset);
  auto *new_ring = offset_to_ptr<std::atomic<ObjectDescriptor>>(new_offset);

  const int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  const int64_t top{my_buffer_->top_.load(std::memory_order_acquire)};

  for (int64_t i{top}; i < bottom; ++i) {
    const ObjectDescriptor item{
        old_ring[i & (current_capacity - 1)].load(std::memory_order_relaxed)};

    new_ring[i & (new_capacity - 1)].store(item, std::memory_order_relaxed);
  }

  RingDescriptor new_desc{.offset_ = new_offset, .capacity_ = new_capacity};
  store_descriptor(my_buffer_, new_desc);

  ebr_retire(old_offset, old_size);
  if (ebr_try_advance())
    ebr_reclaim();

  return true;
}

void Deque::cleanup() noexcept {
  int64_t num_active_processes{-1};
  if (registry_ != nullptr && queue_idx_ != -1) {
    registry_->buffer_offsets_[queue_idx_].value_.store(
        NULL_OFFSET, std::memory_order_release);
    registry_->live_mask_.fetch_and(~(1ULL << queue_idx_),
                                    std::memory_order_release);
    registry_->owner_pids_[queue_idx_].value_.store(NULL_OFFSET,
                                                    std::memory_order_release);

    if (process_count_incremented_) {
      num_active_processes = registry_->num_active_processes_.fetch_sub(
          1, std::memory_order_acq_rel);
      process_count_incremented_ = false;

      // Last process out should clean up all retired memory offsets. As last
      // process, there is no contention with other processes.
      if (num_active_processes == 1) {
        for (int idx{0}; idx < MAX_BUFFERS; ++idx) {
          for (int e{0}; e < EBR_EPOCHS; ++e) {
            uint64_t head_offset{
                registry_->retire_heads_[idx].epoch_lists_[e].exchange(
                    NULL_OFFSET, std::memory_order_relaxed)};
            while (head_offset != NULL_OFFSET) {
              RetireNode *node{offset_to_ptr<RetireNode>(head_offset)};
              uint64_t next_offset{node->next_};
              free(node->offset_, node->size_bytes_);
              free(head_offset, sizeof(RetireNode));
              head_offset = next_offset;
            }
          }
        }
      }
    }
    queue_idx_ = -1;
    registry_ = nullptr;
  }

  if (base_address_ != nullptr && base_address_ != MAP_FAILED) {
    munmap(base_address_, segment_size_);
    base_address_ = nullptr;
  }

  if (shm_fd_ != -1) {
    close(shm_fd_);
    shm_fd_ = -1;
  }

  // A creator that fails at `ftruncate` or `mmap` leaves an invalid or
  // improperly-sized memory region.
  if (num_active_processes == 1 || (is_creator_ && !fully_initialized_))
    shm_unlink(shm_name_.c_str());
}

void Deque::ebr_pin() noexcept {
  registry_->heartbeats_[queue_idx_].last_seen_ns_.store(
      monotonic_ns(), std::memory_order_seq_cst);

  uint64_t epoch{};
  do {
    epoch = registry_->global_epoch_.load(std::memory_order_acquire);
    registry_->pinned_epochs_[queue_idx_].value_.store(
        epoch, std::memory_order_seq_cst);
    std::atomic_thread_fence(std::memory_order_seq_cst);
  } while (registry_->global_epoch_.load(std::memory_order_acquire) != epoch);
}

void Deque::ebr_unpin() noexcept {
  if (registry_ != nullptr && queue_idx_ >= 0)
    registry_->pinned_epochs_[queue_idx_].value_.store(
        Registry::EBR_UNPINNED, std::memory_order_release);
}

void Deque::ebr_retire(uint64_t offset, uint64_t size_bytes) noexcept {
  uint64_t node_offset{allocate(sizeof(RetireNode))};

  if (node_offset == NULL_OFFSET) {
    if (ebr_try_advance())
      ebr_reclaim();
    node_offset = allocate(sizeof(RetireNode));
    if (node_offset == NULL_OFFSET)
      return; // Out of memory
  }

  ebr_pin();
  const uint64_t epoch{
      registry_->global_epoch_.load(std::memory_order_acquire)};
  const int epoch_idx{static_cast<int>(epoch % EBR_EPOCHS)};

  RetireNode *node{offset_to_ptr<RetireNode>(node_offset)};
  node->offset_ = offset;
  node->size_bytes_ = size_bytes;
  node->retire_epoch_ = epoch;

  uint64_t old_head{
      registry_->retire_heads_[queue_idx_].epoch_lists_[epoch_idx].load(
          std::memory_order_relaxed)};
  do {
    node->next_ = old_head;
  } while (!registry_->retire_heads_[queue_idx_]
                .epoch_lists_[epoch_idx]
                .compare_exchange_weak(old_head, node_offset,
                                       std::memory_order_release,
                                       std::memory_order_relaxed));

  ebr_unpin();
}

bool Deque::ebr_try_advance() noexcept {
  uint64_t global{registry_->global_epoch_.load(std::memory_order_acquire)};
  const uint64_t mask{registry_->live_mask_.load(std::memory_order_acquire)};
  const uint64_t now{monotonic_ns()};

  uint64_t temp{mask};
  while (temp) {
    const int idx{std::countr_zero(temp)};
    temp &= temp - 1;

    const uint64_t pinned{
        registry_->pinned_epochs_[idx].value_.load(std::memory_order_acquire)};
    if (pinned == Registry::EBR_UNPINNED || pinned == global)
      continue;

    if (idx == queue_idx_)
      return false; // We are at an old epoch.

    if (slot_owner_alive(idx, now))
      return false; // Alive

    reclaim_dead_slot(idx); // Dead
  }

  return registry_->global_epoch_.compare_exchange_strong(
      global, global + 1, std::memory_order_acq_rel, std::memory_order_relaxed);
}

void Deque::ebr_reclaim() noexcept {
  const uint64_t global{
      registry_->global_epoch_.load(std::memory_order_acquire)};
  if (global < 2)
    return;

  const int reclaim_epoch_idx{static_cast<int>((global - 2) % EBR_EPOCHS)};

  for (int idx{0}; idx < MAX_BUFFERS; ++idx) {
    uint64_t head_offset{
        registry_->retire_heads_[idx].epoch_lists_[reclaim_epoch_idx].exchange(
            NULL_OFFSET, std::memory_order_acquire)};

    while (head_offset != NULL_OFFSET) {
      RetireNode *node{offset_to_ptr<RetireNode>(head_offset)};
      const uint64_t next_offset{node->next_};

      if (node->retire_epoch_ + 2 <= global) {
        free(node->offset_, node->size_bytes_);
        free(head_offset, sizeof(RetireNode));
      } else {
        uint64_t old_head{
            registry_->retire_heads_[idx].epoch_lists_[reclaim_epoch_idx].load(
                std::memory_order_relaxed)};
        do {
          node->next_ = old_head;
        } while (!registry_->retire_heads_[idx]
                      .epoch_lists_[reclaim_epoch_idx]
                      .compare_exchange_weak(old_head, head_offset,
                                             std::memory_order_release,
                                             std::memory_order_relaxed));
      }
      head_offset = next_offset;
    }
  }
}

bool Deque::slot_owner_alive(int idx, uint64_t now) noexcept {
  // Avoid syscalls with heartbeat
  const uint64_t hb{registry_->heartbeats_[idx].last_seen_ns_.load(
      std::memory_order_acquire)};
  if (now <= hb || now - hb < Registry::EBR_PIN_TIMEOUT_NS)
    return true;

  const uint64_t pid{
      registry_->owner_pids_[idx].value_.load(std::memory_order_acquire)};

  if (pid == 0)
    return true; // slot is mid-registration, treat as alive (conservative)

  if (::kill(static_cast<pid_t>(pid), 0) == 0)
    return true;

  if (errno == EPERM)
    return true;

  return false;
}

void Deque::reclaim_dead_slot(int idx) noexcept {
  uint64_t pid{
      registry_->owner_pids_[idx].value_.load(std::memory_order_acquire)};

  if (pid == 0)
    return;

  if (!registry_->owner_pids_[idx].value_.compare_exchange_strong(
          pid, 0, std::memory_order_acq_rel, std::memory_order_relaxed))
    return;

  // After this point you are the sole reclaimer.
  registry_->buffer_offsets_[idx].value_.store(NULL_OFFSET,
                                               std::memory_order_release);
  registry_->pinned_epochs_[idx].value_.store(Registry::EBR_UNPINNED,
                                              std::memory_order_release);
  registry_->num_active_processes_.fetch_sub(1, std::memory_order_acq_rel);
  registry_->live_mask_.fetch_and(~(1ULL << idx), std::memory_order_release);
}

void Deque::beat() noexcept {
  registry_->heartbeats_[queue_idx_].last_seen_ns_.store(
      monotonic_ns(), std::memory_order_release);
}

void Deque::ebr_reap_dead_slots(uint64_t now) noexcept {
  uint64_t temp{registry_->live_mask_.load(std::memory_order_acquire) &
                ~(1ULL << queue_idx_)};
  while (temp) {
    const int idx{std::countr_zero(temp)};
    temp &= temp - 1;
    if (!slot_owner_alive(idx, now))
      reclaim_dead_slot(idx);
  }
}

void Deque::ebr_periodic() noexcept {
  if (ebr_cycles_since_advance_++ < EBR_CYCLE_FORCE_ADVANCE)
    return;
  ebr_cycles_since_advance_ = 0;
  beat();
  ebr_reap_dead_slots(monotonic_ns());
  if (ebr_try_advance())
    ebr_reclaim();
}
} // namespace moveitmoveit
