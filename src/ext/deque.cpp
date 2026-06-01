#include "deque.hpp"
#include "utils/calculate_bin.hpp"
#include "utils/valid_shm_name.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace moveitmoveit {
Deque::Deque(std::string group_id, std::size_t default_size_mb)
    : shm_name_{valid_shm_name(group_id)} {
  shm_fd_ = shm_open(shm_name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);

  // The first deque in the group needs to open shared memory and establish
  // Registry. Detect with flag O_EXCL.
  if (shm_fd_ != -1) {
    is_creator_ = true;
    segment_size_ = default_size_mb * 1024 * 1024;
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

  const uint64_t buf_offset{registry_->unallocated_memory_top_.fetch_add(
      sizeof(Buffer), std::memory_order_relaxed)};
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
  my_buffer_->descriptor_.store(initial_descriptor, std::memory_order_relaxed);

  registry_->num_active_processes_.fetch_add(1, std::memory_order_relaxed);
  process_count_incremented_ = true;

  rng_.seed(getpid());
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

[[nodiscard]] inline const char *
Deque::payload_ptr(const ObjectDescriptor &d) const noexcept {
  return offset_to_ptr<const char>(d.offset_);
}

void Deque::put(const char *serialized_data, std::size_t size) {
  const int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  const int64_t top{my_buffer_->top_.load(std::memory_order_acquire)};
  RingDescriptor buf_desc{
      my_buffer_->descriptor_.load(std::memory_order_relaxed)};
  uint64_t capacity{buf_desc.capacity_};

  // Attempt to resize up if at capacity.
  if (bottom - top >= static_cast<int64_t>(capacity)) {
    if (!grow(capacity))
      throw std::bad_alloc();
    buf_desc = my_buffer_->descriptor_.load(std::memory_order_relaxed);
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

  if (ebr_cycles_since_advance_++ >= EBR_CYCLE_FORCE_ADVANCE) {
    if (ebr_try_advance())
      ebr_reclaim();
    ebr_cycles_since_advance_ = 0;
  }
}

[[nodiscard]] bool Deque::try_put(const char *serialized_data,
                                  std::size_t size) {
  const int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  const int64_t top{my_buffer_->top_.load(std::memory_order_acquire)};
  RingDescriptor buf_desc{
      my_buffer_->descriptor_.load(std::memory_order_relaxed)};
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

  if (ebr_cycles_since_advance_++ >= EBR_CYCLE_FORCE_ADVANCE) {
    if (ebr_try_advance())
      ebr_reclaim();
    ebr_cycles_since_advance_ = 0;
  }

  return true;
}

[[nodiscard]] ObjectDescriptor Deque::get() {
  int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  --bottom;
  my_buffer_->bottom_.store(bottom, std::memory_order_release);

  std::atomic_thread_fence(std::memory_order_seq_cst);

  int64_t top{my_buffer_->top_.load(std::memory_order_seq_cst)};

  if (ebr_cycles_since_advance_++ >= EBR_CYCLE_FORCE_ADVANCE) {
    if (ebr_try_advance())
      ebr_reclaim();
    ebr_cycles_since_advance_ = 0;
  }

  if (top <= bottom) {
    // Deque is nonempty
    RingDescriptor desc{
        my_buffer_->descriptor_.load(std::memory_order_relaxed)};
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
      item = ABORT;

    my_buffer_->bottom_.store(bottom + 1, std::memory_order_relaxed);
    return item;
  } else {
    // Deque is empty
    my_buffer_->bottom_.store(bottom + 1, std::memory_order_relaxed);
    return EMPTY;
  }
}

// TODO: Perhaps modify steal to choose the queue with the most items.
[[nodiscard]] ObjectDescriptor Deque::steal() {
  uint64_t candidates = registry_->live_mask_.load(std::memory_order_acquire) &
                        ~(1ULL << queue_idx_);
  if (candidates == 0)
    return EMPTY;

  const int num_candidates{std::popcount(candidates)};
  const int pick{rng_.random<int>(0, num_candidates - 1)};
  uint64_t temp{candidates};
  for (int i{0}; i < pick; ++i)
    temp &= temp - 1;
  const int victim_idx{std::countr_zero(temp)};

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

  const RingDescriptor desc{
      victim_buffer->descriptor_.load(std::memory_order_acquire)};
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
  const int64_t top{my_buffer_->top_.load(std::memory_order_acquire)};
  const int64_t bottom{my_buffer_->bottom_.load(std::memory_order_relaxed)};
  const int64_t current_size{bottom - top};
  return current_size < 0 ? 0 : static_cast<std::size_t>(current_size);
}

[[nodiscard]] bool Deque::empty() const { return qsize() == 0; }

[[nodiscard]] bool Deque::full() const {
  RingDescriptor desc{my_buffer_->descriptor_.load(std::memory_order_relaxed)};
  return qsize() >= desc.capacity_;
}

// TODO: Consier memory fragmentation issues
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

    auto *next_offset_ptr = offset_to_ptr<std::atomic<uint64_t>>(head.offset_);
    const uint64_t next_offset{
        next_offset_ptr->load(std::memory_order_relaxed)};
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
    uint64_t alignment{CACHE_LINE_SIZE};
    const uint64_t aligned_top{(old_top + alignment - 1) &
                               ~(alignment - 1)}; // Align up
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
  ebr_retire(desc.offset_, static_cast<uint64_t>(desc.size_));
  if (ebr_try_advance())
    ebr_reclaim();
  desc.status_ = ObjectStatus::Empty;
  desc.offset_ = NULL_OFFSET;
}

[[nodiscard]] bool Deque::grow(uint64_t current_capacity) {
  const RingDescriptor desc{
      my_buffer_->descriptor_.load(std::memory_order_relaxed)};
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
  my_buffer_->descriptor_.store(new_desc, std::memory_order_release);

  ebr_retire(old_offset, old_size);
  ebr_try_advance();

  return true;
}

void Deque::cleanup() noexcept {
  int64_t num_active_processes{-1};
  if (registry_ != nullptr && queue_idx_ != -1) {
    registry_->live_mask_.fetch_and(~(1ULL << queue_idx_),
                                    std::memory_order_release);
    if (process_count_incremented_) {
      num_active_processes = registry_->num_active_processes_.fetch_sub(
          1, std::memory_order_acq_rel);
      process_count_incremented_ = false;
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

  if (num_active_processes == 1)
    shm_unlink(shm_name_.c_str());
}

void Deque::ebr_pin() noexcept {
  uint64_t epoch{};
  do {
    epoch = registry_->global_epoch_.load(std::memory_order_acquire);
    registry_->pinned_epochs_[queue_idx_].value_.store(
        epoch, std::memory_order_seq_cst);
  } while (registry_->global_epoch_.load(std::memory_order_acquire) != epoch);
}

void Deque::ebr_unpin() noexcept {
  registry_->pinned_epochs_[queue_idx_].value_.store(Registry::EBR_UNPINNED,
                                                     std::memory_order_release);
}

void Deque::ebr_retire(uint64_t offset, uint64_t size_bytes) noexcept {
  const uint64_t epoch{
      registry_->global_epoch_.load(std::memory_order_acquire)};
  retire_lists_[epoch % EBR_EPOCHS].push_back({offset, size_bytes});
}

bool Deque::ebr_try_advance() noexcept {
  uint64_t global{registry_->global_epoch_.load(std::memory_order_acquire)};
  uint64_t mask{registry_->live_mask_.load(std::memory_order_acquire)};

  uint64_t temp{mask};
  while (temp) {
    const int idx = std::countr_zero(temp);
    temp &= temp - 1;

    const uint64_t pinned =
        registry_->pinned_epochs_[idx].value_.load(std::memory_order_acquire);
    if (pinned != Registry::EBR_UNPINNED && pinned != global)
      return false; // Someone is still in a critical section at an older
                    // epoch.
  }

  return registry_->global_epoch_.compare_exchange_strong(
      global, global + 1, std::memory_order_acq_rel, std::memory_order_relaxed);
}

void Deque::ebr_reclaim() noexcept {
  const uint64_t global{
      registry_->global_epoch_.load(std::memory_order_acquire)};
  if (global < 2)
    return; // Not enough epochs have elapsed yet.

  auto &list = retire_lists_[(global - 2) % EBR_EPOCHS];
  for (const auto &rb : list)
    free(rb.offset_, rb.size_bytes_);
  list.clear();
}

} // namespace moveitmoveit
