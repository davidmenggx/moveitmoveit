#include "deque.hpp"
#include "utils/calculate_bin.hpp"
#include "utils/valid_shm_name.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
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
  // offset registry. Detect with flag O_EXCL.
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
    if (shm_fd_ == -1)
      throw std::runtime_error(
          "CRITICAL: Failed to open existing shared memory.");

    // Wait for the creator to set the memory using ftruncate.
    struct stat statbuf{};
    for (int i{0}; statbuf.st_size == 0; ++i) {
      if (i >= MAX_FTRUNCATE_TRIES) {
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

  if (is_creator_) {
    registry_ = new (base_address_) Registry();
    registry_->global_segement_size_.store(segment_size_,
                                           std::memory_order_relaxed);
    queue_idx_ = registry_->num_active_processes_.fetch_add(
        1, std::memory_order_relaxed);
    registry_->initialized_flag_.store(1, std::memory_order_release);
  } else {
    Registry *temp_reg{reinterpret_cast<Registry *>(base_address_)};
    while (registry_->initialized_flag_.load(std::memory_order_acquire) == 0)
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    registry_ = temp_reg;
    queue_idx_ = registry_->num_active_processes_.fetch_add(
        1, std::memory_order_relaxed);
  }

  if (queue_idx_ >= MAX_BUFFERS) {
    cleanup();
    throw std::runtime_error("CRITICAL: Deque group is full (" +
                             std::to_string(MAX_BUFFERS) + " max).");
  }

  const uint64_t buf_offset{registry_->unallocated_memory_top_.fetch_add(
      sizeof(Buffer), std::memory_order_relaxed)};
  registry_->buffer_offsets_[queue_idx_] = buf_offset;
  my_buffer_ = new (offset_to_ptr<void>(buf_offset)) Buffer();

  const uint64_t initial_capacity{
      1 << MIN_FREE_LIST_BIN_SIZE}; // Must be a power of 2
  const uint64_t initial_buffer_offset{
      allocate(initial_capacity * sizeof(ObjectDescriptor))};

  if (initial_buffer_offset == NULL_OFFSET) {
    cleanup();
    throw std::runtime_error(
        "CRITICAL: Initial buffer allocation failed, out of memory.");
  }

  my_buffer_->buffer_capacity_.store(initial_capacity,
                                     std::memory_order_relaxed);
  my_buffer_->buffer_offset_.store(initial_buffer_offset,
                                   std::memory_order_relaxed);

  rng_.seed(getpid());
}

Deque::~Deque() noexcept {
  const int64_t remaining{
      registry_->num_active_processes_.fetch_sub(1, std::memory_order_acq_rel) -
      1};

  munmap(base_address_, segment_size_);
  close(shm_fd_);

  if (remaining == 0)
    shm_unlink(shm_name_.c_str());
}

[[nodiscard]] uint64_t Deque::allocate(uint64_t capacity) {
  const int bin_idx{
      calculate_bin(capacity, MIN_FREE_LIST_BIN_SIZE, NUM_FREE_LIST_BINS)};
  if (bin_idx < 0) [[unlikely]]
    return NULL_OFFSET;

  Registry::TaggedOffset head{
      registry_->free_lists_[bin_idx].head_.load(std::memory_order_acquire)};

  // CAS loop to find the next free chunk in the appropriate bin.
  while (head.offset_ != NULL_OFFSET) {
    const uint32_t next_offset{
        offset_to_ptr<std::atomic<uint32_t>>(head.offset_)
            ->load(std::memory_order_relaxed)};
    const Registry::TaggedOffset new_head{next_offset, head.aba_tag_ + 1};
    if (registry_->free_lists_[bin_idx].head_.compare_exchange_weak(
            head, new_head, std::memory_order_acquire,
            std::memory_order_acquire))
      return head.offset_;
  }

  // Fallback to bump allocation. Allocate the entire bin (not just capacity) so
  // it can be recycled.
  const uint64_t bytes_needed{1ULL << (MIN_FREE_LIST_BIN_SIZE + bin_idx)};

  uint64_t old_top{
      registry_->unallocated_memory_top_.load(std::memory_order_relaxed)};

  while (true) {
    uint64_t alignment{CACHE_LINE_SIZE};
    // Align up to maintain powers of 2
    const uint64_t aligned_top{(old_top + alignment - 1) & ~(alignment - 1)};
    const uint64_t new_top{aligned_top + bytes_needed};

    if (new_top > registry_->global_segement_size_.load(
                      std::memory_order_relaxed)) [[unlikely]]
      return NULL_OFFSET; // Out of memory

    if (registry_->unallocated_memory_top_.compare_exchange_weak(
            old_top, new_top, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      return old_top;
    }
  }
}

void Deque::cleanup() noexcept {
  if (registry_ != nullptr && queue_idx_ != -1) {
    registry_->num_active_processes_.fetch_sub(1, std::memory_order_relaxed);
    queue_idx_ = -1;
  }

  if (base_address_ != nullptr && base_address_ != MAP_FAILED) {
    munmap(base_address_, segment_size_);
    base_address_ = nullptr;
  }

  if (shm_fd_ != -1) {
    close(shm_fd_);
    shm_fd_ = -1;
  }

  if (is_creator_) {
    shm_unlink(shm_name_.c_str());
    is_creator_ = false;
  }
}

} // namespace moveitmoveit
