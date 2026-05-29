#include "deque.hpp"
#include "utils/valid_shm_name.hpp"

#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

Deque::Deque(std::string group_id, std::size_t default_size_mb)
    : shm_name_{valid_shm_name(group_id)} {
  shm_fd_ = shm_open(shm_name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);

  // The first deque in the group needs to open shared memory and establish
  // offset registry. Detect with flag O_EXCL.
  if (shm_fd_ != -1) {
    is_creator_ = true;
    segment_size_ = default_size_mb * 1024 * 1024;
    ftruncate(shm_fd_, segment_size_);
  } else if (errno == EEXIST) {
    is_creator_ = false;
    shm_fd_ = shm_open(shm_name_.c_str(), O_RDWR, 0666);
    if (shm_fd_ == -1)
      throw std::runtime_error("Failed to open existing shared memory.");

    // Wait for the creator to set the memory using ftruncate.
    struct stat statbuf;
    do {
      fstat(shm_fd_, &statbuf);
      if (statbuf.st_size == 0)
        usleep(1000);
    } while (statbuf.st_size == 0);

    segment_size_ = statbuf.st_size;
  } else {
    throw std::runtime_error("Critical error opening shared memory.");
  }

  base_address_ = mmap(nullptr, segment_size_, PROT_READ | PROT_WRITE,
                       MAP_SHARED, shm_fd_, 0);

  if (is_creator_) {
    registry_ = new (base_address_) Registry();
    registry_->global_segement_size_ = segment_size_;
    queue_idx_ = registry_->num_active_processes_.fetch_add(
        1, std::memory_order_relaxed);
  } else {
    registry_ = reinterpret_cast<Registry *>(base_address_);
    queue_idx_ = registry_->num_active_processes_.fetch_add(
        1, std::memory_order_relaxed);
  }
}
