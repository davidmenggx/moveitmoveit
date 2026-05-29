#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>

#if defined(__cpp_lib_hardware_interference_size)
constexpr std::size_t CACHE_LINE_SIZE =
    std::hardware_destructive_interference_size;
#else
constexpr std::size_t CACHE_LINE_SIZE = 64;
#endif

constexpr int64_t MAX_BUFFERS = 64;

// Metadata for deque objects to support type-agnostic-ness.
struct ObjectDescriptor {
  uint64_t offset_{0};
  uint32_t size_{0};
  uint32_t padding_{0};
};

// TODO: Consider a more performant implementation for single type.

constexpr ObjectDescriptor EMPTY{0, 0, 0};
constexpr ObjectDescriptor ABORT{0, 0, 0};

// Structure and algorithms inspired by David Chase and Yossi Lev, 2005.
struct alignas(CACHE_LINE_SIZE) Buffer {
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> top_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> bottom_{0};
  uint64_t buffer_capacity_{0}; // Must be a power of 2
  uint64_t buffer_offset_{0};
};

// Contains memory offsets for each of the buffers.
struct Registry {
  std::atomic<int64_t> num_active_processes_{0};
  uint64_t global_segement_size_{0};
  alignas(CACHE_LINE_SIZE) uint64_t buffer_offsets_[MAX_BUFFERS];
};

class Deque {
public:
  Deque(std::string group_id, std::size_t default_size_mb = 100);
  ~Deque();

  Deque(const Deque &other) = delete;
  Deque &operator=(const Deque &other) = delete;
  Deque(Deque &&other) = delete;
  Deque &operator=(Deque &&other) = delete;

  // Structure and algorithms inspired by David Chase and Yossi Lev, 2005.

  void push(const char *serialized_data, std::size_t size) noexcept;

  // Never allocates memory, returns false if capacity is reached.
  bool try_push(const char *serialized_data, std::size_t size) noexcept;

  [[nodiscard]] ObjectDescriptor pop() noexcept;
  [[nodiscard]] ObjectDescriptor steal() noexcept;

private:
  void grow(int64_t current_capacity);

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

  std::string shm_name_{};
  int shm_fd_{-1};
  std::size_t segment_size_{0};
  void *base_address_{nullptr};

  // Process local offsets
  Registry *registry_{nullptr};
  Buffer *my_buffer_{nullptr};
  int64_t queue_idx_{-1};
  bool is_creator_{false};
};
