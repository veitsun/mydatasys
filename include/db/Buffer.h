#pragma once

#include "db/Numa.h"

#include <cstddef>

namespace mini_db {

// 轻量页缓冲区：使用 NUMA 分配器在指定节点分配内存。
// 单个页的内存缓冲区，负责按 NUMA 节点分配/释放内存。Page 持有一个 Buffer 对象
class Buffer {
 public:
  Buffer() = default;
  Buffer(size_t size, int node, NumaAllocator* allocator);
  Buffer(const Buffer&) = delete;
  Buffer& operator=(const Buffer&) = delete;
  Buffer(Buffer&& other) noexcept;
  Buffer& operator=(Buffer&& other) noexcept;
  ~Buffer();

  char* data();
  const char* data() const;
  size_t size() const;
  int node() const;

  // 重新分配缓冲区（原内存会释放）。
  void reset(size_t size, int node, NumaAllocator* allocator);
  // 清零缓冲区内容。
  void zero();

 private:
  void release();

  char* data_ = nullptr;
  size_t size_ = 0;
  int node_ = -1;
  NumaAllocator* allocator_ = nullptr;
};

}  // namespace mini_db
