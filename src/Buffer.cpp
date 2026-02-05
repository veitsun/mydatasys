#include "db/Buffer.h"

#include <cstring>

namespace mini_db {

Buffer::Buffer(size_t size, int node, NumaAllocator* allocator) {
  // 构造时立即分配内存。
  reset(size, node, allocator);
}

Buffer::Buffer(Buffer&& other) noexcept {
  // 移动构造：接管缓冲区所有权。
  data_ = other.data_;
  size_ = other.size_;
  node_ = other.node_;
  allocator_ = other.allocator_;
  other.data_ = nullptr;
  other.size_ = 0;
  other.node_ = -1;
  other.allocator_ = nullptr;
}

Buffer& Buffer::operator=(Buffer&& other) noexcept {
  if (this != &other) {
    // 移动赋值：释放旧资源并接管新资源。
    release();
    data_ = other.data_;
    size_ = other.size_;
    node_ = other.node_;
    allocator_ = other.allocator_;
    other.data_ = nullptr;
    other.size_ = 0;
    other.node_ = -1;
    other.allocator_ = nullptr;
  }
  return *this;
}

Buffer::~Buffer() {
  // 析构时释放内存。
  release();
}

char* Buffer::data() {
  return data_;
}

const char* Buffer::data() const {
  return data_;
}

size_t Buffer::size() const {
  return size_;
}

int Buffer::node() const {
  return node_;
}

// 用来重新分配一块缓冲区，并把内容清空，同时维护 Buffer 对象内部的状态字段（指针，大小，NUMA 节点，分配器）
void Buffer::reset(size_t size, int node, NumaAllocator* allocator) {
  // 重新分配缓冲区并清零。
  // size 希望分配的新缓冲区大小（字节数）
  // node NUMA 节点编号（告诉分配器尽量在该 numa node 上分配内存）
  // allocator 用于分配内存的分配器对象指针
  release();  // 释放旧资源
  if (!allocator || size == 0) {
    data_ = nullptr;
    size_ = 0;
    node_ = -1;
    allocator_ = allocator;
    return;
  }
  // 参数有效，记录分配策略并执行分配
  allocator_ = allocator;
  node_ = node;
  size_ = size;
  data_ = allocator_->allocate(size_, node_);
  if (!data_) {
    // 分配失败，回滚部分状态并返回
    size_ = 0;
    node_ = -1;
    return;
  }
  // 把新分配的缓冲区全部写成 0
  std::memset(data_, 0, size_);
}

void Buffer::zero() {
  if (data_ && size_ > 0) {
    std::memset(data_, 0, size_);
  }
}

void Buffer::release() {
  if (data_ && allocator_) {
    allocator_->deallocate(data_, size_);
  }
  data_ = nullptr;
  size_ = 0;
  node_ = -1;
  allocator_ = nullptr;
}

}  // namespace mini_db
