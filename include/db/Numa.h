#pragma once

#include <cstddef>
#include <memory>

namespace mini_db {

// NUMA 拓扑抽象：提供节点数量与当前线程所在节点。
class NumaTopology {
 public:
  virtual ~NumaTopology() = default;
  virtual int node_count() const = 0;
  virtual int current_node() const = 0;
};

// NUMA 分配器抽象：允许按节点分配/释放内存。
class NumaAllocator {
 public:
  virtual ~NumaAllocator() = default;
  virtual char* allocate(size_t size, int node) = 0;
  virtual void deallocate(char* ptr, size_t size) = 0;
};

// 创建 NUMA 拓扑对象；preferred_nodes > 0 时优先使用该数量。
std::unique_ptr<NumaTopology> create_numa_topology(int preferred_nodes);
// 创建 NUMA 分配器（若系统不支持 NUMA，将退化为普通 malloc/free）。
std::unique_ptr<NumaAllocator> create_numa_allocator();

}  // namespace mini_db
