#include "db/Numa.h"

#include <cstdlib>
#include <cstring>
#include <cctype>

#ifdef HAVE_LIBNUMA
#include <numa.h>
#include <sched.h>
#else
#include <sched.h>
#endif

namespace mini_db {

namespace {

class FallbackTopology : public NumaTopology {
 public:
  explicit FallbackTopology(int nodes) : nodes_(nodes > 0 ? nodes : 1) {}

  int node_count() const override { return nodes_; }

  int current_node() const override {
    // 无 NUMA 时退化为按 CPU 取模的“伪节点”。
    int cpu = sched_getcpu();
    if (cpu < 0) {
      return 0;
    }
    return cpu % nodes_;
  }

 private:
  int nodes_ = 1;
};

class FallbackAllocator : public NumaAllocator {
 public:
  char* allocate(size_t size, int /*node*/) override {
    // 退化为普通 malloc。
    return static_cast<char*>(std::malloc(size));
  }

  void deallocate(char* ptr, size_t /*size*/) override { std::free(ptr); }
};

#ifdef HAVE_LIBNUMA
class LibNumaTopology : public NumaTopology {
 public:
  explicit LibNumaTopology(int preferred_nodes) : preferred_nodes_(preferred_nodes) {}

  int node_count() const override {
    // 优先使用用户配置的节点数，但不超过系统配置节点数。
    int configured = numa_num_configured_nodes();
    if (preferred_nodes_ > 0 && preferred_nodes_ < configured) {
      return preferred_nodes_;
    }
    return configured > 0 ? configured : 1;
  }

  int current_node() const override {
    // 使用 libnuma 查询当前 CPU 所属节点。
    int cpu = sched_getcpu();
    if (cpu < 0) {
      return 0;
    }
    int node = numa_node_of_cpu(cpu);
    if (node < 0) {
      return 0;
    }
    int count = node_count();
    return node % count;
  }

 private:
  int preferred_nodes_ = 0;
};

/**
 * @brief LibNumaAllocator 是一个基于 libnuma 的 NUMA 感知内存分配器，用来在指定的 NUAM 节点上分配和释放内存，从而提升多 CPU ，多节点系统下的内存访问性能。
 * 
 */
class LibNumaAllocator : public NumaAllocator {
 public:
  char* allocate(size_t size, int node) override {
    // 在指定节点分配内存，未必保证严格绑定（由系统策略决定）。
    void* ptr = numa_alloc_onnode(size, node);
    return static_cast<char*>(ptr);
  }

  void deallocate(char* ptr, size_t size) override {
    if (ptr) {
      numa_free(ptr, size);
    }
  }
};
#endif

int read_env_nodes() {
  // 读取环境变量 MINI_DB_NUMA_NODES 作为默认节点数。
  const char* env = std::getenv("MINI_DB_NUMA_NODES");
  if (!env || std::strlen(env) == 0) {
    return 0;
  }
  char* end = nullptr;
  long value = std::strtol(env, &end, 10);
  if (end == env || value <= 0) {
    return 0;
  }
  return static_cast<int>(value);
}

}  // namespace

bool is_numa_enabled() {
  // 环境变量 MINI_DB_ENABLE_NUMA=0/false/off 可关闭 NUMA 优化。
  const char* env = std::getenv("MINI_DB_ENABLE_NUMA");
  if (!env || std::strlen(env) == 0) {
    return true;
  }
  std::string value(env);
  for (auto& ch : value) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  if (value == "0" || value == "false" || value == "off") {
    return false;
  }
  return true;
}

int forced_numa_alloc_node() {
  // 环境变量 MINI_DB_NUMA_ALLOC_NODE 可指定分配的目标节点（-1 表示不强制）。
  const char* env = std::getenv("MINI_DB_NUMA_ALLOC_NODE");
  if (!env || std::strlen(env) == 0) {
    return -1;
  }
  char* end = nullptr;
  long value = std::strtol(env, &end, 10);
  if (end == env || value < 0) {
    return -1;
  }
  return static_cast<int>(value);
}

/**
 * @brief 根据运行环境是否支持 NUMA，创建一个合适的 NUMA 拓扑对象，并在不支持时自动降级
 * 
 * @param preferred_nodes 
 * @return std::unique_ptr<NumaTopology> 
 */
std::unique_ptr<NumaTopology> create_numa_topology(int preferred_nodes) {
  int env_nodes = read_env_nodes();
  int final_nodes = preferred_nodes > 0 ? preferred_nodes : env_nodes;
#ifdef HAVE_LIBNUMA
  if (!is_numa_enabled()) {
    return std::make_unique<FallbackTopology>(final_nodes > 0 ? final_nodes : 1);
  }
#endif
#ifdef HAVE_LIBNUMA
  if (numa_available() >= 0) {
    return std::make_unique<LibNumaTopology>(final_nodes);
  }
#endif
  return std::make_unique<FallbackTopology>(final_nodes > 0 ? final_nodes : 1);
}

std::unique_ptr<NumaAllocator> create_numa_allocator() {
#ifdef HAVE_LIBNUMA
  if (is_numa_enabled() && numa_available() >= 0) {
    return std::make_unique<LibNumaAllocator>();
  }
  if (!is_numa_enabled() && forced_numa_alloc_node() >= 0 && numa_available() >= 0) {
    return std::make_unique<LibNumaAllocator>();
  }
#endif
  return std::make_unique<FallbackAllocator>();
}

}  // namespace mini_db
