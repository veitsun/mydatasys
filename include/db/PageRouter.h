#pragma once

#include <cstddef>

namespace mini_db {

// 页分布策略：决定某个页归属哪个 NUMA 节点。
class PageNodeSelector {
 public:
  virtual ~PageNodeSelector() = default;
  virtual int node_for_page(size_t page_id, int node_count) const = 0;
};

// 默认策略：按 page_id 做取模分片。
class ModuloPageSelector : public PageNodeSelector {
 public:
  int node_for_page(size_t page_id, int node_count) const override {
    if (node_count <= 0) {
      return 0;
    }
    return static_cast<int>(page_id % static_cast<size_t>(node_count));
  }
};

}  // namespace mini_db
