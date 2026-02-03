#pragma once

#include "db/Cache.h"
#include "db/Numa.h"
#include "db/PageRouter.h"

#include <memory>
#include <vector>

namespace mini_db {

// NUMA 感知的 BufferPool：将缓存分片到不同节点。整个缓冲池，内部按 NUMA 节点分片成多个 PageCache
class NumaBufferPool {
 public:
  // preferred_nodes 可指定节点数量（0 表示自动探测或使用环境变量）。
  NumaBufferPool(Pager* pager, size_t capacity, size_t page_size, int preferred_nodes);

  // 获取页缓存（根据页归属节点路由到对应分片）。
  Page* get_page(size_t page_id, std::string* err);
  // 标记脏页。
  void mark_dirty(size_t page_id);
  // 刷新所有分片中的脏页。
  void flush(std::string* err);

  // 返回当前可用的 NUMA 节点数。
  int node_count() const;
  // 返回每个 NUMA 节点缓存中的页数量。
  std::vector<size_t> cached_pages_per_node() const;

 private:
  // 根据页号选择其所属分片。
  PageCache& shard_for_page(size_t page_id);

  std::unique_ptr<NumaTopology> topology_;
  std::unique_ptr<NumaAllocator> allocator_;
  std::unique_ptr<PageNodeSelector> selector_;
  std::vector<std::unique_ptr<PageCache>> shards_;
  size_t page_size_ = 0;
};

}  // namespace mini_db
