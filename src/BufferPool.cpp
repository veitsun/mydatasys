#include "db/BufferPool.h"

namespace mini_db {

NumaBufferPool::NumaBufferPool(Pager* pager, size_t capacity, size_t page_size,
                               int preferred_nodes)
    : topology_(create_numa_topology(preferred_nodes)),
      allocator_(create_numa_allocator()),
      selector_(std::make_unique<ModuloPageSelector>()),
      page_size_(page_size) {
  // 根据拓扑信息创建分片缓存；每个分片对应一个 NUMA 节点。
  int nodes = topology_ ? topology_->node_count() : 1;
  if (nodes <= 0) {
    nodes = 1;
  }
  if (nodes == 0) {
    nodes = 1;
  }
  // 简单平均拆分缓存容量，保证每个节点至少有 1 页缓存。
  size_t per_node = capacity;
  if (nodes > 1) {
    per_node = capacity / static_cast<size_t>(nodes);
    if (per_node == 0) {
      per_node = 1;
    }
  }
  shards_.reserve(static_cast<size_t>(nodes));
  for (int i = 0; i < nodes; ++i) {
    // 每个分片维护自身的 LRU 与页内存分配。
    auto shard = std::make_unique<PageCache>(pager, per_node, page_size, i, allocator_.get());
    shards_.push_back(std::move(shard));
  }
}

Page* NumaBufferPool::get_page(size_t page_id, std::string* err) {
  // 按页归属节点路由到对应缓存分片。
  PageCache& shard = shard_for_page(page_id);
  return shard.get_page(page_id, err);
}

void NumaBufferPool::mark_dirty(size_t page_id) {
  // 脏页标记路由到归属分片。
  PageCache& shard = shard_for_page(page_id);
  shard.mark_dirty(page_id);
}

void NumaBufferPool::flush(std::string* err) {
  // 逐分片刷新，保证所有脏页落盘。
  for (auto& shard : shards_) {
    shard->flush(err);
    if (err && !err->empty()) {
      return;
    }
  }
}

int NumaBufferPool::node_count() const {
  return topology_ ? topology_->node_count() : 1;
}

PageCache& NumaBufferPool::shard_for_page(size_t page_id) {
  // 按策略选择页所属节点。
  int nodes = node_count();
  int node = selector_ ? selector_->node_for_page(page_id, nodes) : 0;
  if (node < 0) {
    node = 0;
  }
  if (nodes <= 0) {
    node = 0;
  } else if (node >= nodes) {
    node = node % nodes;
  }
  return *shards_.at(static_cast<size_t>(node));
}

}  // namespace mini_db
