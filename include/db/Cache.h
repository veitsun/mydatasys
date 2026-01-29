#pragma once

#include "db/Buffer.h"
#include "db/Pager.h"

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini_db {

// 单页缓存结构，包含页号、数据与脏标记。每个 Page 用一个 Buffer 来管理内存。
struct Page {
  size_t id = 0;
  // 页数据缓冲区，使用 NUMA 分配器按节点分配。
  Buffer data;
  bool dirty = false;
  int numa_node = -1;
};

// 简易 LRU 页缓存，负责与 Pager 协作提升访问性能。单个缓存分片，内部每个缓存页是 Page
class PageCache {
 public:
  // capacity 为最大缓存页数，page_size 为每页字节大小。
  // node_id 为该缓存分片所属 NUMA 节点，allocator 负责在该节点分配内存。
  PageCache(Pager* pager, size_t capacity, size_t page_size, int node_id,
            NumaAllocator* allocator);

  // 获取指定页；若缓存未命中则从磁盘加载并可能触发淘汰。
  Page* get_page(size_t page_id, std::string* err);
  // 标记页为脏页，flush 时会写回磁盘。
  void mark_dirty(size_t page_id);
  // 刷新所有脏页到磁盘。
  void flush(std::string* err);

 private:
  // 缓存条目：页对象 + LRU 链表迭代器。
  struct Entry {
    Page page;
    std::list<size_t>::iterator lru_it;
  };

  // 如果缓存已满则淘汰最久未使用页。
  bool evict_if_needed(std::string* err);

  Pager* pager_ = nullptr;
  size_t capacity_ = 0;
  size_t page_size_ = 0;
  int node_id_ = 0;                         // 对应的是哪个 numa node， 一个 PageCache 对象对应一个分片，一个 numa node 拥有一个分片
  NumaAllocator* allocator_ = nullptr;
  std::list<size_t> lru_;
  std::unordered_map<size_t, Entry> pages_;
};

}  // namespace mini_db
