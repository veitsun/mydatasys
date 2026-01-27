#pragma once

#include "db/Pager.h"

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini_db {

// 单页缓存结构，包含页号、数据与脏标记。
struct Page {
  size_t id = 0;
  std::vector<char> data;
  bool dirty = false;
};

// 简易 LRU 页缓存，负责与 Pager 协作提升访问性能。
class PageCache {
 public:
  // capacity 为最大缓存页数，page_size 为每页字节大小。
  PageCache(Pager* pager, size_t capacity, size_t page_size);

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
  std::list<size_t> lru_;
  std::unordered_map<size_t, Entry> pages_;
};

}  // namespace mini_db
