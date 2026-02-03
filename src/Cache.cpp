#include "db/Cache.h"

namespace mini_db {

PageCache::PageCache(Pager* pager, size_t capacity, size_t page_size, int node_id,
                     NumaAllocator* allocator)
    : pager_(pager),
      capacity_(capacity),
      page_size_(page_size),
      node_id_(node_id),
      allocator_(allocator) {}

bool PageCache::evict_if_needed(std::string* err) {
  // 缓存未满时无需淘汰。
  if (capacity_ == 0 || pages_.size() < capacity_) {
    return true;
  }
  // 淘汰 LRU 链表尾部（最久未使用）。
  size_t victim_id = lru_.back();
  auto it = pages_.find(victim_id);
  if (it != pages_.end()) {
    if (it->second.page.dirty) {
      // 脏页需要先写回。
      if (!pager_->write_page(victim_id, it->second.page.data.data(),
                              it->second.page.data.size(), err)) {
        return false;
      }
    }
    lru_.pop_back();
    pages_.erase(it);
  }
  return true;
}

Page* PageCache::get_page(size_t page_id, std::string* err) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 命中缓存：更新 LRU 顺序。
  auto it = pages_.find(page_id);
  if (it != pages_.end()) {
    lru_.erase(it->second.lru_it);
    lru_.push_front(page_id);
    it->second.lru_it = lru_.begin();
    return &it->second.page;
  }
  if (!evict_if_needed(err)) {
    return nullptr;
  }
  // 未命中：从磁盘加载新页。
  // 使用 NUMA 分配器在指定节点创建页缓冲区。
  Entry entry;
  entry.page.id = page_id;
  entry.page.data.reset(page_size_, node_id_, allocator_);
  entry.page.dirty = false;
  entry.page.numa_node = node_id_;
  if (!entry.page.data.data()) {
    if (err) {
      *err = "failed to allocate page buffer";
    }
    return nullptr;
  }
  if (!pager_->read_page(page_id, entry.page.data.data(), entry.page.data.size(), err)) {
    return nullptr;
  }
  lru_.push_front(page_id);
  entry.lru_it = lru_.begin();
  auto result = pages_.emplace(page_id, std::move(entry));
  return &result.first->second.page;
}

void PageCache::mark_dirty(size_t page_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 标记脏页，flush 时写回。
  auto it = pages_.find(page_id);
  if (it != pages_.end()) {
    it->second.page.dirty = true;
  }
}

void PageCache::flush(std::string* err) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 写回所有脏页并刷新底层文件。
  for (auto& pair : pages_) {
    if (pair.second.page.dirty) {
      if (!pager_->write_page(pair.first, pair.second.page.data.data(),
                              pair.second.page.data.size(), err)) {
        return;
      }
      pair.second.page.dirty = false;
    }
  }
  pager_->flush();
}

size_t PageCache::page_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return pages_.size();
}

}  // namespace mini_db
