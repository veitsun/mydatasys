#include "db/Cache.h"

namespace mini_db {

PageCache::PageCache(Pager* pager, size_t capacity, size_t page_size)
    : pager_(pager), capacity_(capacity), page_size_(page_size) {}

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
      if (!pager_->write_page(victim_id, it->second.page.data, err)) {
        return false;
      }
    }
    lru_.pop_back();
    pages_.erase(it);
  }
  return true;
}

Page* PageCache::get_page(size_t page_id, std::string* err) {
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
  Entry entry;
  entry.page.id = page_id;
  entry.page.data.resize(page_size_, 0);
  entry.page.dirty = false;
  if (!pager_->read_page(page_id, &entry.page.data, err)) {
    return nullptr;
  }
  lru_.push_front(page_id);
  entry.lru_it = lru_.begin();
  auto result = pages_.emplace(page_id, std::move(entry));
  return &result.first->second.page;
}

void PageCache::mark_dirty(size_t page_id) {
  // 标记脏页，flush 时写回。
  auto it = pages_.find(page_id);
  if (it != pages_.end()) {
    it->second.page.dirty = true;
  }
}

void PageCache::flush(std::string* err) {
  // 写回所有脏页并刷新底层文件。
  for (auto& pair : pages_) {
    if (pair.second.page.dirty) {
      if (!pager_->write_page(pair.first, pair.second.page.data, err)) {
        return;
      }
      pair.second.page.dirty = false;
    }
  }
  pager_->flush();
}

}  // namespace mini_db
