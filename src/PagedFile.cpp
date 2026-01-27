#include "db/PagedFile.h"

#include <cstring>

namespace mini_db {

PagedFile::PagedFile(const std::string& path, size_t page_size, size_t cache_pages)
    : pager_(path, page_size), cache_(&pager_, cache_pages, page_size) {}

bool PagedFile::read_item(size_t offset, size_t size, DataItem* item, std::string* err) {
  // 按偏移跨页读取，拼接为连续的数据块。
  if (!item) {
    if (err) {
      *err = "data item missing";
    }
    return false;
  }
  item->offset = offset;
  item->data.assign(size, 0);
  size_t remaining = size;
  size_t current_offset = offset;
  size_t dest_offset = 0;
  while (remaining > 0) {
    size_t page_id = current_offset / pager_.page_size();
    size_t page_offset = current_offset % pager_.page_size();
    size_t chunk = pager_.page_size() - page_offset;
    if (chunk > remaining) {
      chunk = remaining;
    }
    Page* page = cache_.get_page(page_id, err);
    if (!page) {
      return false;
    }
    // 复制当前页片段到输出缓冲。
    std::memcpy(item->data.data() + dest_offset, page->data.data() + page_offset, chunk);
    current_offset += chunk;
    dest_offset += chunk;
    remaining -= chunk;
  }
  return true;
}

bool PagedFile::write_item(size_t offset, const std::vector<char>& data, std::string* err) {
  // 按偏移跨页写入，逐页写回到缓存。
  size_t remaining = data.size();
  size_t current_offset = offset;
  size_t src_offset = 0;
  while (remaining > 0) {
    size_t page_id = current_offset / pager_.page_size();
    size_t page_offset = current_offset % pager_.page_size();
    size_t chunk = pager_.page_size() - page_offset;
    if (chunk > remaining) {
      chunk = remaining;
    }
    Page* page = cache_.get_page(page_id, err);
    if (!page) {
      return false;
    }
    // 覆盖页内指定区间并标记脏页。
    std::memcpy(page->data.data() + page_offset, data.data() + src_offset, chunk);
    cache_.mark_dirty(page_id);
    current_offset += chunk;
    src_offset += chunk;
    remaining -= chunk;
  }
  return true;
}

void PagedFile::flush(std::string* err) {
  // 将缓存中的脏页写回磁盘。
  cache_.flush(err);
}

void PagedFile::reset(const std::string& path, size_t page_size, size_t cache_pages) {
  // 重新初始化底层 pager 与 cache，用于切换文件。
  pager_ = Pager(path, page_size);
  cache_ = PageCache(&pager_, cache_pages, page_size);
}

size_t PagedFile::page_size() const {
  return pager_.page_size();
}

size_t PagedFile::file_size() const {
  return pager_.file_size();
}

const std::string& PagedFile::path() const {
  return pager_.path();
}

}  // namespace mini_db
