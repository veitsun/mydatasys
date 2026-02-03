#pragma once

#include "db/BufferPool.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace mini_db {

// 逻辑数据项：用偏移和字节数组描述上层需要的数据块。
struct DataItem {
  size_t offset = 0;
  std::vector<char> data;
};

// 基于页缓存的文件读写封装，提供按偏移读写的 DataItem 抽象。
class PagedFile {
 public:
  // path 为文件路径，page_size 为页大小，cache_pages 为缓存页数，numa_nodes 为 NUMA 节点数。
  PagedFile(const std::string& path, size_t page_size, size_t cache_pages, int numa_nodes);

  // 从指定偏移读取 size 字节到 item。
  bool read_item(size_t offset, size_t size, DataItem* item, std::string* err);
  // 将 data 写入指定偏移位置（会跨页写入）。
  bool write_item(size_t offset, const std::vector<char>& data, std::string* err);
  // 刷新缓存与底层文件。
  void flush(std::string* err);
  // 重新绑定到新的文件路径或页配置（用于 schema 重建后替换文件）。
  void reset(const std::string& path, size_t page_size, size_t cache_pages, int numa_nodes);

  // 返回页大小。
  size_t page_size() const;
  // 返回文件大小（字节）。
  size_t file_size() const;
  // 返回文件路径。
  const std::string& path() const;
  // 返回每个 NUMA 节点当前缓存页数。
  std::vector<size_t> cached_pages_per_node() const;

 private:
  // PagedFile 是上层封装，用 Pager + NumaBufferPool 提供按偏移读写数据项的接口。
  std::unique_ptr<Pager> pager_;
  std::unique_ptr<NumaBufferPool> cache_;
};

}  // namespace mini_db
