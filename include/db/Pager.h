#pragma once

#include <cstddef>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

namespace mini_db {

// 页式文件访问器：按固定页大小读写磁盘文件。
class Pager {
 public:
  // path 为文件路径，page_size 为页大小（字节）。
  Pager(const std::string& path, size_t page_size);

  // 返回文件是否打开成功。
  bool is_open() const;
  // 返回文件路径。
  const std::string& path() const;
  // 返回页大小。
  size_t page_size() const;

  // 读取指定页到 out（size 必须等于 page_size）。
  bool read_page(size_t page_id, char* out, size_t size, std::string* err);
  // 将 data 写入指定页（size 必须等于 page_size）。
  bool write_page(size_t page_id, const char* data, size_t size, std::string* err);
  // 刷新文件缓冲区。
  void flush();
  // 返回文件当前大小（字节）。
  size_t file_size() const;

 private:
  // 打开文件；不存在则创建。
  bool open_file(std::string* err);

  std::string path_;
  size_t page_size_ = 0;
  std::fstream file_;
  bool open_ = false;
  mutable std::mutex mutex_;
};

}  // namespace mini_db
