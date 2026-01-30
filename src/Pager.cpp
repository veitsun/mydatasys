#include "db/Pager.h"

#include <cstring>
#include <sys/stat.h>

namespace mini_db {

Pager::Pager(const std::string& path, size_t page_size) : path_(path), page_size_(page_size) {
  // 构造时尝试打开或创建文件。
  std::string err;
  open_ = open_file(&err);
}

bool Pager::open_file(std::string* err) {
  // 以读写二进制方式打开文件，不存在则创建。
  file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
  if (!file_.is_open()) {
    std::ofstream create(path_, std::ios::out | std::ios::binary);
    create.close();
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
  }
  if (!file_.is_open()) {
    if (err) {
      *err = "failed to open file: " + path_;
    }
    return false;
  }
  return true;
}

bool Pager::is_open() const {
  return open_;
}

const std::string& Pager::path() const {
  return path_;
}

size_t Pager::page_size() const {
  return page_size_;
}

size_t Pager::file_size() const {
  // 使用 stat 获取文件大小，失败视为 0。
  struct stat st;
  if (stat(path_.c_str(), &st) != 0) {
    return 0;
  }
  return static_cast<size_t>(st.st_size);
}

bool Pager::read_page(size_t page_id, char* out, size_t size, std::string* err) {
  // 读取指定页，不足部分用 0 填充。
  if (!open_) {
    if (err) {
      *err = "pager not open";
    }
    return false;
  }
  if (!out) {
    if (err) {
      *err = "output buffer missing";
    }
    return false;
  }
  if (size != page_size_) {
    if (err) {
      *err = "page size mismatch";
    }
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  std::memset(out, 0, page_size_);
  size_t offset = page_id * page_size_;
  size_t file_bytes = file_size();
  if (offset >= file_bytes) {
    // 读取超出文件末尾时返回全 0 页。
    return true;
  }
  file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  file_.read(out, static_cast<std::streamsize>(page_size_));
  std::streamsize read_bytes = file_.gcount();
  if (read_bytes < 0) {
    if (err) {
      *err = "failed to read page";
    }
    return false;
  }
  if (static_cast<size_t>(read_bytes) < page_size_) {
    // 读到文件末尾时补 0，确保页大小一致。
    std::memset(out + read_bytes, 0, page_size_ - static_cast<size_t>(read_bytes));
  }
  return true;
}

bool Pager::write_page(size_t page_id, const char* data, size_t size, std::string* err) {
  // 将整页写入指定偏移位置。
  if (!open_) {
    if (err) {
      *err = "pager not open";
    }
    return false;
  }
  if (size != page_size_) {
    if (err) {
      *err = "page size mismatch";
    }
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  size_t offset = page_id * page_size_;
  file_.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
  file_.write(data, static_cast<std::streamsize>(size));
  if (!file_) {
    if (err) {
      *err = "failed to write page";
    }
    return false;
  }
  return true;
}

void Pager::flush() {
  if (open_) {
    std::lock_guard<std::mutex> lock(mutex_);
    file_.flush();
  }
}

}  // namespace mini_db
