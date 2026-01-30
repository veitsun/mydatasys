#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace mini_db {

// 日志条目：用于崩溃恢复的最小 redo 信息。
struct LogEntry {
  uint64_t lsn = 0;
  std::string op;
  std::string table;
  uint64_t row_id = 0;
  std::vector<char> data;
};

// 简易日志管理器：顺序追加与读取，支持恢复与清空。
class LogManager {
 public:
  // path 为日志文件路径。
  explicit LogManager(const std::string& path);

  // 追加一条日志记录（op/表名/行号/记录数据）。
  bool append(const std::string& op, const std::string& table, uint64_t row_id,
              const std::vector<char>& data, std::string* err);
  // 读取全部日志记录，用于恢复。
  bool read_all(std::vector<LogEntry>* entries, std::string* err) const;
  // 清空日志文件（检查点之后调用）。
  void clear(std::string* err);

 private:
  std::string path_;
  uint64_t next_lsn_ = 1;
  mutable std::mutex mutex_;
};

}  // namespace mini_db
