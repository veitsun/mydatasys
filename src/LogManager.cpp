#include "db/LogManager.h"

#include "db/Utils.h"

#include <fstream>
#include <sstream>

namespace mini_db {

LogManager::LogManager(const std::string& path) : path_(path) {}

bool LogManager::append(const std::string& op, const std::string& table, uint64_t row_id,
                        const std::vector<char>& data, std::string* err) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 日志格式：LSN|OP|TABLE|ROW_ID|HEX(DATA)
  std::ofstream file(path_, std::ios::app | std::ios::out);
  if (!file.is_open()) {
    if (err) {
      *err = "failed to open log file";
    }
    return false;
  }
  std::string hex = hex_encode(data);
  file << next_lsn_++ << "|" << op << "|" << table << "|" << row_id << "|" << hex << "\n";
  file.flush();
  if (!file) {
    if (err) {
      *err = "failed to write log entry";
    }
    return false;
  }
  return true;
}

bool LogManager::read_all(std::vector<LogEntry>* entries, std::string* err) const {
  std::lock_guard<std::mutex> lock(mutex_);
  // 顺序读取日志文件并解析。
  if (!entries) {
    if (err) {
      *err = "entries output missing";
    }
    return false;
  }
  entries->clear();
  std::ifstream file(path_);
  if (!file.is_open()) {
    return true;
  }
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    // 以 '|' 分隔字段，解析失败则忽略该行。
    std::vector<std::string> parts;
    std::stringstream ss(line);
    std::string segment;
    while (std::getline(ss, segment, '|')) {
      parts.push_back(segment);
    }
    if (parts.size() < 5) {
      continue;
    }
    LogEntry entry;
    try {
      entry.lsn = std::stoull(parts[0]);
      entry.op = parts[1];
      entry.table = parts[2];
      entry.row_id = std::stoull(parts[3]);
    } catch (...) {
      continue;
    }
    std::vector<char> data;
    if (!hex_decode(parts[4], &data)) {
      continue;
    }
    entry.data = std::move(data);
    entries->push_back(std::move(entry));
  }
  return true;
}

void LogManager::clear(std::string* err) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 截断日志文件，用于检查点之后清理。
  std::ofstream file(path_, std::ios::out | std::ios::trunc);
  if (!file.is_open()) {
    if (err) {
      *err = "failed to truncate log";
    }
    return;
  }
}

}  // namespace mini_db
