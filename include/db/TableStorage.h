#pragma once

#include "db/LogManager.h"
#include "db/PagedFile.h"
#include "db/Schema.h"

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace mini_db {

// 单表存储引擎：负责表文件读写、记录管理、简单的增删改查与日志写入。
class TableStorage {
 public:
  // path 为表文件路径，name 为表名，schema 为表结构，numa_nodes 为 NUMA 节点数。
  TableStorage(const std::string& path, const std::string& name, const Schema& schema,
               size_t page_size, size_t cache_pages, int numa_nodes, LogManager* log);

  // 加载表文件（新建或读取头部与重建空闲列表）。
  bool load(std::string* err);
  // 返回表名。
  const std::string& name() const;
  // 返回表结构。
  const Schema& schema() const;
  // 返回当前记录数（包含已删除的逻辑行）。
  uint64_t row_count() const;

  // 插入一行，返回行号（row_id）。
  bool insert(const std::vector<Value>& values, uint64_t* row_id, std::string* err);
  // 查询：仅支持单列等值过滤。
  bool select(const Condition& where, std::vector<std::vector<Value>>* rows, std::string* err);
  // 更新：支持 SET 多列与可选 WHERE 条件。
  bool update(const std::vector<SetClause>& sets, const Condition& where, size_t* updated,
              std::string* err);
  // 删除：逻辑删除并加入空闲列表。
  bool remove(const Condition& where, size_t* removed, std::string* err);

  // 按行号读取记录（用于多线程按页路由场景）。
  bool read_row(uint64_t row_id, std::vector<Value>* values, bool* valid, std::string* err);
  // 按行号更新指定列（不会扫描全表）。
  bool update_row(uint64_t row_id, const std::vector<SetClause>& sets, std::string* err);
  // 按行号逻辑删除记录。
  bool delete_row(uint64_t row_id, std::string* err);
  // 按行号覆盖写入记录（valid=false 表示逻辑删除）。
  bool write_row(uint64_t row_id, const std::vector<Value>& values, bool valid, std::string* err);
  // 根据行号计算其所在页号。
  size_t page_id_for_row(uint64_t row_id) const;

  // 日志恢复时应用 redo 记录（覆盖指定 row_id）。
  bool apply_redo(uint64_t row_id, const std::vector<char>& record, std::string* err);
  // ALTER TABLE 后重建文件（根据新 schema 迁移数据）。
  bool rebuild_for_schema(const Schema& new_schema, std::string* err);
  // 扫描重建空闲列表（删除标记的行）。
  bool rebuild_free_list(std::string* err);
  // 刷新缓存与文件。
  void flush(std::string* err);

 private:
  // 表文件头部：魔数、记录大小、行数等元数据。
  struct Header {
    char magic[4];
    uint32_t record_size;
    uint64_t row_count;
    uint64_t reserved;
  };

  // 读取/写入表头。
  bool read_header(std::string* err);
  bool write_header(std::string* err);
  // 按 row_id 读取/写入记录（含有效标记）。
  bool read_record(uint64_t row_id, std::vector<char>* record, std::string* err);
  bool write_record(uint64_t row_id, const std::vector<char>& record, std::string* err);
  // 预留：用于生成新行号（当前实现直接使用 row_count_ / 空闲列表）。
  uint64_t allocate_row_id();
  // 计算记录在文件中的偏移。
  size_t record_offset(uint64_t row_id) const;
  // 根据页号选择锁分片，降低锁开销。
  std::mutex& page_lock(size_t page_id);

  std::string path_;
  std::string name_;
  Schema schema_;
  PagedFile file_;
  LogManager* log_ = nullptr;
  uint64_t row_count_ = 0;
  std::vector<uint64_t> free_list_;
  size_t page_size_ = 0;
  size_t cache_pages_ = 0;
  int numa_nodes_ = 1;
  mutable std::shared_mutex table_mutex_;
  std::mutex meta_mutex_;
  std::vector<std::mutex> page_mutexes_;

  static constexpr size_t kHeaderSize = 32;
  static constexpr size_t kPageLockStripes = 64;
};

}  // namespace mini_db
