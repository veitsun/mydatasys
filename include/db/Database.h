#pragma once

#include "db/Catalog.h"
#include "db/LogManager.h"
#include "db/TableStorage.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini_db {

// 数据库入口：管理 Catalog、日志与多个表实例，提供 DDL/DML 统一接口。
class Database {
 public:
  // base_dir 为数据目录，page_size/ cache_pages 用于表文件与缓存配置。
  Database(const std::string& base_dir, size_t page_size, size_t cache_pages);

  // 打开数据库（加载 catalog、表文件，并做恢复）。
  bool open(std::string* err);
  // 关闭数据库（执行检查点与刷盘）。
  void close(std::string* err);

  // DDL：创建/删除表、添加列。
  bool create_table(const std::string& name, const std::vector<Column>& columns, std::string* err);
  bool drop_table(const std::string& name, std::string* err);
  bool alter_add_column(const std::string& name, const Column& column, std::string* err);

  // DML：插入、查询、更新、删除。
  bool insert(const std::string& table, const std::vector<Value>& values, uint64_t* row_id,
              std::string* err);
  bool select(const std::string& table, const Condition& where,
              std::vector<std::vector<Value>>* rows, std::string* err);
  bool update(const std::string& table, const std::vector<SetClause>& sets, const Condition& where,
              size_t* updated, std::string* err);
  bool remove(const std::string& table, const Condition& where, size_t* removed, std::string* err);

  // 获取表结构与表列表。
  bool get_schema(const std::string& table, Schema* schema, std::string* err) const;
  std::vector<std::string> list_tables() const;

 private:
  // 拼接表文件路径。
  std::string table_path(const std::string& name) const;
  // 获取已加载表的存储对象。
  TableStorage* get_table(const std::string& name);
  // 加载所有表实例。
  bool load_tables(std::string* err);
  // 根据日志做 redo 恢复。
  bool recover(std::string* err);
  // 检查点：刷盘并清理日志。
  void checkpoint(std::string* err);

  std::string base_dir_;
  size_t page_size_ = 0;
  size_t cache_pages_ = 0;
  Catalog catalog_;
  LogManager log_;
  std::unordered_map<std::string, std::unique_ptr<TableStorage>> tables_;
};

}  // namespace mini_db
