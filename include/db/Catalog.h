#pragma once

#include "db/Schema.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace mini_db {

// Catalog 管理器：保存表名与 Schema 的映射，并持久化到元数据文件。
class Catalog {
 public:
  // path 为 catalog 文件路径（例如 catalog.meta）。
  explicit Catalog(const std::string& path);

  // 从磁盘加载表结构元数据。
  bool load(std::string* err);
  // 保存当前所有表结构元数据到磁盘。
  bool save(std::string* err) const;

  // 创建表结构记录。
  bool create_table(const std::string& name, const Schema& schema, std::string* err);
  // 删除表结构记录。
  bool drop_table(const std::string& name, std::string* err);
  // 为表新增列（仅修改元数据）。
  bool alter_add_column(const std::string& name, const Column& column, std::string* err);
  // 获取指定表结构。
  bool get_schema(const std::string& name, Schema* schema) const;
  // 列出所有表名。
  std::vector<std::string> list_tables() const;

 private:
  std::string path_;
  std::unordered_map<std::string, Schema> schemas_;
};

}  // namespace mini_db
