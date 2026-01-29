#include "db/Database.h"

#include "db/Utils.h"

#include <cerrno>
#include <cstdio>
#include <sys/stat.h>

namespace mini_db {

namespace {

// 递归创建目录（支持简单路径如 ./data 或 data/sub）。
bool ensure_dir(const std::string& path, std::string* err) {
  if (path.empty()) {
    if (err) {
      *err = "data directory path is empty";
    }
    return false;
  }
  std::string current;
  for (size_t i = 0; i <= path.size(); ++i) {
    if (i == path.size() || path[i] == '/') {
      std::string part = path.substr(0, i);
      if (part.empty() || part == ".") {
        continue;
      }
      struct stat st;
      if (stat(part.c_str(), &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
          if (err) {
            *err = "path exists and is not a directory: " + part;
          }
          return false;
        }
      } else {
        // 子目录不存在则创建。
        if (mkdir(part.c_str(), 0755) != 0 && errno != EEXIST) {
          if (err) {
            *err = "failed to create directory: " + part;
          }
          return false;
        }
      }
    }
  }
  return true;
}

// 检查列名是否重复，避免 schema 冲突。
bool has_duplicate_columns(const std::vector<Column>& columns, std::string* err) {
  std::unordered_map<std::string, bool> seen;
  for (const auto& col : columns) {
    std::string key = to_lower(col.name);
    if (key.empty()) {
      if (err) {
        *err = "column name cannot be empty";
      }
      return true;
    }
    if (seen.find(key) != seen.end()) {
      if (err) {
        *err = "duplicate column name: " + col.name;
      }
      return true;
    }
    seen[key] = true;
  }
  return false;
}

}  // namespace

Database::Database(const std::string& base_dir, size_t page_size, size_t cache_pages,
                   int numa_nodes)
    : base_dir_(base_dir),
      page_size_(page_size),
      cache_pages_(cache_pages),
      numa_nodes_(numa_nodes),
      catalog_(base_dir + "/catalog.meta"),
      log_(base_dir + "/db.log") {}

bool Database::open(std::string* err) {
  // 打开数据库：创建目录 -> 读取 catalog -> 加载表 -> 日志恢复。
  if (!ensure_dir(base_dir_, err)) {
    return false;
  }
  if (!catalog_.load(err)) {
    return false;
  }
  if (!load_tables(err)) {
    return false;
  }
  return recover(err);
}

void Database::close(std::string* err) {
  // 关闭时执行检查点，清理日志。
  checkpoint(err);
}

bool Database::create_table(const std::string& name, const std::vector<Column>& columns,
                            std::string* err) {
  // 基础校验：列不能为空、列名不重复。
  if (columns.empty()) {
    if (err) {
      *err = "table must have at least one column";
    }
    return false;
  }
  if (has_duplicate_columns(columns, err)) {
    return false;
  }
  Schema schema(columns);
  if (!catalog_.create_table(name, schema, err)) {
    return false;
  }
  // 创建并加载表文件。
  std::string key = to_lower(name);
  auto table = std::make_unique<TableStorage>(table_path(key), key, schema, page_size_,
                                              cache_pages_, numa_nodes_, &log_);
  if (!table->load(err)) {
    return false;
  }
  tables_[key] = std::move(table);
  return true;
}

bool Database::drop_table(const std::string& name, std::string* err) {
  // 先删除 catalog 元数据，再删除表文件。
  std::string key = to_lower(name);
  if (!catalog_.drop_table(key, err)) {
    return false;
  }
  auto it = tables_.find(key);
  if (it != tables_.end()) {
    tables_.erase(it);
  }
  if (std::remove(table_path(key).c_str()) != 0 && errno != ENOENT) {
    if (err) {
      *err = "failed to remove table file";
    }
    return false;
  }
  return true;
}

bool Database::alter_add_column(const std::string& name, const Column& column, std::string* err) {
  // ALTER TABLE 需要先读取旧 schema，再做迁移。
  std::string key = to_lower(name);
  Schema schema;
  if (!catalog_.get_schema(key, &schema)) {
    if (err) {
      *err = "table not found: " + name;
    }
    return false;
  }
  std::vector<Column> cols = schema.columns();
  for (const auto& col : cols) {
    if (iequals(col.name, column.name)) {
      if (err) {
        *err = "column already exists: " + column.name;
      }
      return false;
    }
  }
  cols.push_back(column);
  Schema new_schema(cols);

  // 重建表文件以应用新结构。
  TableStorage* table = get_table(key);
  if (!table) {
    if (err) {
      *err = "table not loaded: " + name;
    }
    return false;
  }
  if (!table->rebuild_for_schema(new_schema, err)) {
    return false;
  }
  return catalog_.alter_add_column(key, column, err);
}

bool Database::insert(const std::string& table, const std::vector<Value>& values, uint64_t* row_id,
                      std::string* err) {
  // 插入后进行检查点，简化恢复逻辑。
  TableStorage* storage = get_table(table);
  if (!storage) {
    if (err) {
      *err = "table not found: " + table;
    }
    return false;
  }
  if (!storage->insert(values, row_id, err)) {
    return false;
  }
  checkpoint(err);
  return err ? err->empty() : true;
}

bool Database::select(const std::string& table, const Condition& where,
                      std::vector<std::vector<Value>>* rows, std::string* err) {
  TableStorage* storage = get_table(table);
  if (!storage) {
    if (err) {
      *err = "table not found: " + table;
    }
    return false;
  }
  return storage->select(where, rows, err);
}

bool Database::update(const std::string& table, const std::vector<SetClause>& sets,
                      const Condition& where, size_t* updated, std::string* err) {
  // 更新后进行检查点。
  TableStorage* storage = get_table(table);
  if (!storage) {
    if (err) {
      *err = "table not found: " + table;
    }
    return false;
  }
  if (!storage->update(sets, where, updated, err)) {
    return false;
  }
  checkpoint(err);
  return err ? err->empty() : true;
}

bool Database::remove(const std::string& table, const Condition& where, size_t* removed,
                      std::string* err) {
  // 删除后进行检查点。
  TableStorage* storage = get_table(table);
  if (!storage) {
    if (err) {
      *err = "table not found: " + table;
    }
    return false;
  }
  if (!storage->remove(where, removed, err)) {
    return false;
  }
  checkpoint(err);
  return err ? err->empty() : true;
}

bool Database::get_schema(const std::string& table, Schema* schema, std::string* err) const {
  if (!catalog_.get_schema(table, schema)) {
    if (err) {
      *err = "table not found: " + table;
    }
    return false;
  }
  return true;
}

std::vector<std::string> Database::list_tables() const {
  return catalog_.list_tables();
}

std::string Database::table_path(const std::string& name) const {
  // 统一表文件命名规则：<table>.tbl
  return base_dir_ + "/" + name + ".tbl";
}

TableStorage* Database::get_table(const std::string& name) {
  std::string key = to_lower(name);
  auto it = tables_.find(key);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.get();
}

bool Database::load_tables(std::string* err) {
  // 根据 catalog 加载所有表实例。
  tables_.clear();
  for (const auto& table_name : catalog_.list_tables()) {
    Schema schema;
    if (!catalog_.get_schema(table_name, &schema)) {
      continue;
    }
    auto table = std::make_unique<TableStorage>(table_path(table_name), table_name, schema,
                                                page_size_, cache_pages_, numa_nodes_, &log_);
    if (!table->load(err)) {
      return false;
    }
    tables_[table_name] = std::move(table);
  }
  return true;
}

bool Database::recover(std::string* err) {
  // 读取日志并重放所有记录，保证一致性。
  std::vector<LogEntry> entries;
  if (!log_.read_all(&entries, err)) {
    return false;
  }
  if (entries.empty()) {
    return true;
  }
  for (const auto& entry : entries) {
    // 逐条日志找到对应表并覆盖行数据。
    TableStorage* table = get_table(entry.table);
    if (!table) {
      if (err) {
        *err = "table missing during recovery: " + entry.table;
      }
      return false;
    }
    if (!table->apply_redo(entry.row_id, entry.data, err)) {
      return false;
    }
  }
  for (auto& pair : tables_) {
    // 恢复后重建空闲列表。
    if (!pair.second->rebuild_free_list(err)) {
      return false;
    }
  }
  log_.clear(err);
  return err ? err->empty() : true;
}

void Database::checkpoint(std::string* err) {
  // 刷新所有表并清空日志。
  for (auto& pair : tables_) {
    pair.second->flush(err);
    if (err && !err->empty()) {
      return;
    }
  }
  log_.clear(err);
}

}  // namespace mini_db
