#include "db/Catalog.h"

#include "db/Utils.h"

#include <fstream>
#include <sstream>

namespace mini_db {

namespace {

// 解析文本类型描述（如 INT / TEXT(32)）。
bool parse_column_type(const std::string& input, Column* column, std::string* err) {
  std::string upper = to_upper(input);
  if (upper == "INT") {
    column->type = ColumnType::Int;
    column->length = 0;
    return true;
  }
  if (upper.rfind("TEXT", 0) == 0) {
    column->type = ColumnType::Text;
    // 支持 TEXT 与 TEXT(n) 两种写法。
    size_t open = input.find('(');
    size_t close = input.find(')');
    if (open != std::string::npos && close != std::string::npos && close > open + 1) {
      std::string len = input.substr(open + 1, close - open - 1);
      if (!is_number(trim(len))) {
        if (err) {
          *err = "invalid TEXT length";
        }
        return false;
      }
      column->length = static_cast<uint32_t>(std::stoul(len));
    } else {
      column->length = 64;
    }
    return true;
  }
  if (err) {
    *err = "unknown column type: " + input;
  }
  return false;
}

// 将列类型格式化为 catalog 存储字符串。
std::string format_column_type(const Column& column) {
  if (column.type == ColumnType::Int) {
    return "INT";
  }
  std::ostringstream oss;
  oss << "TEXT(" << column.length << ")";
  return oss.str();
}

}  // namespace

Catalog::Catalog(const std::string& path) : path_(path) {}

bool Catalog::load(std::string* err) {
  // catalog 格式：table|col:type|col:type...
  schemas_.clear();
  std::ifstream file(path_);
  if (!file.is_open()) {
    return true;
  }
  std::string line;
  while (std::getline(file, line)) {
    line = trim(line);
    if (line.empty()) {
      continue;
    }
    std::stringstream ss(line);
    std::string segment;
    std::vector<std::string> parts;
    while (std::getline(ss, segment, '|')) {
      parts.push_back(segment);
    }
    if (parts.size() < 2) {
      continue;
    }
    std::string table = to_lower(trim(parts[0]));
    std::vector<Column> columns;
    for (size_t i = 1; i < parts.size(); ++i) {
      std::string part = trim(parts[i]);
      size_t colon = part.find(':');
      if (colon == std::string::npos) {
        continue;
      }
      Column col;
      col.name = trim(part.substr(0, colon));
      std::string type_str = trim(part.substr(colon + 1));
      std::string parse_err;
      if (!parse_column_type(type_str, &col, &parse_err)) {
        if (err) {
          *err = parse_err;
        }
        return false;
      }
      columns.push_back(col);
    }
    schemas_[table] = Schema(columns);
  }
  return true;
}

bool Catalog::save(std::string* err) const {
  // 覆盖写出当前内存中的表结构。
  std::ofstream file(path_, std::ios::out | std::ios::trunc);
  if (!file.is_open()) {
    if (err) {
      *err = "failed to write catalog";
    }
    return false;
  }
  for (const auto& pair : schemas_) {
    file << pair.first;
    const auto& cols = pair.second.columns();
    for (const auto& col : cols) {
      file << "|" << col.name << ":" << format_column_type(col);
    }
    file << "\n";
  }
  return true;
}

bool Catalog::create_table(const std::string& name, const Schema& schema, std::string* err) {
  std::string key = to_lower(name);
  if (schemas_.find(key) != schemas_.end()) {
    if (err) {
      *err = "table already exists: " + name;
    }
    return false;
  }
  schemas_[key] = schema;
  return save(err);
}

bool Catalog::drop_table(const std::string& name, std::string* err) {
  std::string key = to_lower(name);
  if (schemas_.erase(key) == 0) {
    if (err) {
      *err = "table not found: " + name;
    }
    return false;
  }
  return save(err);
}

bool Catalog::alter_add_column(const std::string& name, const Column& column, std::string* err) {
  std::string key = to_lower(name);
  auto it = schemas_.find(key);
  if (it == schemas_.end()) {
    if (err) {
      *err = "table not found: " + name;
    }
    return false;
  }
  std::vector<Column> cols = it->second.columns();
  for (const auto& col : cols) {
    if (iequals(col.name, column.name)) {
      if (err) {
        *err = "column already exists: " + column.name;
      }
      return false;
    }
  }
  cols.push_back(column);
  it->second = Schema(cols);
  return save(err);
}

bool Catalog::get_schema(const std::string& name, Schema* schema) const {
  std::string key = to_lower(name);
  auto it = schemas_.find(key);
  if (it == schemas_.end() || !schema) {
    return false;
  }
  *schema = it->second;
  return true;
}

std::vector<std::string> Catalog::list_tables() const {
  std::vector<std::string> tables;
  tables.reserve(schemas_.size());
  for (const auto& pair : schemas_) {
    tables.push_back(pair.first);
  }
  return tables;
}

}  // namespace mini_db
