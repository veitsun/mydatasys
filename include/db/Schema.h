#pragma once

#include "db/Types.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace mini_db {

// 表结构描述：负责列定义、记录编码/解码与值合法性校验。
class Schema {
 public:
  Schema() = default;
  // 使用列定义构建 Schema，同时建立列名到索引的映射。
  explicit Schema(const std::vector<Column>& columns);

  // 返回列定义列表（按创建顺序）。
  const std::vector<Column>& columns() const;
  // 仅列数据占用的字节数（不含有效位）。
  size_t data_size() const;
  // 单条记录占用的总字节数（包含 1 字节有效标记）。
  size_t record_size() const;
  // 根据列名查找索引，找不到返回 -1。
  int column_index(const std::string& name) const;

  // 归一化单个列的值（类型转换、长度校验等）。
  bool normalize_value(size_t col_index, Value* value, std::string* err) const;
  // 校验并归一化整行数据（列数与类型匹配）。
  bool validate_values(std::vector<Value>* values, std::string* err) const;
  // 将一行数据编码为记录字节（包含有效标记）。
  std::vector<char> encode_record(std::vector<Value> values, bool valid, std::string* err) const;
  // 从记录字节解码为一行数据，并输出有效标记。
  bool decode_record(const std::vector<char>& record, std::vector<Value>* values, bool* valid,
                     std::string* err) const;
  // 生成默认值（INT 为 0，TEXT 为空字符串）。
  std::vector<Value> default_values() const;

 private:
  // 列定义与列名索引。
  std::vector<Column> columns_;
  std::unordered_map<std::string, size_t> column_map_;
};

}  // namespace mini_db
