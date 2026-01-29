#include "db/TableStorage.h"

#include "db/Utils.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>

namespace mini_db {

namespace {

// 按小端序写入 32 位无符号整数。
void write_uint32(std::vector<char>* out, size_t offset, uint32_t value) {
  (*out)[offset] = static_cast<char>(value & 0xFF);
  (*out)[offset + 1] = static_cast<char>((value >> 8) & 0xFF);
  (*out)[offset + 2] = static_cast<char>((value >> 16) & 0xFF);
  (*out)[offset + 3] = static_cast<char>((value >> 24) & 0xFF);
}

// 按小端序读取 32 位无符号整数。
uint32_t read_uint32(const std::vector<char>& data, size_t offset) {
  uint32_t b0 = static_cast<unsigned char>(data[offset]);
  uint32_t b1 = static_cast<unsigned char>(data[offset + 1]);
  uint32_t b2 = static_cast<unsigned char>(data[offset + 2]);
  uint32_t b3 = static_cast<unsigned char>(data[offset + 3]);
  return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
}

// 按小端序写入 64 位无符号整数。
void write_uint64(std::vector<char>* out, size_t offset, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    (*out)[offset + i] = static_cast<char>((value >> (8 * i)) & 0xFF);
  }
}

// 按小端序读取 64 位无符号整数。
uint64_t read_uint64(const std::vector<char>& data, size_t offset) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(static_cast<unsigned char>(data[offset + i])) << (8 * i);
  }
  return value;
}

// 按列类型比较两个 Value 是否相等。
bool values_equal(const Value& a, const Value& b, ColumnType type) {
  if (type == ColumnType::Int) {
    return a.int_value == b.int_value;
  }
  return a.text_value == b.text_value;
}

}  // namespace

TableStorage::TableStorage(const std::string& path, const std::string& name, const Schema& schema,
                           size_t page_size, size_t cache_pages, int numa_nodes, LogManager* log)
    : path_(path),
      name_(name),
      schema_(schema),
      file_(path, page_size, cache_pages, numa_nodes),
      log_(log),
      page_size_(page_size),
      cache_pages_(cache_pages),
      numa_nodes_(numa_nodes) {}

bool TableStorage::load(std::string* err) {
  // 检查记录大小是否超过页大小，避免无法存储。
  if (schema_.record_size() > page_size_) { 
    if (err) {
      *err = "record size exceeds page size";
    }
    return false;
  }
  if (file_.file_size() == 0) {
    // 新建表文件时写入表头。
    row_count_ = 0;
    return write_header(err);
  }
  if (!read_header(err)) {
    return false;
  }
  return rebuild_free_list(err);
}

const std::string& TableStorage::name() const {
  return name_;
}

const Schema& TableStorage::schema() const {
  return schema_;
}

uint64_t TableStorage::row_count() const {
  return row_count_;
}

bool TableStorage::insert(const std::vector<Value>& values, uint64_t* row_id, std::string* err) {
  // 插入时先做类型与长度校验。
  std::vector<Value> normalized = values;
  if (!schema_.validate_values(&normalized, err)) {
    return false;
  }
  uint64_t new_row_id = 0;
  bool reused = false;
  if (!free_list_.empty()) {
    // 复用已删除记录的空位。
    new_row_id = free_list_.back();
    free_list_.pop_back();
    reused = true;
  } else {
    // 追加新行。
    new_row_id = row_count_;
    row_count_++;
  }
  std::vector<char> record = schema_.encode_record(normalized, true, err);
  if (record.empty()) {
    return false;
  }
  if (log_) {
    // 先写日志再写数据，保证恢复时可重放。
    if (!log_->append("INSERT", name_, new_row_id, record, err)) {
      return false;
    }
  }
  if (!write_record(new_row_id, record, err)) {
    return false;
  }
  if (!reused) {
    // 仅在新增行时更新表头行数。
    if (!write_header(err)) {
      return false;
    }
  }
  if (row_id) {
    *row_id = new_row_id;
  }
  return true;
}

bool TableStorage::select(const Condition& where, std::vector<std::vector<Value>>* rows,
                          std::string* err) {
  // 全表扫描 + 单列等值过滤。
  if (!rows) {
    if (err) {
      *err = "rows output missing";
    }
    return false;
  }
  rows->clear();
  int where_idx = -1;
  Value where_value;
  if (where.has) {
    // 预解析 WHERE 列索引与值类型。
    where_idx = schema_.column_index(where.column);
    if (where_idx < 0) {
      if (err) {
        *err = "unknown column in WHERE: " + where.column;
      }
      return false;
    }
    where_value = where.value;
    if (!schema_.normalize_value(static_cast<size_t>(where_idx), &where_value, err)) {
      return false;
    }
  }
  for (uint64_t row_id = 0; row_id < row_count_; ++row_id) {
    // 逐行读取记录并解码。
    std::vector<char> record;
    if (!read_record(row_id, &record, err)) {
      return false;
    }
    bool valid = false;
    std::vector<Value> values;
    if (!schema_.decode_record(record, &values, &valid, err)) {
      return false;
    }
    if (!valid) {
      continue;
    }
    if (where.has) {
      if (!values_equal(values[where_idx], where_value, schema_.columns()[where_idx].type)) {
        continue;
      }
    }
    rows->push_back(std::move(values));
  }
  return true;
}

bool TableStorage::update(const std::vector<SetClause>& sets, const Condition& where,
                          size_t* updated, std::string* err) {
  // 预解析 SET 列并转换类型。
  if (sets.empty()) {
    if (err) {
      *err = "no columns to update";
    }
    return false;
  }
  std::vector<std::pair<size_t, Value>> set_values;
  set_values.reserve(sets.size());
  for (const auto& set : sets) {
    int idx = schema_.column_index(set.column);
    if (idx < 0) {
      if (err) {
        *err = "unknown column in SET: " + set.column;
      }
      return false;
    }
    Value normalized = set.value;
    if (!schema_.normalize_value(static_cast<size_t>(idx), &normalized, err)) {
      return false;
    }
    set_values.emplace_back(static_cast<size_t>(idx), std::move(normalized));
  }

  int where_idx = -1;
  Value where_value;
  if (where.has) {
    // 预解析 WHERE 条件。
    where_idx = schema_.column_index(where.column);
    if (where_idx < 0) {
      if (err) {
        *err = "unknown column in WHERE: " + where.column;
      }
      return false;
    }
    where_value = where.value;
    if (!schema_.normalize_value(static_cast<size_t>(where_idx), &where_value, err)) {
      return false;
    }
  }

  size_t count = 0;
  for (uint64_t row_id = 0; row_id < row_count_; ++row_id) {
    // 遍历所有有效记录，匹配条件后更新。
    std::vector<char> record;
    if (!read_record(row_id, &record, err)) {
      return false;
    }
    bool valid = false;
    std::vector<Value> values;
    if (!schema_.decode_record(record, &values, &valid, err)) {
      return false;
    }
    if (!valid) {
      continue;
    }
    if (where.has) {
      if (!values_equal(values[where_idx], where_value, schema_.columns()[where_idx].type)) {
        continue;
      }
    }
    for (const auto& pair : set_values) {
      // 覆盖对应列的值。
      values[pair.first] = pair.second;
    }
    std::vector<char> updated_record = schema_.encode_record(values, true, err);
    if (updated_record.empty()) {
      return false;
    }
    if (log_) {
      // 记录更新后的整行，用于 redo。
      if (!log_->append("UPDATE", name_, row_id, updated_record, err)) {
        return false;
      }
    }
    if (!write_record(row_id, updated_record, err)) {
      return false;
    }
    ++count;
  }
  if (updated) {
    *updated = count;
  }
  return true;
}

bool TableStorage::remove(const Condition& where, size_t* removed, std::string* err) {
  // 删除同样使用全表扫描。
  int where_idx = -1;
  Value where_value;
  if (where.has) {
    where_idx = schema_.column_index(where.column);
    if (where_idx < 0) {
      if (err) {
        *err = "unknown column in WHERE: " + where.column;
      }
      return false;
    }
    where_value = where.value;
    if (!schema_.normalize_value(static_cast<size_t>(where_idx), &where_value, err)) {
      return false;
    }
  }

  size_t count = 0;
  for (uint64_t row_id = 0; row_id < row_count_; ++row_id) {
    std::vector<char> record;
    if (!read_record(row_id, &record, err)) {
      return false;
    }
    bool valid = false;
    std::vector<Value> values;
    if (!schema_.decode_record(record, &values, &valid, err)) {
      return false;
    }
    if (!valid) {
      continue;
    }
    if (where.has) {
      if (!values_equal(values[where_idx], where_value, schema_.columns()[where_idx].type)) {
        continue;
      }
    }
    // 逻辑删除：将有效标记置 0。
    record[0] = 0;
    if (log_) {
      // 记录删除后的行镜像（有效标记为 0）。
      if (!log_->append("DELETE", name_, row_id, record, err)) {
        return false;
      }
    }
    if (!write_record(row_id, record, err)) {
      return false;
    }
    free_list_.push_back(row_id);
    ++count;
  }
  if (removed) {
    *removed = count;
  }
  return true;
}

bool TableStorage::apply_redo(uint64_t row_id, const std::vector<char>& record, std::string* err) {
  // 恢复时直接覆盖指定行。
  if (record.size() != schema_.record_size()) {
    if (err) {
      *err = "redo record size mismatch";
    }
    return false;
  }
  if (row_id >= row_count_) {
    // 日志中可能包含新增行，需扩展 row_count_。
    row_count_ = row_id + 1;
    if (!write_header(err)) {
      return false;
    }
  }
  return write_record(row_id, record, err);
}

bool TableStorage::rebuild_for_schema(const Schema& new_schema, std::string* err) {
  // 通过创建临时表文件并迁移数据完成 schema 变更。
  std::string temp_path = path_ + ".tmp";
  TableStorage temp_table(temp_path, name_, new_schema, page_size_, cache_pages_, numa_nodes_,
                          nullptr);
  if (!temp_table.load(err)) {
    return false;
  }

  for (uint64_t row_id = 0; row_id < row_count_; ++row_id) {
    // 逐行读取旧记录并映射到新 schema。
    std::vector<char> record;
    if (!read_record(row_id, &record, err)) {
      return false;
    }
    bool valid = false;
    std::vector<Value> values;
    if (!schema_.decode_record(record, &values, &valid, err)) {
      return false;
    }
    std::vector<Value> new_values;
    new_values.reserve(new_schema.columns().size());
    for (const auto& col : new_schema.columns()) {
      int old_idx = schema_.column_index(col.name);
      if (old_idx >= 0 && static_cast<size_t>(old_idx) < values.size()) {
        new_values.push_back(values[static_cast<size_t>(old_idx)]);
      } else {
        // 新增列填充默认值。
        if (col.type == ColumnType::Int) {
          new_values.push_back(Value::Int(0));
        } else {
          new_values.push_back(Value::Text(""));
        }
      }
    }
    std::vector<char> new_record = new_schema.encode_record(new_values, valid, err);
    if (new_record.empty()) {
      return false;
    }
    if (!temp_table.write_record(row_id, new_record, err)) {
      return false;
    }
  }
  temp_table.row_count_ = row_count_;
  if (!temp_table.write_header(err)) {
    return false;
  }
  temp_table.flush(err);
  if (err && !err->empty()) {
    return false;
  }

  // 用临时文件替换旧表文件（带简单备份回滚）。
  std::string backup_path = path_ + ".bak";
  std::remove(backup_path.c_str());
  if (std::rename(path_.c_str(), backup_path.c_str()) != 0) {
    if (err) {
      *err = "failed to backup table file";
    }
    return false;
  }
  if (std::rename(temp_path.c_str(), path_.c_str()) != 0) {
    std::rename(backup_path.c_str(), path_.c_str());
    if (err) {
      *err = "failed to replace table file";
    }
    return false;
  }
  std::remove(backup_path.c_str());

  schema_ = new_schema;
  file_.reset(path_, page_size_, cache_pages_, numa_nodes_);
  return rebuild_free_list(err);
}

bool TableStorage::rebuild_free_list(std::string* err) {
  // 扫描所有记录，重建空闲列表。
  free_list_.clear();
  for (uint64_t row_id = 0; row_id < row_count_; ++row_id) {
    std::vector<char> record;
    if (!read_record(row_id, &record, err)) {
      return false;
    }
    bool valid = false;
    if (!schema_.decode_record(record, nullptr, &valid, err)) {
      return false;
    }
    if (!valid) {
      free_list_.push_back(row_id);
    }
  }
  return true;
}

void TableStorage::flush(std::string* err) {
  // 刷新底层文件缓存。
  file_.flush(err);
}

bool TableStorage::read_header(std::string* err) {
  // 表头位于文件第一个页的起始位置。
  DataItem item;
  if (!file_.read_item(0, kHeaderSize, &item, err)) {
    return false;
  }
  if (item.data.size() < kHeaderSize) {
    if (err) {
      *err = "header too small";
    }
    return false;
  }
  if (item.data[0] != 'T' || item.data[1] != 'B' || item.data[2] != 'L' || item.data[3] != '1') {
    if (err) {
      *err = "invalid table file";
    }
    return false;
  }
  uint32_t record_size = read_uint32(item.data, 4);
  if (record_size != schema_.record_size()) {
    if (err) {
      *err = "record size mismatch with schema";
    }
    return false;
  }
  row_count_ = read_uint64(item.data, 8);
  return true;
}

bool TableStorage::write_header(std::string* err) {
  // 写入固定长度表头：魔数 + 记录大小 + 行数。
  std::vector<char> header(kHeaderSize, 0);
  header[0] = 'T';
  header[1] = 'B';
  header[2] = 'L';
  header[3] = '1';
  write_uint32(&header, 4, static_cast<uint32_t>(schema_.record_size()));
  write_uint64(&header, 8, row_count_);
  write_uint64(&header, 16, 0);
  return file_.write_item(0, header, err);
}

bool TableStorage::read_record(uint64_t row_id, std::vector<char>* record, std::string* err) {
  // 记录偏移 = 页大小（预留表头页）+ row_id * 记录大小。
  if (!record) {
    if (err) {
      *err = "record output missing";
    }
    return false;
  }
  DataItem item;
  if (!file_.read_item(record_offset(row_id), schema_.record_size(), &item, err)) {
    return false;
  }
  *record = std::move(item.data);
  return true;
}

bool TableStorage::write_record(uint64_t row_id, const std::vector<char>& record, std::string* err) {
  // 直接覆盖对应行记录。
  if (record.size() != schema_.record_size()) {
    if (err) {
      *err = "record size mismatch";
    }
    return false;
  }
  return file_.write_item(record_offset(row_id), record, err);
}

size_t TableStorage::record_offset(uint64_t row_id) const {
  // 第一页保留为表头页，数据从第二页开始。
  return page_size_ + static_cast<size_t>(row_id) * schema_.record_size();
}

}  // namespace mini_db
