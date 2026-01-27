#include "db/Schema.h"

#include "db/Utils.h"

#include <cstring>
#include <limits>

namespace mini_db {

namespace {

// 按小端序写入 32 位整数。
void write_int32(std::vector<char>* out, size_t offset, int32_t value) {
  (*out)[offset] = static_cast<char>(value & 0xFF);
  (*out)[offset + 1] = static_cast<char>((value >> 8) & 0xFF);
  (*out)[offset + 2] = static_cast<char>((value >> 16) & 0xFF);
  (*out)[offset + 3] = static_cast<char>((value >> 24) & 0xFF);
}

// 按小端序读取 32 位整数。
int32_t read_int32(const std::vector<char>& data, size_t offset) {
  uint32_t b0 = static_cast<unsigned char>(data[offset]);
  uint32_t b1 = static_cast<unsigned char>(data[offset + 1]);
  uint32_t b2 = static_cast<unsigned char>(data[offset + 2]);
  uint32_t b3 = static_cast<unsigned char>(data[offset + 3]);
  return static_cast<int32_t>(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24));
}

}  // namespace

Schema::Schema(const std::vector<Column>& columns) : columns_(columns) {
  // 预构建列名索引，便于快速查找。
  for (size_t i = 0; i < columns_.size(); ++i) {
    column_map_[to_lower(columns_[i].name)] = i;
  }
}

const std::vector<Column>& Schema::columns() const {
  return columns_;
}

size_t Schema::data_size() const {
  // INT 固定 4 字节，TEXT 使用列定义的固定长度。
  size_t size = 0;
  for (const auto& col : columns_) {
    if (col.type == ColumnType::Int) {
      size += sizeof(int32_t);
    } else {
      size += col.length;
    }
  }
  return size;
}

size_t Schema::record_size() const {
  // 记录首字节为有效标记。
  return 1 + data_size();
}

int Schema::column_index(const std::string& name) const {
  auto it = column_map_.find(to_lower(name));
  if (it == column_map_.end()) {
    return -1;
  }
  return static_cast<int>(it->second);
}

bool Schema::normalize_value(size_t col_index, Value* value, std::string* err) const {
  // 将输入值转换为目标列类型并校验长度/范围。
  if (col_index >= columns_.size()) {
    if (err) {
      *err = "column index out of range";
    }
    return false;
  }
  const Column& col = columns_[col_index];
  if (col.type == ColumnType::Int) {
    if (value->type == ColumnType::Int) {
      return true;
    }
    if (value->type == ColumnType::Text) {
      // TEXT -> INT：要求字符串是有效整数并且不越界。
      if (!is_number(value->text_value)) {
        if (err) {
          *err = "expected INT for column " + col.name;
        }
        return false;
      }
      long long parsed = 0;
      try {
        parsed = std::stoll(value->text_value);
      } catch (...) {
        if (err) {
          *err = "invalid INT value for column " + col.name;
        }
        return false;
      }
      if (parsed < std::numeric_limits<int32_t>::min() ||
          parsed > std::numeric_limits<int32_t>::max()) {
        if (err) {
          *err = "INT value out of range for column " + col.name;
        }
        return false;
      }
      value->type = ColumnType::Int;
      value->int_value = static_cast<int32_t>(parsed);
      value->text_value.clear();
      return true;
    }
  }

  if (col.type == ColumnType::Text) {
    if (value->type == ColumnType::Text) {
      // TEXT 原样保存，额外做长度限制。
      if (col.length > 0 && value->text_value.size() > col.length) {
        if (err) {
          *err = "TEXT value too long for column " + col.name;
        }
        return false;
      }
      return true;
    }
    if (value->type == ColumnType::Int) {
      // INT -> TEXT：使用十进制字符串表示。
      value->type = ColumnType::Text;
      value->text_value = std::to_string(value->int_value);
      if (col.length > 0 && value->text_value.size() > col.length) {
        if (err) {
          *err = "TEXT value too long for column " + col.name;
        }
        return false;
      }
      return true;
    }
  }

  if (err) {
    *err = "unsupported value type for column " + col.name;
  }
  return false;
}

bool Schema::validate_values(std::vector<Value>* values, std::string* err) const {
  // 列数必须匹配，并且每个值都要能转换为列类型。
  if (!values) {
    if (err) {
      *err = "values missing";
    }
    return false;
  }
  if (values->size() != columns_.size()) {
    if (err) {
      *err = "value count does not match column count";
    }
    return false;
  }
  for (size_t i = 0; i < values->size(); ++i) {
    if (!normalize_value(i, &(*values)[i], err)) {
      return false;
    }
  }
  return true;
}

std::vector<char> Schema::encode_record(std::vector<Value> values, bool valid, std::string* err) const {
  // 先归一化数据，再按列顺序编码到固定长度记录中。
  if (!validate_values(&values, err)) {
    return {};
  }
  std::vector<char> record(record_size(), 0);
  record[0] = valid ? 1 : 0;
  size_t offset = 1;
  for (size_t i = 0; i < columns_.size(); ++i) {
    const Column& col = columns_[i];
    const Value& val = values[i];
    if (col.type == ColumnType::Int) {
      write_int32(&record, offset, val.int_value);
      offset += sizeof(int32_t);
    } else {
      std::string text = val.text_value;
      if (col.length > 0 && text.size() > col.length) {
        if (err) {
          *err = "TEXT value too long for column " + col.name;
        }
        return {};
      }
      // TEXT 列使用固定长度，不足补 0。
      std::memset(record.data() + offset, 0, col.length);
      std::memcpy(record.data() + offset, text.data(), text.size());
      offset += col.length;
    }
  }
  return record;
}

bool Schema::decode_record(const std::vector<char>& record, std::vector<Value>* values, bool* valid,
                           std::string* err) const {
  // 按列定义读取记录，并还原为 Value 列表。
  if (record.size() < record_size()) {
    if (err) {
      *err = "record size mismatch";
    }
    return false;
  }
  if (values) {
    values->clear();
    values->reserve(columns_.size());
  }
  if (valid) {
    *valid = record[0] != 0;
  }
  size_t offset = 1;
  for (const auto& col : columns_) {
    if (col.type == ColumnType::Int) {
      int32_t val = read_int32(record, offset);
      if (values) {
        values->push_back(Value::Int(val));
      }
      offset += sizeof(int32_t);
    } else {
      // TEXT 列读取到第一个 '\0' 结束。
      std::string text;
      text.reserve(col.length);
      for (size_t i = 0; i < col.length; ++i) {
        char c = record[offset + i];
        if (c == '\0') {
          break;
        }
        text.push_back(c);
      }
      if (values) {
        values->push_back(Value::Text(text));
      }
      offset += col.length;
    }
  }
  return true;
}

std::vector<Value> Schema::default_values() const {
  // 用于补齐新增列的默认值。
  std::vector<Value> values;
  values.reserve(columns_.size());
  for (const auto& col : columns_) {
    if (col.type == ColumnType::Int) {
      values.push_back(Value::Int(0));
    } else {
      values.push_back(Value::Text(""));
    }
  }
  return values;
}

}  // namespace mini_db
