#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace mini_db {

// 列类型枚举，目前支持整数与定长文本。
enum class ColumnType {
  Int,
  Text,
};

// 数据值，使用简单的 union-like 结构保存 INT 或 TEXT。
struct Value {
  ColumnType type = ColumnType::Int;
  int32_t int_value = 0;
  std::string text_value;

  // 创建 INT 类型值。
  static Value Int(int32_t v) {
    Value value;
    value.type = ColumnType::Int;
    value.int_value = v;
    return value;
  }

  // 创建 TEXT 类型值。
  static Value Text(const std::string& v) {
    Value value;
    value.type = ColumnType::Text;
    value.text_value = v;
    return value;
  }
};

// 列定义：名称、类型、定长文本长度（仅 TEXT 生效）。
struct Column {
  std::string name;
  ColumnType type = ColumnType::Int;
  uint32_t length = 0;
};

// WHERE 条件，只支持单列等值匹配。
struct Condition {
  bool has = false;
  std::string column;
  Value value;
};

// UPDATE 语句的 SET 子句。
struct SetClause {
  std::string column;
  Value value;
};

// SQL 语句类型枚举。
enum class StatementType {
  CreateTable,
  DropTable,
  AlterTableAdd,
  Insert,
  Select,
  Update,
  Delete,
  Unknown,
};

// 解析后的 SQL 语句结构，供执行器使用。
struct Statement {
  StatementType type = StatementType::Unknown;
  // 目标表名。
  std::string table;
  // CREATE TABLE 使用的列定义。
  std::vector<Column> columns;
  // INSERT 使用的值列表。
  std::vector<Value> values;
  // UPDATE 使用的 set 子句集合。
  std::vector<SetClause> set_clauses;
  // WHERE 条件（可选）。
  Condition where;
  // ALTER TABLE ADD COLUMN 使用的列定义。
  Column alter_column;
};

}  // namespace mini_db
