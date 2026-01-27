#include "db/Executor.h"

#include <sstream>

namespace mini_db {

namespace {

// 将 Value 转为输出文本（用于 SELECT 显示）。
std::string value_to_string(const Value& value) {
  if (value.type == ColumnType::Int) {
    return std::to_string(value.int_value);
  }
  return value.text_value;
}

}  // namespace

bool Executor::execute(const Statement& statement, Database* db, std::string* output,
                       std::string* err) {
  // 根据语句类型调用 Database 对应接口，并组织输出。
  if (!db) {
    if (err) {
      *err = "database not available";
    }
    return false;
  }
  if (output) {
    output->clear();
  }
  switch (statement.type) {
    case StatementType::CreateTable: {
      // DDL：创建表。
      if (!db->create_table(statement.table, statement.columns, err)) {
        return false;
      }
      if (output) {
        *output = "OK";
      }
      return true;
    }
    case StatementType::DropTable: {
      // DDL：删除表。
      if (!db->drop_table(statement.table, err)) {
        return false;
      }
      if (output) {
        *output = "OK";
      }
      return true;
    }
    case StatementType::AlterTableAdd: {
      // DDL：添加列。
      if (!db->alter_add_column(statement.table, statement.alter_column, err)) {
        return false;
      }
      if (output) {
        *output = "OK";
      }
      return true;
    }
    case StatementType::Insert: {
      // DML：插入记录。
      uint64_t row_id = 0;
      if (!db->insert(statement.table, statement.values, &row_id, err)) {
        return false;
      }
      if (output) {
        *output = "Inserted row " + std::to_string(row_id);
      }
      return true;
    }
    case StatementType::Select: {
      // DML：查询并打印结果表格。
      std::vector<std::vector<Value>> rows;
      if (!db->select(statement.table, statement.where, &rows, err)) {
        return false;
      }
      Schema schema;
      if (!db->get_schema(statement.table, &schema, err)) {
        return false;
      }
      std::ostringstream oss;
      const auto& cols = schema.columns();
      // 输出表头。
      for (size_t i = 0; i < cols.size(); ++i) {
        oss << cols[i].name;
        if (i + 1 < cols.size()) {
          oss << "\t";
        }
      }
      oss << "\n";
      for (const auto& row : rows) {
        // 输出每一行的列值。
        for (size_t i = 0; i < row.size(); ++i) {
          oss << value_to_string(row[i]);
          if (i + 1 < row.size()) {
            oss << "\t";
          }
        }
        oss << "\n";
      }
      oss << "Rows: " << rows.size();
      if (output) {
        *output = oss.str();
      }
      return true;
    }
    case StatementType::Update: {
      // DML：更新记录。
      size_t updated = 0;
      if (!db->update(statement.table, statement.set_clauses, statement.where, &updated, err)) {
        return false;
      }
      if (output) {
        *output = "Updated " + std::to_string(updated) + " rows";
      }
      return true;
    }
    case StatementType::Delete: {
      // DML：删除记录。
      size_t removed = 0;
      if (!db->remove(statement.table, statement.where, &removed, err)) {
        return false;
      }
      if (output) {
        *output = "Deleted " + std::to_string(removed) + " rows";
      }
      return true;
    }
    default:
      if (err) {
        *err = "unsupported statement";
      }
      return false;
  }
}

}  // namespace mini_db
