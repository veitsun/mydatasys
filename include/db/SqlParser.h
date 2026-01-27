#pragma once

#include "db/Types.h"

#include <string>

namespace mini_db {

// 简易 SQL 解析器：将 SQL 文本转换为 Statement 结构。
class SqlParser {
 public:
  // 解析 SQL，成功返回 true，失败返回 false 并写入 err。
  bool parse(const std::string& sql, Statement* statement, std::string* err) const;
};

}  // namespace mini_db
