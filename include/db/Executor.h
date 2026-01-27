#pragma once

#include "db/Database.h"
#include "db/Types.h"

#include <string>

namespace mini_db {

// 执行器：将解析后的 Statement 转化为数据库操作，并生成输出文本。
class Executor {
 public:
  // 执行一条语句，output 用于可视化输出（如 SELECT）。
  bool execute(const Statement& statement, Database* db, std::string* output, std::string* err);
};

}  // namespace mini_db
