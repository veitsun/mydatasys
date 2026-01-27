#include "db/Database.h"
#include "db/Executor.h"
#include "db/SqlParser.h"
#include "db/Utils.h"

#include <iostream>

namespace {

// 根据是否是多行输入显示不同提示符。
void print_prompt(bool continuation) {
  if (continuation) {
    std::cout << "....> ";
  } else {
    std::cout << "MiniRDB> ";
  }
}

}  // namespace

int main() {
  // 初始化数据库（数据目录、页大小、缓存页数）。
  mini_db::Database db("./data", 4096, 64);
  std::string err;
  if (!db.open(&err)) {
    std::cerr << "Failed to open database: " << err << "\n";
    return 1;
  }

  // 交互式 REPL：解析 SQL 并执行。
  mini_db::SqlParser parser;
  mini_db::Executor executor;
  std::string buffer;
  print_prompt(false);

  std::string line;
  while (std::getline(std::cin, line)) {
    std::string trimmed = mini_db::trim(line);
    // 空缓冲时遇到 quit/exit 直接退出。
    if (mini_db::trim(buffer).empty() && (trimmed == "exit" || trimmed == "quit")) {
      break;
    }
    buffer += line;
    buffer += " ";

    size_t pos = 0;
    while ((pos = buffer.find(';')) != std::string::npos) {
      // 以分号作为语句结束符，支持多语句输入。
      std::string sql = mini_db::trim(buffer.substr(0, pos));
      buffer = buffer.substr(pos + 1);
      if (sql.empty()) {
        continue;
      }
      mini_db::Statement stmt;
      std::string parse_err;
      if (!parser.parse(sql, &stmt, &parse_err)) {
        // 解析失败提示错误。
        std::cout << "Error: " << parse_err << "\n";
        continue;
      }
      std::string output;
      std::string exec_err;
      if (!executor.execute(stmt, &db, &output, &exec_err)) {
        // 执行失败提示错误。
        std::cout << "Error: " << exec_err << "\n";
        continue;
      }
      if (!output.empty()) {
        std::cout << output << "\n";
      }
    }
    // 若缓冲区仍有未完成语句，则显示续行提示。
    print_prompt(!mini_db::trim(buffer).empty());
  }

  // 收尾：刷盘并关闭数据库。 
  db.close(&err);
  return 0;
}
