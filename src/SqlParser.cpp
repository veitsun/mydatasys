#include "db/SqlParser.h"

#include "db/Utils.h"

#include <cctype>
#include <limits>
#include <sstream>

namespace mini_db {

namespace {

// 词法 Token 类型：标识符/数字/字符串/符号。
enum class TokenType {
  Identifier,
  Number,
  String,
  Symbol,
};

// 词法 Token。
struct Token {
  TokenType type;
  std::string text;
};

// SQL 中支持的单字符符号。
bool is_symbol(char c) {
  return c == '(' || c == ')' || c == ',' || c == '=' || c == '*';
}

// 简单词法分析：按空白分隔、识别符号、字符串与数字。
std::vector<Token> tokenize(const std::string& sql, std::string* err) {
  std::vector<Token> tokens;
  size_t i = 0;
  while (i < sql.size()) {
    char c = sql[i];
    if (std::isspace(static_cast<unsigned char>(c)) || c == ';') {
      ++i;
      continue;
    }
    if (is_symbol(c)) {
      tokens.push_back({TokenType::Symbol, std::string(1, c)});
      ++i;
      continue;
    }
    if (c == '"' || c == '\'') {
      // 字符串字面量（不支持转义）。
      char quote = c;
      ++i;
      std::string value;
      while (i < sql.size() && sql[i] != quote) {
        value.push_back(sql[i]);
        ++i;
      }
      if (i >= sql.size()) {
        if (err) {
          *err = "unterminated string";
        }
        return {};
      }
      ++i;
      tokens.push_back({TokenType::String, value});
      continue;
    }
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
      // 标识符或关键字。
      size_t start = i;
      while (i < sql.size() &&
             (std::isalnum(static_cast<unsigned char>(sql[i])) || sql[i] == '_')) {
        ++i;
      }
      tokens.push_back({TokenType::Identifier, sql.substr(start, i - start)});
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(c)) ||
        ((c == '-' || c == '+') && i + 1 < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
      // 数字字面量（仅整数）。
      size_t start = i;
      ++i;
      while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) {
        ++i;
      }
      tokens.push_back({TokenType::Number, sql.substr(start, i - start)});
      continue;
    }
    if (err) {
      *err = "unexpected character: " + std::string(1, c);
    }
    return {};
  }
  return tokens;
}

// 递归下降式解析器，按 token 序列解析 SQL。
class Parser {
 public:
  explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  // 是否已到达 token 末尾。
  bool eof() const { return pos_ >= tokens_.size(); }

  // 查看当前 token（不前进）。
  const Token* peek() const {
    if (eof()) {
      return nullptr;
    }
    return &tokens_[pos_];
  }

  bool match_symbol(char sym) {
    if (eof()) {
      return false;
    }
    const Token& tok = tokens_[pos_];
    if (tok.type == TokenType::Symbol && tok.text.size() == 1 && tok.text[0] == sym) {
      ++pos_;
      return true;
    }
    return false;
  }

  bool expect_symbol(char sym, std::string* err) {
    if (!match_symbol(sym)) {
      if (err) {
        *err = std::string("expected symbol: ") + sym;
      }
      return false;
    }
    return true;
  }

  bool match_keyword(const std::string& keyword) {
    if (eof()) {
      return false;
    }
    const Token& tok = tokens_[pos_];
    if (tok.type == TokenType::Identifier && to_upper(tok.text) == keyword) {
      ++pos_;
      return true;
    }
    return false;
  }

  bool expect_keyword(const std::string& keyword, std::string* err) {
    if (!match_keyword(keyword)) {
      if (err) {
        *err = "expected keyword: " + keyword;
      }
      return false;
    }
    return true;
  }

  bool expect_identifier(std::string* out, std::string* err) {
    // 读取普通标识符。
    if (eof()) {
      if (err) {
        *err = "expected identifier";
      }
      return false;
    }
    const Token& tok = tokens_[pos_];
    if (tok.type != TokenType::Identifier) {
      if (err) {
        *err = "expected identifier";
      }
      return false;
    }
    if (out) {
      *out = tok.text;
    }
    ++pos_;
    return true;
  }

  bool expect_number_text(std::string* out, std::string* err) {
    // 读取数字 token（某些场景允许作为标识符读取）。
    if (eof()) {
      if (err) {
        *err = "expected number";
      }
      return false;
    }
    const Token& tok = tokens_[pos_];
    if (tok.type != TokenType::Number && tok.type != TokenType::Identifier) {
      if (err) {
        *err = "expected number";
      }
      return false;
    }
    if (out) {
      *out = tok.text;
    }
    ++pos_;
    return true;
  }

  bool parse_value(Value* value, std::string* err) {
    // 解析常量值：数字/字符串/标识符（作为 TEXT）。
    if (eof()) {
      if (err) {
        *err = "expected value";
      }
      return false;
    }
    const Token& tok = tokens_[pos_];
    if (tok.type == TokenType::Number) {
      // 数值范围限制为 int32。
      long long parsed = 0;
      try {
        parsed = std::stoll(tok.text);
      } catch (...) {
        if (err) {
          *err = "invalid number";
        }
        return false;
      }
      if (parsed < std::numeric_limits<int32_t>::min() ||
          parsed > std::numeric_limits<int32_t>::max()) {
        if (err) {
          *err = "number out of range";
        }
        return false;
      }
      if (value) {
        *value = Value::Int(static_cast<int32_t>(parsed));
      }
      ++pos_;
      return true;
    }
    if (tok.type == TokenType::String) {
      if (value) {
        *value = Value::Text(tok.text);
      }
      ++pos_;
      return true;
    }
    if (tok.type == TokenType::Identifier) {
      // 允许未加引号的文本，按 TEXT 处理。
      if (value) {
        *value = Value::Text(tok.text);
      }
      ++pos_;
      return true;
    }
    if (err) {
      *err = "expected value";
    }
    return false;
  }

  bool parse_column_type(Column* column, std::string* err) {
    // 解析列类型描述（INT / TEXT(n)）。
    std::string type;
    if (!expect_identifier(&type, err)) {
      return false;
    }
    std::string upper = to_upper(type);
    if (upper == "INT") {
      column->type = ColumnType::Int;
      column->length = 0;
      return true;
    }
    if (upper == "TEXT") {
      column->type = ColumnType::Text;
      column->length = 64;
      if (match_symbol('(')) {
        std::string length_token;
        if (!expect_number_text(&length_token, err)) {
          return false;
        }
        if (!is_number(length_token)) {
          if (err) {
            *err = "invalid TEXT length";
          }
          return false;
        }
        column->length = static_cast<uint32_t>(std::stoul(length_token));
        if (!expect_symbol(')', err)) {
          return false;
        }
      }
      return true;
    }
    if (err) {
      *err = "unsupported column type: " + type;
    }
    return false;
  }

 private:
  std::vector<Token> tokens_;
  size_t pos_ = 0;
};

}  // namespace

bool SqlParser::parse(const std::string& sql, Statement* statement, std::string* err) const {
  // 顶层语法入口：根据关键字分派不同语句。
  if (!statement) {
    if (err) {
      *err = "statement output missing";
    }
    return false;
  }
  if (err) {
    err->clear();
  }
  *statement = Statement{};
  std::vector<Token> tokens = tokenize(sql, err);
  if (!err || err->empty()) {
    if (tokens.empty()) {
      if (err) {
        *err = "empty statement";
      }
      return false;
    }
  } else {
    return false;
  }

  Parser parser(std::move(tokens));
  if (parser.match_keyword("CREATE")) {
    // CREATE TABLE t (col TYPE, ...);
    statement->type = StatementType::CreateTable;
    if (!parser.expect_keyword("TABLE", err)) {
      return false;
    }
    if (!parser.expect_identifier(&statement->table, err)) {
      return false;
    }
    if (!parser.expect_symbol('(', err)) {
      return false;
    }
    while (true) {
      Column col;
      if (!parser.expect_identifier(&col.name, err)) {
        return false;
      }
      if (!parser.parse_column_type(&col, err)) {
        return false;
      }
      statement->columns.push_back(col);
      if (parser.match_symbol(',')) {
        continue;
      }
      break;
    }
    if (!parser.expect_symbol(')', err)) {
      return false;
    }
    return true;
  }
  if (parser.match_keyword("DROP")) {
    // DROP TABLE t;
    statement->type = StatementType::DropTable;
    if (!parser.expect_keyword("TABLE", err)) {
      return false;
    }
    return parser.expect_identifier(&statement->table, err);
  }
  if (parser.match_keyword("ALTER")) {
    // ALTER TABLE t ADD [COLUMN] col TYPE;
    statement->type = StatementType::AlterTableAdd;
    if (!parser.expect_keyword("TABLE", err)) {
      return false;
    }
    if (!parser.expect_identifier(&statement->table, err)) {
      return false;
    }
    if (!parser.expect_keyword("ADD", err)) {
      return false;
    }
    parser.match_keyword("COLUMN");
    if (!parser.expect_identifier(&statement->alter_column.name, err)) {
      return false;
    }
    if (!parser.parse_column_type(&statement->alter_column, err)) {
      return false;
    }
    return true;
  }
  if (parser.match_keyword("INSERT")) {
    // INSERT INTO t VALUES (...);
    statement->type = StatementType::Insert;
    if (!parser.expect_keyword("INTO", err)) {
      return false;
    }
    if (!parser.expect_identifier(&statement->table, err)) {
      return false;
    }
    if (!parser.expect_keyword("VALUES", err)) {
      return false;
    }
    if (!parser.expect_symbol('(', err)) {
      return false;
    }
    while (true) {
      Value value;
      if (!parser.parse_value(&value, err)) {
        return false;
      }
      statement->values.push_back(std::move(value));
      if (parser.match_symbol(',')) {
        continue;
      }
      break;
    }
    if (!parser.expect_symbol(')', err)) {
      return false;
    }
    return true;
  }
  if (parser.match_keyword("SELECT")) {
    // SELECT * FROM t [WHERE col = value];
    statement->type = StatementType::Select;
    if (!parser.expect_symbol('*', err)) {
      return false;
    }
    if (!parser.expect_keyword("FROM", err)) {
      return false;
    }
    if (!parser.expect_identifier(&statement->table, err)) {
      return false;
    }
    if (parser.match_keyword("WHERE")) {
      statement->where.has = true;
      if (!parser.expect_identifier(&statement->where.column, err)) {
        return false;
      }
      if (!parser.expect_symbol('=', err)) {
        return false;
      }
      if (!parser.parse_value(&statement->where.value, err)) {
        return false;
      }
    }
    return true;
  }
  if (parser.match_keyword("UPDATE")) {
    // UPDATE t SET col=value [, ...] [WHERE col=value];
    statement->type = StatementType::Update;
    if (!parser.expect_identifier(&statement->table, err)) {
      return false;
    }
    if (!parser.expect_keyword("SET", err)) {
      return false;
    }
    while (true) {
      SetClause set;
      if (!parser.expect_identifier(&set.column, err)) {
        return false;
      }
      if (!parser.expect_symbol('=', err)) {
        return false;
      }
      if (!parser.parse_value(&set.value, err)) {
        return false;
      }
      statement->set_clauses.push_back(std::move(set));
      if (parser.match_symbol(',')) {
        continue;
      }
      break;
    }
    if (parser.match_keyword("WHERE")) {
      statement->where.has = true;
      if (!parser.expect_identifier(&statement->where.column, err)) {
        return false;
      }
      if (!parser.expect_symbol('=', err)) {
        return false;
      }
      if (!parser.parse_value(&statement->where.value, err)) {
        return false;
      }
    }
    return true;
  }
  if (parser.match_keyword("DELETE")) {
    // DELETE FROM t [WHERE col=value];
    statement->type = StatementType::Delete;
    if (!parser.expect_keyword("FROM", err)) {
      return false;
    }
    if (!parser.expect_identifier(&statement->table, err)) {
      return false;
    }
    if (parser.match_keyword("WHERE")) {
      statement->where.has = true;
      if (!parser.expect_identifier(&statement->where.column, err)) {
        return false;
      }
      if (!parser.expect_symbol('=', err)) {
        return false;
      }
      if (!parser.parse_value(&statement->where.value, err)) {
        return false;
      }
    }
    return true;
  }

  if (err) {
    *err = "unsupported statement";
  }
  return false;
}

}  // namespace mini_db
