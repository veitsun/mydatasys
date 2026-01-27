#include "db/Utils.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace mini_db {

std::string to_lower(const std::string& input) {
  // 逐字符转换为小写，保持原字符串长度不变。
  std::string out = input;
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return out;
}

std::string to_upper(const std::string& input) {
  // 逐字符转换为大写。
  std::string out = input;
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
    return static_cast<char>(std::toupper(c));
  });
  return out;
}

std::string trim(const std::string& input) {
  // 找到首尾非空白的位置并截取。
  size_t start = 0;
  while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
    ++start;
  }
  size_t end = input.size();
  while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return input.substr(start, end - start);
}

bool iequals(const std::string& a, const std::string& b) {
  // 统一大小写后比较。
  return to_lower(a) == to_lower(b);
}

std::string hex_encode(const std::vector<char>& data) {
  // 每个字节拆成高 4 位与低 4 位编码为十六进制字符。
  static const char* kHex = "0123456789ABCDEF";
  std::string out;
  out.reserve(data.size() * 2);
  for (unsigned char c : data) {
    out.push_back(kHex[(c >> 4) & 0xF]);
    out.push_back(kHex[c & 0xF]);
  }
  return out;
}

bool hex_decode(const std::string& hex, std::vector<char>* out) {
  // 将十六进制文本还原为字节数组。
  if (!out) {
    return false;
  }
  out->clear();
  if (hex.size() % 2 != 0) {
    return false;
  }
  out->reserve(hex.size() / 2);
  auto hex_value = [](char c) -> int {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    return -1;
  };
  for (size_t i = 0; i < hex.size(); i += 2) {
    int high = hex_value(hex[i]);
    int low = hex_value(hex[i + 1]);
    if (high < 0 || low < 0) {
      return false;
    }
    out->push_back(static_cast<char>((high << 4) | low));
  }
  return true;
}

bool is_number(const std::string& input) {
  // 只支持整数形式（可带正负号）。
  if (input.empty()) {
    return false;
  }
  size_t i = 0;
  if (input[0] == '-' || input[0] == '+') {
    i = 1;
  }
  if (i >= input.size()) {
    return false;
  }
  for (; i < input.size(); ++i) {
    if (!std::isdigit(static_cast<unsigned char>(input[i]))) {
      return false;
    }
  }
  return true;
}

}  // namespace mini_db
