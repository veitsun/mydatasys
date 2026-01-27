#pragma once

#include <string>
#include <vector>

namespace mini_db {

// 工具函数集合：字符串处理、十六进制编解码、简单数值判断。

// 将字符串全部转换为小写，主要用于大小写不敏感的比较。
std::string to_lower(const std::string& input);
// 将字符串全部转换为大写。
std::string to_upper(const std::string& input);
// 去掉字符串首尾空白字符（空格/制表符/换行等）。
std::string trim(const std::string& input);
// 大小写不敏感的字符串比较。
bool iequals(const std::string& a, const std::string& b);

// 将二进制数据编码为十六进制字符串，便于写入日志文件。
std::string hex_encode(const std::vector<char>& data);
// 将十六进制字符串解码为二进制数据，失败返回 false。
bool hex_decode(const std::string& hex, std::vector<char>* out);

// 判断字符串是否为整数（允许前导 + / -）。
bool is_number(const std::string& input);

}  // namespace mini_db
