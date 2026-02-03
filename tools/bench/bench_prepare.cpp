#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cerrno>

namespace {

// 压测准备配置：控制表结构与行数。
struct PrepareConfig {
  std::string data_dir = "./data_bench";   // 数据目录
  std::string table = "bench_table";       // 表名
  size_t rows = 10000;                     // 初始化行数
  bool reset = true;                       // 是否清空旧表
};

bool is_number(const std::string& value) {
  if (value.empty()) {
    return false;
  }
  for (char ch : value) {
    if (ch < '0' || ch > '9') {
      return false;
    }
  }
  return true;
}

bool parse_size(const std::string& value, size_t* out) {
  if (!out || !is_number(value)) {
    return false;
  }
  *out = static_cast<size_t>(std::stoull(value));
  return true;
}

void print_usage() {
  std::cout
      << "mini_db_bench_prepare usage:\n"
      << "  --rows=N           初始化数据行数 (default 10000)\n"
      << "  --data=PATH        数据目录 (default ./data_bench)\n"
      << "  --table=NAME       表名 (default bench_table)\n"
      << "  --no-reset         不清空旧表 (默认会重建表)\n";
}

bool parse_args(int argc, char** argv, PrepareConfig* config) {
  if (!config) {
    return false;
  }
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      print_usage();
      return false;
    }
    if (arg == "--no-reset") {
      config->reset = false;
      continue;
    }
    auto eq = arg.find('=');
    if (eq == std::string::npos) {
      std::cerr << "Unknown argument: " << arg << "\n";
      return false;
    }
    std::string key = arg.substr(0, eq);
    std::string value = arg.substr(eq + 1);
    if (key == "--rows") {
      if (!parse_size(value, &config->rows)) {
        return false;
      }
    } else if (key == "--data") {
      config->data_dir = value;
    } else if (key == "--table") {
      config->table = value;
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      return false;
    }
  }
  return true;
}

void write_uint32(std::vector<char>* out, size_t offset, uint32_t value) {
  (*out)[offset] = static_cast<char>(value & 0xFF);
  (*out)[offset + 1] = static_cast<char>((value >> 8) & 0xFF);
  (*out)[offset + 2] = static_cast<char>((value >> 16) & 0xFF);
  (*out)[offset + 3] = static_cast<char>((value >> 24) & 0xFF);
}

void write_uint64(std::vector<char>* out, size_t offset, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    (*out)[offset + i] = static_cast<char>((value >> (8 * i)) & 0xFF);
  }
}

std::vector<char> make_text32(int id) {
  std::string text = "value_" + std::to_string(id);
  std::vector<char> buffer(32, 0);
  size_t copy_len = text.size() < buffer.size() ? text.size() : buffer.size();
  std::memcpy(buffer.data(), text.data(), copy_len);
  return buffer;
}

bool write_catalog(const std::string& path, const std::string& table) {
  std::ofstream file(path, std::ios::out | std::ios::trunc);
  if (!file.is_open()) {
    return false;
  }
  file << table << "|id:INT|value:TEXT(32)\n";
  return file.good();
}

bool write_table_file(const std::string& path, size_t rows) {
  const size_t page_size = 4096;
  const size_t record_size = 1 + sizeof(int32_t) + 32;
  const size_t header_size = 32;
  std::ofstream file(path, std::ios::out | std::ios::binary | std::ios::trunc);
  if (!file.is_open()) {
    return false;
  }

  std::vector<char> header(header_size, 0);
  header[0] = 'T';
  header[1] = 'B';
  header[2] = 'L';
  header[3] = '1';
  write_uint32(&header, 4, static_cast<uint32_t>(record_size));
  write_uint64(&header, 8, static_cast<uint64_t>(rows));
  write_uint64(&header, 16, 0);
  file.write(header.data(), static_cast<std::streamsize>(header.size()));
  if (!file) {
    return false;
  }

  if (page_size > header.size()) {
    std::vector<char> padding(page_size - header.size(), 0);
    file.write(padding.data(), static_cast<std::streamsize>(padding.size()));
    if (!file) {
      return false;
    }
  }

  std::vector<char> record(record_size, 0);
  for (size_t i = 0; i < rows; ++i) {
    record.assign(record_size, 0);
    record[0] = 1;
    int32_t id = static_cast<int32_t>(i + 1);
    write_uint32(&record, 1, static_cast<uint32_t>(id));
    std::vector<char> text = make_text32(static_cast<int>(i + 1));
    std::memcpy(record.data() + 1 + sizeof(int32_t), text.data(), text.size());
    file.write(record.data(), static_cast<std::streamsize>(record.size()));
    if (!file) {
      return false;
    }
  }
  file.flush();
  return file.good();
}

std::string normalize_path(const std::string& path) {
  if (!path.empty() && path[0] == '/') {
    return path;
  }
  char buf[4096];
  if (!getcwd(buf, sizeof(buf))) {
    return path;
  }
  std::string cwd(buf);
  if (cwd.empty()) {
    return path;
  }
  if (path.empty()) {
    return cwd;
  }
  return cwd + "/" + path;
}

bool ensure_dir(const std::string& path, std::string* err) {
  if (path.empty()) {
    if (err) {
      *err = "data dir empty";
    }
    return false;
  }
  std::string current;
  for (size_t i = 0; i < path.size(); ++i) {
    char ch = path[i];
    current.push_back(ch);
    if (ch == '/' || i + 1 == path.size()) {
      if (current.size() == 1 && current[0] == '/') {
        continue;
      }
      if (mkdir(current.c_str(), 0755) != 0) {
        if (errno != EEXIST) {
          if (err) {
            *err = "failed to create dir: " + current;
          }
          return false;
        }
      }
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  PrepareConfig config;
  if (!parse_args(argc, argv, &config)) {
    return 1;
  }

  std::string err;
  if (!ensure_dir(config.data_dir, &err)) {
    std::cerr << "Failed to create data dir: " << err << "\n";
    return 1;
  }

  const std::string data_dir = normalize_path(config.data_dir);
  const std::string catalog_path = data_dir + "/catalog.meta";
  const std::string table_path = data_dir + "/" + config.table + ".tbl";
  if (!config.reset) {
    std::ifstream table_file(table_path);
    if (table_file.good()) {
      std::cout << "Table already exists, skip prepare.\n";
      return 0;
    }
  }

  if (!write_catalog(catalog_path, config.table)) {
    std::cerr << "Failed to write catalog.meta\n";
    return 1;
  }
  std::cout << "Loading " << config.rows << " rows...\n";
  if (!write_table_file(table_path, config.rows)) {
    std::cerr << "Failed to write table file\n";
    return 1;
  }
  std::cout << "Prepare done: " << table_path << "\n";
  return 0;
}
