#include "db/Database.h"
#include "db/Types.h"
#include "db/Utils.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace {

// 压测配置：控制数据规模、负载比例与运行参数。
struct BenchConfig {
  std::string data_dir = "./data_bench";   // 数据目录
  std::string table = "bench_table";       // 表名
  size_t rows = 10000;                     // 初始化行数
  size_t ops = 10000;                      // 压测操作次数
  int read_ratio = 70;                     // 读比例
  int update_ratio = 20;                   // 更新比例
  int delete_ratio = 10;                   // 删除比例
  bool reset = true;                       // 是否清空旧表
  int numa_nodes = 2;                      // NUMA 节点数
  size_t cache_pages = 256;                // 缓存页数
};

// 解析 size_t 类型参数（只允许正整数）。
bool parse_size(const std::string& value, size_t* out) {
  if (!out) {
    return false;
  }
  if (value.empty()) {
    return false;
  }
  if (!mini_db::is_number(value)) {
    return false;
  }
  *out = static_cast<size_t>(std::stoull(value));
  return true;
}

// 解析 int 类型参数（只允许正整数）。
bool parse_int(const std::string& value, int* out) {
  if (!out) {
    return false;
  }
  if (value.empty()) {
    return false;
  }
  if (!mini_db::is_number(value)) {
    return false;
  }
  *out = std::stoi(value);
  return true;
}

// 打印压测工具的使用说明。
void print_usage() {
  std::cout
      << "mini_db_bench usage:\n"
      << "  --rows=N           初始化数据行数 (default 10000)\n"
      << "  --ops=N            压测操作次数 (default 10000)\n"
      << "  --read=PCT         读比例 (default 70)\n"
      << "  --update=PCT       更新比例 (default 20)\n"
      << "  --delete=PCT       删除比例 (default 10)\n"
      << "  --data=PATH        数据目录 (default ./data_bench)\n"
      << "  --table=NAME       表名 (default bench_table)\n"
      << "  --cache=N          缓存页数 (default 256)\n"
      << "  --numa=N           NUMA 节点数 (default 2)\n"
      << "  --no-reset         不清空旧表 (默认会重建表)\n";
}

// 解析命令行参数并写入配置结构。
bool parse_args(int argc, char** argv, BenchConfig* config) {
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
    } else if (key == "--ops") {
      if (!parse_size(value, &config->ops)) {
        return false;
      }
    } else if (key == "--read") {
      if (!parse_int(value, &config->read_ratio)) {
        return false;
      }
    } else if (key == "--update") {
      if (!parse_int(value, &config->update_ratio)) {
        return false;
      }
    } else if (key == "--delete") {
      if (!parse_int(value, &config->delete_ratio)) {
        return false;
      }
    } else if (key == "--data") {
      config->data_dir = value;
    } else if (key == "--table") {
      config->table = value;
    } else if (key == "--cache") {
      if (!parse_size(value, &config->cache_pages)) {
        return false;
      }
    } else if (key == "--numa") {
      if (!parse_int(value, &config->numa_nodes)) {
        return false;
      }
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      return false;
    }
  }
  return true;
}

// 构造 value 列的字符串内容，便于更新/插入。
mini_db::Value make_value(int id) {
  return mini_db::Value::Text("value_" + std::to_string(id));
}

}  // namespace

int main(int argc, char** argv) {
  // 1) 解析配置参数。
  BenchConfig config;
  if (!parse_args(argc, argv, &config)) {
    return 1;
  }

  // 2) 校验比例合法性。
  int ratio_sum = config.read_ratio + config.update_ratio + config.delete_ratio;
  if (ratio_sum <= 0) {
    std::cerr << "Invalid ratios" << "\n";
    return 1;
  }

  // 3) 初始化数据库实例。
  mini_db::Database db(config.data_dir, 4096, config.cache_pages, config.numa_nodes);
  std::string err;
  if (!db.open(&err)) {
    std::cerr << "Failed to open database: " << err << "\n";
    return 1;
  }
  std::cout << "Buffer pool fixed at init. NUMA nodes: " << config.numa_nodes
            << ", page->node: page_id % " << config.numa_nodes << "\n";

  // 4) 准备测试表。
  if (config.reset) {
    db.drop_table(config.table, &err);
    err.clear();
  }

  std::vector<mini_db::Column> columns;
  columns.push_back({"id", mini_db::ColumnType::Int, 0});
  columns.push_back({"value", mini_db::ColumnType::Text, 32});
  if (!db.create_table(config.table, columns, &err)) {
    // 如果表已存在，直接继续。
    err.clear();
  }

  // 5) 装载初始数据。
  std::cout << "Loading " << config.rows << " rows...\n";
  for (size_t i = 0; i < config.rows; ++i) {
    std::vector<mini_db::Value> values;
    values.push_back(mini_db::Value::Int(static_cast<int32_t>(i + 1)));
    values.push_back(make_value(static_cast<int>(i + 1)));
    uint64_t row_id = 0;
    if (!db.insert(config.table, values, &row_id, &err)) {
      std::cerr << "Insert failed: " << err << "\n";
      return 1;
    }
  }

  // 6) 初始化随机数生成器与负载分布。
  std::mt19937 rng(static_cast<unsigned int>(std::chrono::steady_clock::now()
                                                 .time_since_epoch()
                                                 .count()));
  std::uniform_int_distribution<int> key_dist(1, static_cast<int>(config.rows));
  std::uniform_int_distribution<int> op_dist(1, ratio_sum);

  size_t read_count = 0;
  size_t update_count = 0;
  size_t delete_count = 0;
  size_t query_count = 0;
  std::vector<double> latencies_ms;
  latencies_ms.reserve(config.ops);

  // 7) 压测执行：按比例随机执行读/更新/删除+回插。
  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < config.ops; ++i) {
    auto op_start = std::chrono::steady_clock::now();
    int key = key_dist(rng);
    int op = op_dist(rng);
    if (op <= config.read_ratio) {
      // 读操作：按 id 等值查询。
      mini_db::Condition where;
      where.has = true;
      where.column = "id";
      where.value = mini_db::Value::Int(key);
      std::vector<std::vector<mini_db::Value>> rows;
      if (!db.select(config.table, where, &rows, &err)) {
        std::cerr << "Select failed: " << err << "\n";
        return 1;
      }
      ++read_count;
      query_count += 1;
    } else if (op <= config.read_ratio + config.update_ratio) {
      // 更新操作：更新 value 列。
      mini_db::SetClause set;
      set.column = "value";
      set.value = make_value(static_cast<int>(i));
      mini_db::Condition where;
      where.has = true;
      where.column = "id";
      where.value = mini_db::Value::Int(key);
      size_t updated = 0;
      if (!db.update(config.table, {set}, where, &updated, &err)) {
        std::cerr << "Update failed: " << err << "\n";
        return 1;
      }
      ++update_count;
      query_count += 1;
    } else {
      // 删除操作：删除后回插，保持数据规模稳定。
      mini_db::Condition where;
      where.has = true;
      where.column = "id";
      where.value = mini_db::Value::Int(key);
      size_t removed = 0;
      if (!db.remove(config.table, where, &removed, &err)) {
        std::cerr << "Delete failed: " << err << "\n";
        return 1;
      }
      // 删除后重新插入，保持数据规模稳定。
      std::vector<mini_db::Value> values;
      values.push_back(mini_db::Value::Int(key));
      values.push_back(make_value(key));
      uint64_t row_id = 0;
      if (!db.insert(config.table, values, &row_id, &err)) {
        std::cerr << "Re-insert failed: " << err << "\n";
        return 1;
      }
      ++delete_count;
      query_count += 2;
    }
    auto op_end = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> op_elapsed = op_end - op_start;
    latencies_ms.push_back(op_elapsed.count());
  }
  auto end = std::chrono::steady_clock::now();

  // 8) 汇总统计并输出结果。
  std::chrono::duration<double> elapsed = end - start;
  double seconds = elapsed.count();
  double tps = seconds > 0.0 ? static_cast<double>(config.ops) / seconds : 0.0;
  double qps = seconds > 0.0 ? static_cast<double>(query_count) / seconds : 0.0;

  double p99 = 0.0;
  if (!latencies_ms.empty()) {
    size_t idx = static_cast<size_t>(0.99 * (latencies_ms.size() - 1));
    std::nth_element(latencies_ms.begin(), latencies_ms.begin() + idx, latencies_ms.end());
    p99 = latencies_ms[idx];
  }

  std::cout << "\nBenchmark finished:\n";
  std::cout << "  total_ops:   " << config.ops << "\n";
  std::cout << "  read_ops:    " << read_count << "\n";
  std::cout << "  update_ops:  " << update_count << "\n";
  std::cout << "  delete_ops:  " << delete_count << "\n";
  std::cout << "  total_qry:   " << query_count << "\n";
  std::cout << "  elapsed:     " << seconds << " s\n";
  std::cout << "  tps:         " << tps << " ops/s\n";
  std::cout << "  qps:         " << qps << " queries/s\n";
  std::cout << "  p99:         " << p99 << " ms\n";

  // 9) 关闭数据库。
  db.close(&err);
  return 0;
}
