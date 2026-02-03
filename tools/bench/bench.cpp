#include "db/Database.h"
#include "db/NumaExecutor.h"
#include "db/Types.h"
#include "db/Utils.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <deque>
#include <future>
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
  int threads_per_node = 1;                // 每个节点的工作线程数
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
      << "  --threads-per-node=N 每个 NUMA 节点线程数 (default 1)\n"
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
    } else if (key == "--threads-per-node") {
      if (!parse_int(value, &config->threads_per_node)) {
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

struct TaskResult {
  bool ok = true;
  std::string err;
};

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
  if (config.numa_nodes <= 0) {
    config.numa_nodes = 1;
  }
  if (config.threads_per_node <= 0) {
    config.threads_per_node = 1;
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
  std::cout << "Worker threads per node: " << config.threads_per_node << "\n";
  {
    std::vector<size_t> pages = db.cached_pages_per_node();
    std::cout << "Buffer pool pages per NUMA node:";
    for (size_t i = 0; i < pages.size(); ++i) {
      std::cout << " N" << i << "=" << pages[i];
    }
    std::cout << "\n";
  }

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

  mini_db::Schema schema;
  if (!db.get_schema(config.table, &schema, &err)) {
    std::cerr << "Failed to get schema: " << err << "\n";
    return 1;
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
  {
    std::vector<size_t> pages = db.cached_pages_per_node();
    std::cout << "Buffer pool pages per NUMA node after load:";
    for (size_t i = 0; i < pages.size(); ++i) {
      std::cout << " N" << i << "=" << pages[i];
    }
    std::cout << "\n";
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

  mini_db::NumaExecutor executor(config.numa_nodes, config.threads_per_node);
  executor.start();

  struct Pending {
    std::future<TaskResult> future;
    std::chrono::steady_clock::time_point start;
  };
  std::deque<Pending> pending;
  const size_t max_inflight = 1024;
  size_t page_size = db.page_size();
  size_t record_size = schema.record_size();

  // 7) 压测执行：按比例随机执行读/更新/删除+回插（按页归属路由到节点队列）。
  const std::string table_name = config.table;
  auto start = std::chrono::steady_clock::now();    // 开始计时
  for (size_t i = 0; i < config.ops; ++i) {
    int key = key_dist(rng);
    uint64_t row_id = static_cast<uint64_t>(key - 1);
    size_t page_id = (page_size + row_id * record_size) / page_size;
    int node = static_cast<int>(page_id % static_cast<size_t>(config.numa_nodes));
    int op = op_dist(rng);
    auto op_start = std::chrono::steady_clock::now();
    if (op <= config.read_ratio) {
      // 读操作：按行号读取，失效行视为成功。
      auto future = executor.submit(node, [&db, table_name, row_id]() -> TaskResult {
        TaskResult result;
        std::vector<mini_db::Value> values;
        bool valid = false;
        std::string err;
        if (!db.read_row(table_name, row_id, &values, &valid, &err)) {
          result.ok = false;
          result.err = err;
          return result;
        }
        return result;
      });
      pending.push_back({std::move(future), op_start});
      ++read_count;
      query_count += 1;
    } else if (op <= config.read_ratio + config.update_ratio) {
      // 更新操作：按行号更新 value 列。
      mini_db::SetClause set;
      set.column = "value";
      set.value = make_value(static_cast<int>(i));
      auto future = executor.submit(node, [&db, table_name, row_id, set]() -> TaskResult {
        TaskResult result;
        std::string err;
        if (!db.update_row(table_name, row_id, {set}, &err)) {
          if (err != "row is deleted") {
            result.ok = false;
            result.err = err;
          }
        }
        return result;
      });
      pending.push_back({std::move(future), op_start});
      ++update_count;
      query_count += 1;
    } else {
      // 删除操作：删除后回插，保持数据规模稳定。
      std::vector<mini_db::Value> values;
      values.push_back(mini_db::Value::Int(key));
      values.push_back(make_value(key));
      auto future = executor.submit(node, [&db, table_name, row_id, values]() -> TaskResult {
        TaskResult result;
        std::string err;
        if (!db.delete_row(table_name, row_id, &err)) {
          if (err != "row is deleted") {
            result.ok = false;
            result.err = err;
            return result;
          }
        }
        if (!db.write_row(table_name, row_id, values, true, &err)) {
          result.ok = false;
          result.err = err;
        }
        return result;
      });
      pending.push_back({std::move(future), op_start});
      ++delete_count;
      query_count += 2;
    }

    if (pending.size() >= max_inflight) {
      Pending front = std::move(pending.front());
      pending.pop_front();
      TaskResult result = front.future.get();
      auto finish = std::chrono::steady_clock::now();
      std::chrono::duration<double, std::milli> op_elapsed = finish - front.start;
      latencies_ms.push_back(op_elapsed.count());
      if (!result.ok) {
        std::cerr << "Operation failed: " << result.err << "\n";
        return 1;
      }
    }
  }

  while (!pending.empty()) {
    Pending front = std::move(pending.front());
    pending.pop_front();
    TaskResult result = front.future.get();
    auto finish = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> op_elapsed = finish - front.start;
    latencies_ms.push_back(op_elapsed.count());
    if (!result.ok) {
      std::cerr << "Operation failed: " << result.err << "\n";
      return 1;
    }
  }
  auto end = std::chrono::steady_clock::now();  // 结束计时
  executor.stop();

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
  {
    std::vector<size_t> pages = db.cached_pages_per_node();
    std::cout << "Buffer pool pages per NUMA node after benchmark:";
    for (size_t i = 0; i < pages.size(); ++i) {
      std::cout << " N" << i << "=" << pages[i];
    }
    std::cout << "\n";
  }

  // 9) 关闭数据库。
  db.close(&err);
  return 0;
}
