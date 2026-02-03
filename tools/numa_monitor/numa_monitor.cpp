#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <unistd.h>

namespace {

// 配置参数：被监控的 PID 与刷新间隔。
struct Config {
  int pid = 0;
  int interval_ms = 1000;
  bool once = false;
};

// 将字符串解析为正整数。
bool parse_int(const std::string& value, int* out) {
  if (!out) {
    return false;
  }
  if (value.empty()) {
    return false;
  }
  char* end = nullptr;
  long parsed = std::strtol(value.c_str(), &end, 10);
  if (end == value.c_str() || parsed <= 0) {
    return false;
  }
  *out = static_cast<int>(parsed);
  return true;
}

// 解析命令行参数。
bool parse_args(int argc, char** argv, Config* config) {
  if (!config) {
    return false;
  }
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      return false;
    }
    if (arg == "--once") {
      config->once = true;
      continue;
    }
    std::string key;
    std::string value;
    if (arg.rfind("--pid=", 0) == 0) {
      key = "--pid";
      value = arg.substr(6);
    } else if (arg.rfind("--interval-ms=", 0) == 0) {
      key = "--interval-ms";
      value = arg.substr(14);
    } else if (arg == "--pid" || arg == "--interval-ms") {
      if (i + 1 >= argc) {
        return false;
      }
      key = arg;
      value = argv[++i];
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      return false;
    }

    if (key == "--pid") {
      if (!parse_int(value, &config->pid)) {
        return false;
      }
    } else if (key == "--interval-ms") {
      if (!parse_int(value, &config->interval_ms)) {
        return false;
      }
    }
  }
  return config->pid > 0;
}

// 打印使用说明。
void print_usage() {
  std::cout
      << "mini_db_numa_monitor usage:\n"
      << "  --pid=PID               目标进程 PID（必填）\n"
      << "  --interval-ms=MS        刷新间隔，毫秒 (default 1000)\n"
      << "  --once                  只输出一次后退出\n"
      << "  -h/--help               显示帮助\n";
}

// 解析 /proc/<pid>/numa_maps，汇总各节点页数。
bool parse_numa_maps(int pid, std::map<int, long long>* pages_by_node,
                     long long* total_pages, std::string* err) {
  if (!pages_by_node || !total_pages) {
    if (err) {
      *err = "output missing";
    }
    return false;
  }
  pages_by_node->clear();
  *total_pages = 0;
  std::string path = "/proc/" + std::to_string(pid) + "/numa_maps";
  std::ifstream file(path);
  if (!file.is_open()) {
    if (err) {
      *err = "failed to open " + path;
    }
    return false;
  }
  std::string line;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
      if (token.size() < 3 || token[0] != 'N') {
        continue;
      }
      size_t eq = token.find('=');
      if (eq == std::string::npos || eq <= 1 || eq + 1 >= token.size()) {
        continue;
      }
      std::string node_str = token.substr(1, eq - 1);
      std::string count_str = token.substr(eq + 1);
      char* end = nullptr;
      long node = std::strtol(node_str.c_str(), &end, 10);
      if (end == node_str.c_str() || node < 0) {
        continue;
      }
      end = nullptr;
      long long count = std::strtoll(count_str.c_str(), &end, 10);
      if (end == count_str.c_str() || count < 0) {
        continue;
      }
      (*pages_by_node)[static_cast<int>(node)] += count;
      *total_pages += count;
    }
  }
  return true;
}

// 解析 /proc/<pid>/numastat，读取各指标的节点计数。
bool parse_numa_stat(int pid, std::unordered_map<std::string, std::vector<long long>>* metrics,
                     std::string* err) {
  if (!metrics) {
    if (err) {
      *err = "output missing";
    }
    return false;
  }
  metrics->clear();
  std::string path = "/proc/" + std::to_string(pid) + "/numastat";
  std::ifstream file(path);
  if (!file.is_open()) {
    if (err) {
      *err = "failed to open " + path + ": " + std::strerror(errno);
    }
    return false;
  }
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    std::istringstream iss(line);
    std::string name;
    iss >> name;
    if (name.empty()) {
      continue;
    }
    std::vector<long long> values;
    long long value = 0;
    while (iss >> value) {
      values.push_back(value);
    }
    if (!values.empty()) {
      (*metrics)[name] = std::move(values);
    }
  }
  return true;
}

// 获取当前解析结果的最大节点数。
int detect_node_count(const std::map<int, long long>& pages_by_node,
                      const std::unordered_map<std::string, std::vector<long long>>& metrics) {
  int max_node = -1;
  for (const auto& pair : pages_by_node) {
    if (pair.first > max_node) {
      max_node = pair.first;
    }
  }
  for (const auto& pair : metrics) {
    int last_index = static_cast<int>(pair.second.size()) - 1;
    if (last_index > max_node) {
      max_node = last_index;
    }
  }
  return max_node >= 0 ? max_node + 1 : 1;
}

// 将 vector 补齐到指定节点数。
std::vector<long long> normalize_vector(const std::vector<long long>& input, int nodes) {
  std::vector<long long> result(static_cast<size_t>(nodes), 0);
  for (int i = 0; i < nodes && i < static_cast<int>(input.size()); ++i) {
    result[static_cast<size_t>(i)] = input[static_cast<size_t>(i)];
  }
  return result;
}

// 格式化输出每节点数据。
void print_node_values(const std::vector<double>& values, const std::string& suffix) {
  for (size_t i = 0; i < values.size(); ++i) {
    std::cout << " N" << i << "=" << std::fixed << std::setprecision(2) << values[i] << suffix;
  }
  std::cout << "\n";
}

}  // namespace

int main(int argc, char** argv) {
  Config config;
  if (!parse_args(argc, argv, &config)) {
    print_usage();
    return 1;
  }
  long page_size = sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    page_size = 4096;
  }
  std::unordered_map<std::string, std::vector<long long>> prev_metrics;
  bool has_prev = false;
  bool numastat_warned = false;

  while (true) {
    std::map<int, long long> pages_by_node;
    long long total_pages = 0;
    std::unordered_map<std::string, std::vector<long long>> metrics;
    std::string err;
    if (!parse_numa_maps(config.pid, &pages_by_node, &total_pages, &err)) {
      std::cerr << "numa_maps error: " << err << "\n";
      return 1;
    }
    err.clear();
    bool numastat_ok = parse_numa_stat(config.pid, &metrics, &err);
    if (!numastat_ok) {
      if (!numastat_warned) {
        std::cerr << "numastat unavailable: " << err << "\n";
        numastat_warned = true;
      }
      metrics.clear();
      prev_metrics.clear();
      has_prev = false;
    }

    int nodes = detect_node_count(pages_by_node, metrics);
    std::vector<long long> mem_pages(static_cast<size_t>(nodes), 0);
    for (const auto& pair : pages_by_node) {
      if (pair.first >= 0 && pair.first < nodes) {
        mem_pages[static_cast<size_t>(pair.first)] = pair.second;
      }
    }

    std::cout << "PID " << config.pid << " | interval " << config.interval_ms << " ms\n";
    std::cout << "Memory usage by NUMA node (MB):\n";
    std::vector<double> mem_mb;
    mem_mb.reserve(mem_pages.size());
    double total_mb = 0.0;
    for (long long pages : mem_pages) {
      double mb = static_cast<double>(pages) * static_cast<double>(page_size) / 1024.0 / 1024.0;
      mem_mb.push_back(mb);
      total_mb += mb;
    }
    print_node_values(mem_mb, "MB");
    std::cout << "Total: " << std::fixed << std::setprecision(2) << total_mb << "MB\n";

    if (metrics.empty()) {
      std::cout << "NUMA access stats: unavailable\n";
    } else {
      std::cout << "NUMA access stats (";
      if (has_prev) {
        std::cout << "delta per sec";
      } else {
        std::cout << "total";
      }
      std::cout << "):\n";

      const std::vector<std::string> keys = {
          "numa_hit", "numa_miss", "numa_foreign", "interleave_hit", "local_node", "other_node"};
      double interval_sec = static_cast<double>(config.interval_ms) / 1000.0;
      std::vector<long long> local_delta(nodes, 0);
      std::vector<long long> other_delta(nodes, 0);

      for (const auto& key : keys) {
        auto it = metrics.find(key);
        if (it == metrics.end()) {
          continue;
        }
        std::vector<long long> current = normalize_vector(it->second, nodes);
        std::vector<long long> values(current.size(), 0);
        if (has_prev) {
          auto prev_it = prev_metrics.find(key);
          std::vector<long long> prev = prev_it == prev_metrics.end()
                                            ? std::vector<long long>(current.size(), 0)
                                            : normalize_vector(prev_it->second, nodes);
          for (size_t i = 0; i < current.size(); ++i) {
            values[i] = current[i] - prev[i];
          }
        } else {
          values = current;
        }

        std::cout << "  " << key << ":";
        std::vector<double> line(values.size(), 0.0);
        for (size_t i = 0; i < values.size(); ++i) {
          line[i] = has_prev ? static_cast<double>(values[i]) / interval_sec
                             : static_cast<double>(values[i]);
        }
        print_node_values(line, has_prev ? "/s" : "");

        if (key == "local_node") {
          local_delta = values;
        } else if (key == "other_node") {
          other_delta = values;
        }
      }

      if (!local_delta.empty() && !other_delta.empty()) {
        std::cout << "  remote_ratio:";
        std::vector<double> ratios(static_cast<size_t>(nodes), 0.0);
        for (int i = 0; i < nodes; ++i) {
          long long local = local_delta[static_cast<size_t>(i)];
          long long other = other_delta[static_cast<size_t>(i)];
          long long total = local + other;
          ratios[static_cast<size_t>(i)] =
              total > 0 ? static_cast<double>(other) * 100.0 / static_cast<double>(total) : 0.0;
        }
        print_node_values(ratios, "%");
      }
    }

    std::cout << "----\n";
    if (!metrics.empty()) {
      prev_metrics = metrics;
      has_prev = true;
    }

    if (config.once) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(config.interval_ms));
  }
  return 0;
}
