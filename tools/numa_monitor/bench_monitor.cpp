#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

// 运行配置：压测程序路径、监控程序路径、刷新间隔与压测参数。
struct Config {
  std::string bench_path = "./mini_db_bench";
  std::string monitor_path = "./mini_db_numa_monitor";
  int interval_ms = 1000;
  bool once = false;
  std::vector<std::string> bench_args;
};

bool parse_int(const std::string& value, int* out) {
  if (!out || value.empty()) {
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

void print_usage() {
  std::cout
      << "mini_db_bench_monitor usage:\n"
      << "  --bench=PATH            压测程序路径 (default ./mini_db_bench)\n"
      << "  --monitor=PATH          监控程序路径 (default ./mini_db_numa_monitor)\n"
      << "  --interval-ms=MS        监控刷新间隔 (default 1000)\n"
      << "  --once                  监控只输出一次后退出\n"
      << "  --                      分隔符，后续参数传给压测程序\n"
      << "  -h/--help               显示帮助\n"
      << "\n"
      << "example:\n"
      << "  ./mini_db_bench_monitor --interval-ms=1000 -- --rows=10000 --ops=200000\n";
}

bool parse_args(int argc, char** argv, Config* config) {
  if (!config) {
    return false;
  }
  bool bench_mode = false;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (bench_mode) {
      config->bench_args.push_back(arg);
      continue;
    }
    if (arg == "--") {
      bench_mode = true;
      continue;
    }
    if (arg == "--help" || arg == "-h") {
      return false;
    }
    std::string key;
    std::string value;
    if (arg.rfind("--bench=", 0) == 0) {
      key = "--bench";
      value = arg.substr(8);
    } else if (arg.rfind("--monitor=", 0) == 0) {
      key = "--monitor";
      value = arg.substr(10);
    } else if (arg.rfind("--interval-ms=", 0) == 0) {
      key = "--interval-ms";
      value = arg.substr(14);
    } else if (arg == "--bench" || arg == "--monitor" || arg == "--interval-ms") {
      if (i + 1 >= argc) {
        return false;
      }
      key = arg;
      value = argv[++i];
    } else if (arg == "--once") {
      config->once = true;
      continue;
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      return false;
    }

    if (key == "--bench") {
      config->bench_path = value;
    } else if (key == "--monitor") {
      config->monitor_path = value;
    } else if (key == "--interval-ms") {
      if (!parse_int(value, &config->interval_ms)) {
        return false;
      }
    }
  }
  return true;
}

std::vector<char*> build_argv(const std::string& path, const std::vector<std::string>& args) {
  std::vector<char*> result;
  result.reserve(args.size() + 2);
  result.push_back(const_cast<char*>(path.c_str()));
  for (const auto& arg : args) {
    result.push_back(const_cast<char*>(arg.c_str()));
  }
  result.push_back(nullptr);
  return result;
}

int wait_child(pid_t pid, const std::string& name) {
  int status = 0;
  if (waitpid(pid, &status, 0) < 0) {
    std::cerr << "Failed to wait for " << name << ": " << std::strerror(errno) << "\n";
    return 1;
  }
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  if (WIFSIGNALED(status)) {
    std::cerr << name << " terminated by signal " << WTERMSIG(status) << "\n";
    return 1;
  }
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  Config config;
  if (!parse_args(argc, argv, &config)) {
    print_usage();
    return 1;
  }

  pid_t bench_pid = fork();
  if (bench_pid < 0) {
    std::cerr << "Failed to fork bench: " << std::strerror(errno) << "\n";
    return 1;
  }
  if (bench_pid == 0) {
    std::vector<char*> bench_argv = build_argv(config.bench_path, config.bench_args);
    execvp(bench_argv[0], bench_argv.data());
    std::cerr << "Failed to exec bench: " << std::strerror(errno) << "\n";
    _exit(127);
  }

  pid_t monitor_pid = fork();
  if (monitor_pid < 0) {
    std::cerr << "Failed to fork monitor: " << std::strerror(errno) << "\n";
  } else if (monitor_pid == 0) {
    std::vector<std::string> args;
    args.push_back("--pid=" + std::to_string(static_cast<long long>(bench_pid)));
    args.push_back("--interval-ms=" + std::to_string(config.interval_ms));
    if (config.once) {
      args.push_back("--once");
    }
    std::vector<char*> monitor_argv = build_argv(config.monitor_path, args);
    execvp(monitor_argv[0], monitor_argv.data());
    std::cerr << "Failed to exec monitor: " << std::strerror(errno) << "\n";
    _exit(127);
  }

  std::cout << "Bench PID: " << bench_pid << "\n";
  if (monitor_pid > 0) {
    std::cout << "Monitor PID: " << monitor_pid << "\n";
  }

  int bench_code = wait_child(bench_pid, "bench");
  if (monitor_pid > 0) {
    // 压测结束后停止监控进程。
    if (kill(monitor_pid, SIGTERM) == 0) {
      wait_child(monitor_pid, "monitor");
    }
  }
  return bench_code;
}
