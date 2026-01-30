#pragma once

#include "db/NumaThread.h"

#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace mini_db {

// NUMA 线程执行器：每个 NUMA 节点有固定线程组与任务队列。
class NumaExecutor {
 public:
  NumaExecutor(int nodes, int threads_per_node);
  ~NumaExecutor();

  NumaExecutor(const NumaExecutor&) = delete;
  NumaExecutor& operator=(const NumaExecutor&) = delete;

  // 启动工作线程。
  void start();
  // 停止线程并等待退出。
  void stop();
  // 返回节点数量。
  int node_count() const;

  // 提交任务到指定节点的队列；返回 future 供调用方等待。
  template <typename Fn>
  auto submit(int node, Fn&& fn) -> std::future<decltype(fn())> {
    using Result = decltype(fn());
    auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<Fn>(fn)); // 把传入的函数封装成 packaged_task 可以绑定到一个 future ， 执行时会把结果写入这个 future
    auto future = task->get_future();     // 获取未来结果句柄返回给调用方
    if (!running_) { // 如果执行器尚未启动，就在当前线程同步执行，避免任务丢失，同时依然返回 future，保证接口一致
      (*task)();
      return future;
    }
    enqueue(node, [task]() mutable { (*task)(); }); // 把任务刨床成一个 void() 的可调用对象塞进队列；工作线程执行时就出发 packaged_task ，结果写入 future
    return future;
  }

 private:
  struct WorkerGroup {
    int node = 0;
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<std::function<void()>> tasks;
    bool stop = false;
    std::vector<std::thread> threads;
  };

  // 将任务放入对应节点的队列。
  void enqueue(int node, std::function<void()> task);
  // 节点工作线程主循环。
  void worker_loop(WorkerGroup* group);

  int nodes_ = 1;
  int threads_per_node_ = 1;
  std::vector<std::unique_ptr<WorkerGroup>> groups_;
  bool running_ = false;
};

}  // namespace mini_db
