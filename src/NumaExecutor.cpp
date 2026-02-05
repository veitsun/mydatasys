#include "db/NumaExecutor.h"

#include "db/Numa.h"

namespace mini_db {

NumaExecutor::NumaExecutor(int nodes, int threads_per_node)
    : nodes_(nodes > 0 ? nodes : 1),
      threads_per_node_(threads_per_node > 0 ? threads_per_node : 1) {}

NumaExecutor::~NumaExecutor() {
  stop();
}


void NumaExecutor::start() {
  // 启动每个 NUMA 节点的线程组。
  // 如果这里 start() 被多个线程同时调用，这里不是线程安全的
  if (running_) {
    // 如果已经启动了，就直接返回，避免重复创建线程
    return;
  }
  running_ = true;
  // 清空并预分配 groups_
  groups_.clear();
  groups_.reserve(static_cast<size_t>(nodes_));
  for (int node = 0; node < nodes_; ++node) {
    // 为每个 NUMA 节点创建一个 WorkerGroup
    auto group = std::make_unique<WorkerGroup>();
    group->node = node;  // 记录这个组属于哪个 numa 节点（后续可能用于绑核/绑内存）
    group->stop = false; // 初始化停止标志，让 worker_loop 能正常跑
    group->threads.reserve(static_cast<size_t>(threads_per_node_)); // 每个 WorkerGroup 里启动 threads_per_node_ 个工作线程
    for (int i = 0; i < threads_per_node_; ++i) {
      // 每个节点启动 threads_per_node 个工作线程。
      WorkerGroup* group_ptr = group.get();
      // 每个工作线程都跑同一个 worker_loop(group_ptr) ，从对应 group 的任务队列里取任务执行
      group->threads.emplace_back([this, group_ptr]() { worker_loop(group_ptr); });
    }
    groups_.push_back(std::move(group));
  }
}

void NumaExecutor::stop() {
  // 停止所有节点的线程组，并等待退出。
  if (!running_) {
    return;
  }
  for (auto& group_ptr : groups_) {
    WorkerGroup& group = *group_ptr;
    {
      std::lock_guard<std::mutex> lock(group.mutex);
      group.stop = true;
    }
    group.cv.notify_all();
  }
  for (auto& group_ptr : groups_) {
    WorkerGroup& group = *group_ptr;
    for (auto& thread : group.threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    group.threads.clear();
  }
  groups_.clear();
  running_ = false;
}

int NumaExecutor::node_count() const {
  return nodes_;
}

void NumaExecutor::enqueue(int node, std::function<void()> task) {
  // 将任务路由到指定节点队列。
  if (!running_) {
    return;
  }
  int target = node;
  if (target < 0) {
    target = 0;
  }
  if (target >= nodes_) {
    target = target % nodes_;
  }
  WorkerGroup& group = *groups_[static_cast<size_t>(target)];
  {
    std::lock_guard<std::mutex> lock(group.mutex);  // 加锁保护队列
    group.tasks.push_back(std::move(task));         // 把任务入队 FIFO
  }
  group.cv.notify_one();    // 唤醒该节点上一个等待中的工作线程来执行任务
}

/**
 * @brief worker_loop 是每个 NUMA 节点工作线程的主循环，职责是绑定线程 + 循环取任务执行
 * 
 * @param group 
 */
void NumaExecutor::worker_loop(WorkerGroup* group) {
  if (!group) {
    return;
  }
  // 将线程绑定到对应 NUMA 节点。
  std::string err;
  bind_thread_to_node(group->node, &err);
  // 线程绑定到固定 NUMA 节点后，按队列顺序执行任务，只有在 stop 且队列空时退出
  for (;;) {
    std::function<void()> task;
    {
      // 在 lock 的作用域内，当前线程独占 group->mutex ，其他线程如果也试图锁同一个 mutext，会被阻塞（同一个 group 对象里只有一把锁 mutex）
      std::unique_lock<std::mutex> lock(group->mutex);
      group->cv.wait(lock, [&group]() { return group->stop || !group->tasks.empty(); });
      if (group->stop && group->tasks.empty()) {
        return;
      }

      // 从队列头取出一个任务并移除
      task = std::move(group->tasks.front());
      group->tasks.pop_front();
    }
    // 任务从队列中取出后，就由该worker线程同步执行，执行完后才会取下一个
    if (task) {
      task();
    }
  }
}

}  // namespace mini_db
