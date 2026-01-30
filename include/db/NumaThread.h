#pragma once

#include <string>

namespace mini_db {

// 将当前线程绑定到指定 NUMA 节点的 CPU 列表；失败时返回 false。
bool bind_thread_to_node(int node, std::string* err);

}  // namespace mini_db
