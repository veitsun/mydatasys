#include "db/NumaThread.h"

#include "db/Numa.h"

#ifdef HAVE_LIBNUMA
#include <numa.h>
#endif

namespace mini_db {

bool bind_thread_to_node(int node, std::string* err) {
#ifdef HAVE_LIBNUMA
  // 使用 libnuma 将当前线程绑定到指定节点。
  if (numa_available() < 0) {
    if (err) {
      *err = "libnuma not available";
    }
    return false;
  }
  if (numa_run_on_node(node) != 0) {
    if (err) {
      *err = "failed to bind thread to NUMA node";
    }
    return false;
  }
  return true;
#else
  // 未启用 libnuma 时不可绑定，返回失败。
  if (err) {
    *err = "NUMA thread binding disabled";
  }
  return false;
#endif
}

}  // namespace mini_db
