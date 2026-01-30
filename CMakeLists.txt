cmake_minimum_required(VERSION 3.10)
project(mini_db LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(COMMON_SOURCES
  src/Utils.cpp
  src/Schema.cpp
  src/Pager.cpp
  src/Cache.cpp
  src/PagedFile.cpp
  src/Buffer.cpp
  src/BufferPool.cpp
  src/Numa.cpp
  src/NumaExecutor.cpp
  src/NumaThread.cpp
  src/LogManager.cpp
  src/TableStorage.cpp
  src/Catalog.cpp
  src/Database.cpp
  src/SqlParser.cpp
  src/Executor.cpp
)

add_executable(mini_db
  src/main.cpp
  ${COMMON_SOURCES}
)

add_executable(mini_db_bench
  tools/bench/bench.cpp
  ${COMMON_SOURCES}
)

target_include_directories(mini_db PRIVATE include)
target_include_directories(mini_db_bench PRIVATE include)

# 线程库依赖（std::thread / shared_mutex 需要 pthread）。
find_package(Threads REQUIRED)
target_link_libraries(mini_db PRIVATE Threads::Threads)
target_link_libraries(mini_db_bench PRIVATE Threads::Threads)

# 可选 NUMA 支持：若系统提供 libnuma，则启用真实的 NUMA 分配与拓扑查询。
find_path(NUMA_INCLUDE_DIR numa.h)
find_library(NUMA_LIB numa)
if (NUMA_INCLUDE_DIR AND NUMA_LIB)
  target_include_directories(mini_db PRIVATE ${NUMA_INCLUDE_DIR})
  target_link_libraries(mini_db PRIVATE ${NUMA_LIB})
  target_compile_definitions(mini_db PRIVATE HAVE_LIBNUMA=1)
  target_include_directories(mini_db_bench PRIVATE ${NUMA_INCLUDE_DIR})
  target_link_libraries(mini_db_bench PRIVATE ${NUMA_LIB})
  target_compile_definitions(mini_db_bench PRIVATE HAVE_LIBNUMA=1)
endif()
