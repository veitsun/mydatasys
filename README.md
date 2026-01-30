MiniRDB (C++)

A minimal relational database with paging, storage, DDL/DML, cache+log, and a tiny SQL parser.

Build

- mkdir -p build
- cd build
- cmake ..
- cmake --build .

Run

- ./mini_db
- MINI_DB_NUMA_NODES=2 ./mini_db
- ./mini_db_bench --rows=10000 --ops=10000 --read=70 --update=20 --delete=10 --data=./data_bench --table=bench_table --cache=256 --numa=2 --threads-per-node=2

Supported SQL (case-insensitive)

- CREATE TABLE t (id INT, name TEXT(32));
- DROP TABLE t;
- ALTER TABLE t ADD COLUMN age INT;
- INSERT INTO t VALUES (1, "alice");
- SELECT * FROM t;
- SELECT * FROM t WHERE id = 1;
- UPDATE t SET name = "bob" WHERE id = 1;
- DELETE FROM t WHERE id = 1;

Notes

- TEXT uses fixed length storage; values longer than the column length are rejected.
- Data files are stored under ./data (catalog.meta, db.log, and *.tbl).
- NUMA nodes are configurable via MINI_DB_NUMA_NODES (default: 2). If libnuma is available, the buffer pool allocates pages on the chosen node.
- mini_db_bench is a local sysbench-like benchmark tool (multi-threaded, NUMA-aware). It reports TPS, QPS, and P99 latency.
- NumaExecutor provides per-node worker queues; benchmark operations are routed by page_id to avoid cross-node migration. Thread binding uses libnuma when available, otherwise it falls back to OS scheduling.

---

项目文件说明（中文）

构建与入口

- CMakeLists.txt: CMake 构建脚本，定义目标与源文件。
- README.md: 项目说明与使用示例。
- src/main.cpp: 命令行 REPL 入口，负责读取 SQL、解析并执行。
- tools/bench/bench.cpp: 本地压测工具入口，生成数据并执行混合读写负载。

SQL 解析与执行

- include/db/SqlParser.h / src/SqlParser.cpp: SQL 解析器，将文本解析为 Statement。
- include/db/Executor.h / src/Executor.cpp: 执行器，将 Statement 转为数据库操作并输出结果。
- include/db/Types.h: SQL 语句与数据类型定义（Statement/Value/Column 等）。

数据库与元数据

- include/db/Database.h / src/Database.cpp: 数据库入口，管理表实例、日志与恢复流程。
- include/db/Catalog.h / src/Catalog.cpp: 表结构元数据管理与持久化（catalog.meta）。
- include/db/Schema.h / src/Schema.cpp: 表结构定义、记录编码/解码、值校验。

存储与分页

- include/db/Pager.h / src/Pager.cpp: 直接与磁盘文件交互的分页读写器。
- include/db/Cache.h / src/Cache.cpp: 页缓存分片（LRU），每个分片对应一个 NUMA 节点。
- include/db/BufferPool.h / src/BufferPool.cpp: NUMA 感知的 BufferPool，按页归属节点路由到缓存分片。
- include/db/Buffer.h / src/Buffer.cpp: 页数据缓冲区，支持按节点分配与释放。
- include/db/Numa.h / src/Numa.cpp: NUMA 拓扑与分配器抽象，支持 libnuma 或退化模式。
- include/db/PagedFile.h / src/PagedFile.cpp: 按偏移读写数据项的封装，内部使用 Pager + NUMA BufferPool。
- include/db/TableStorage.h / src/TableStorage.cpp: 单表存储引擎，行级 CRUD、表头与空闲行管理。
- include/db/LogManager.h / src/LogManager.cpp: 简易日志管理，用于崩溃恢复。

通用工具

- include/db/Utils.h / src/Utils.cpp: 字符串处理、十六进制编解码等工具函数。
- include/db/PageRouter.h: 页归属路由策略接口与默认实现。
- include/db/NumaExecutor.h / src/NumaExecutor.cpp: NUMA 线程执行器（每节点固定线程组与队列，用于按页归属路由执行任务）。
- include/db/NumaThread.h / src/NumaThread.cpp: 线程绑定到 NUMA 节点的工具封装（libnuma 可用时绑定）。

本地压测工具（mini_db_bench）

- 用途: 类似 sysbench 的本地压测工具（多线程、NUMA 感知），用于评估基础读写性能。
- 执行模型: 请求按记录所属页路由到对应 NUMA 节点的队列执行，避免运行中频繁切核。
- 参数示例: ./mini_db_bench --rows=50000 --ops=200000 --read=80 --update=15 --delete=5 --cache=512 --numa=2 --threads-per-node=2
- 常用参数:
  - --rows=N: 初始化行数。
  - --ops=N: 压测操作次数。
  - --read=PCT/--update=PCT/--delete=PCT: 读/写/删比例。
  - --data=PATH: 数据目录。
  - --table=NAME: 表名。
  - --cache=N: 缓存页数。
  - --numa=N: NUMA 节点数。
  - --threads-per-node=N: 每个 NUMA 节点线程数。
  - --no-reset: 不清空旧表。
- 输出指标: TPS, QPS, P99 延迟（ms）。
