MiniRDB (C++)

A minimal relational database with paging, storage, DDL/DML, cache+log, and a tiny SQL parser.

Build

- mkdir -p build
- cd build
- cmake ..
- cmake --build .

Run

- ./mini_db

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

---

项目文件说明（中文）

构建与入口

- CMakeLists.txt: CMake 构建脚本，定义目标与源文件。
- README.md: 项目说明与使用示例。
- src/main.cpp: 命令行 REPL 入口，负责读取 SQL、解析并执行。

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
- include/db/Cache.h / src/Cache.cpp: 页缓存（LRU），提升 I/O 性能。
- include/db/PagedFile.h / src/PagedFile.cpp: 按偏移读写数据项的封装，内部使用 Pager + Cache。
- include/db/TableStorage.h / src/TableStorage.cpp: 单表存储引擎，行级 CRUD、表头与空闲行管理。
- include/db/LogManager.h / src/LogManager.cpp: 简易日志管理，用于崩溃恢复。

通用工具

- include/db/Utils.h / src/Utils.cpp: 字符串处理、十六进制编解码等工具函数。
