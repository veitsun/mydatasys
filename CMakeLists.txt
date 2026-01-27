cmake_minimum_required(VERSION 3.10)
project(mini_db LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(mini_db
  src/main.cpp
  src/Utils.cpp
  src/Schema.cpp
  src/Pager.cpp
  src/Cache.cpp
  src/PagedFile.cpp
  src/LogManager.cpp
  src/TableStorage.cpp
  src/Catalog.cpp
  src/Database.cpp
  src/SqlParser.cpp
  src/Executor.cpp
)

target_include_directories(mini_db PRIVATE include)
