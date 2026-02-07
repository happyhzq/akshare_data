# AkShare数据自动获取与更新项目 - 使用说明

## 项目概述

本项目是一个模块化、可扩展的数据管道系统，用于自动获取AkShare API提供的投资数据（宏观、公司、股票、外汇等），清洗数据，与现有数据比对，并将新数据存入MySQL数据库，同时防止重复插入。系统设计支持后期扩展到其他数据源和数据库。

## 项目结构

```
akshare_data_pipeline/
├── architecture.md          # 架构设计文档
├── config.yaml              # 配置文件
├── database_schema.md       # 数据库模式设计文档
├── main.py                  # 主程序入口
├── todo.md                  # 任务清单
├── core/                    # 核心模块
│   ├── __init__.py
│   ├── config_manager.py    # 配置管理器
│   ├── logger.py            # 日志管理器
│   └── exceptions.py        # 异常处理
├── data_fetchers/           # 数据获取模块
│   ├── __init__.py
│   ├── base_fetcher.py      # 基础获取器
│   ├── akshare_fetcher.py   # AkShare获取器
│   └── fetcher_factory.py   # 获取器工厂
├── data_processors/         # 数据处理模块
│   ├── __init__.py
│   ├── data_cleaner.py      # 数据清洗器
│   ├── data_transformer.py  # 数据转换器
│   └── data_comparator.py   # 数据比对器
├── data_storage/            # 数据存储模块
│   ├── __init__.py
│   ├── db_connection.py     # 数据库连接管理
│   └── data_store.py        # 数据存储器
├── task_manager/           # 任务管理模块
│   ├── __init__.py
│   ├── task_definition.py   # 任务定义
│   ├── task_executor.py     # 任务执行器
│   └── task_registry.py     # 任务注册表
└── pipeline_examples/      # 管道配置示例
    └── stock_pipeline.yaml  # 股票数据管道示例
```

## 安装与配置

### 1. 安装依赖

```bash
pip install akshare pymysql sqlalchemy pyyaml pandas
```

### 2. 配置数据库

1. 创建MySQL数据库
```sql
CREATE DATABASE akshare_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

2. 修改`config.yaml`中的数据库连接信息
```yaml
database:
  driver: "mysql+pymysql"
  host: "localhost"
  port: 3306
  username: "your_username"  # 修改为实际用户名
  password: "your_password"  # 修改为实际密码
  database: "akshare_data"   # 修改为实际数据库名
```

3. 初始化数据库表结构（参考`database_schema.md`中的SQL语句）

### 3. 配置日志目录

确保`config.yaml`中的`log_dir`指向一个可写的目录：
```yaml
log_dir: "/path/to/logs"
```

## 使用方法

### 1. 运行单个接口数据获取

```bash
python main.py --config config.yaml --interface ak_stock_zh_a_hist
```

### 2. 运行自定义管道

```bash
python main.py --config config.yaml --pipeline pipeline_examples/stock_pipeline.yaml
```

### 3. 列出所有可用的AkShare接口

```bash
python main.py --config config.yaml --list-interfaces
```

### 4. 搜索AkShare接口

```bash
python main.py --config config.yaml --search 股票
```

### 5. 常用指令

```bash

python main.py --config config.yaml --interface futures_zh_minute_sina --params '{"symbol": "I0", "period": "1"}'

python main.py --config config.yaml --interface futures_zh_minute_sina --params '{"symbol": "SR0", "period": "1"}'

python main.py --config config.yaml --interface futures_zh_minute_sina --params '{"symbol": "SP0", "period": "1"}'


python main.py --config config.yaml --interface stock_us_hist_min_em --params '{"symbol": "105.SNDK"}'

python main.py --config config.yaml --interface stock_us_hist_min_em --params '{"symbol": "105.WDC"}'

python main.py --config config.yaml --interface stock_us_hist_min_em --params '{"symbol": "105.INTC"}'

python main.py --config config.yaml --interface stock_us_hist_min_em --params '{"symbol": "105.SRPT"}'

python main.py --config config.yaml --interface stock_us_hist_min_em --params '{"symbol": "105.GOOG"}'

```


## 自动化执行

由于系统不支持内置的定时任务功能，建议以下替代方案：

### 1. 使用操作系统的cron作业（Linux/Unix）

```bash
# 每天早上9点运行股票数据管道
0 9 * * * cd /path/to/akshare_data_pipeline && python main.py --config config.yaml --pipeline pipeline_examples/stock_pipeline.yaml >> /path/to/logs/cron.log 2>&1
```

### 2. 使用Windows任务计划程序

创建一个批处理文件（.bat），然后在任务计划程序中设置定时运行。

### 3. 使用专业的工作流调度工具

如Apache Airflow、Luigi等。

## 扩展指南

### 1. 添加新的数据源

1. 在`data_fetchers`目录下创建新的获取器类，继承`BaseFetcher`
2. 在`fetcher_factory.py`中注册新的获取器类
3. 更新配置文件，添加新数据源的配置

### 2. 添加新的数据库后端

1. 在`data_storage`目录下修改或扩展数据库连接和操作类
2. 更新配置文件，切换到新的数据库后端

### 3. 添加新的数据类型

1. 在数据库中创建新的表结构
2. 在`config.yaml`的`schema_mapping`中添加新的映射配置
3. 创建新的管道配置文件

## 故障排除

### 1. 数据库连接问题

- 检查数据库连接信息是否正确
- 确保数据库服务器正在运行
- 检查用户权限

### 2. AkShare接口问题

- 确保AkShare版本兼容
- 检查接口参数是否正确
- 查看日志文件了解详细错误信息

### 3. 日志查看

日志文件位于配置的`log_dir`目录下，按模块和日期命名。

## 项目文档

- `architecture.md`: 详细的架构设计文档
- `database_schema.md`: 数据库模式设计文档
- 各模块代码中的注释和文档字符串

## 联系与支持

如有问题或需要支持，请联系项目维护者。
