#!/bin/bash
# 自动运行AkShare数据获取与更新的定时脚本
# 将此脚本设置为可执行: chmod +x run_akshare_pipeline.sh

# 配置项 - 请根据实际情况修改
PROJECT_DIR="/path/to/akshare_data_pipeline"  # 项目目录路径
CONFIG_FILE="config.yaml"                     # 配置文件名
LOG_DIR="${PROJECT_DIR}/logs"                 # 日志目录
DATE=$(date +"%Y-%m-%d")                      # 当前日期

# 确保日志目录存在
mkdir -p ${LOG_DIR}

# 日志文件路径
LOG_FILE="${LOG_DIR}/pipeline_${DATE}.log"

# 进入项目目录
cd ${PROJECT_DIR}

# 记录开始时间
echo "===== 开始执行数据管道 $(date) =====" >> ${LOG_FILE}

# 运行所有配置的管道
echo "运行股票数据管道..." >> ${LOG_FILE}
python main.py --config ${CONFIG_FILE} --pipeline pipeline_examples/stock_pipeline.yaml >> ${LOG_FILE} 2>&1

# 可以添加更多管道
# echo "运行宏观经济数据管道..." >> ${LOG_FILE}
# python main.py --config ${CONFIG_FILE} --pipeline pipeline_examples/macro_pipeline.yaml >> ${LOG_FILE} 2>&1

# echo "运行外汇数据管道..." >> ${LOG_FILE}
# python main.py --config ${CONFIG_FILE} --pipeline pipeline_examples/forex_pipeline.yaml >> ${LOG_FILE} 2>&1

# 记录结束时间
echo "===== 执行完成 $(date) =====" >> ${LOG_FILE}
echo "" >> ${LOG_FILE}

# 输出执行状态
echo "数据管道执行完成，日志保存在: ${LOG_FILE}"
