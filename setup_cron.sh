#!/bin/bash
# 设置crontab自动运行AkShare数据获取与更新的脚本
# 将此脚本设置为可执行: chmod +x setup_cron.sh

# 配置项 - 请根据实际情况修改
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"  # 自动获取脚本所在目录
RUN_SCRIPT="${PROJECT_DIR}/run_akshare_pipeline.sh"
CRON_TIME="0 8 * * *"  # 默认每天早上8点运行，可根据需要修改

# 确保运行脚本有执行权限
chmod +x ${RUN_SCRIPT}

# 创建临时crontab文件
TEMP_CRON=$(mktemp)

# 导出当前crontab配置
crontab -l > ${TEMP_CRON} 2>/dev/null || echo "# 新建crontab配置" > ${TEMP_CRON}

# 检查是否已存在相同配置
if grep -q "${RUN_SCRIPT}" ${TEMP_CRON}; then
    echo "crontab配置已存在，将更新..."
    # 删除旧配置
    sed -i "\|${RUN_SCRIPT}|d" ${TEMP_CRON}
fi

# 添加新的crontab配置
echo "# AkShare数据自动获取与更新 - 每天${CRON_TIME/0 /}运行" >> ${TEMP_CRON}
echo "${CRON_TIME} ${RUN_SCRIPT} # 自动运行AkShare数据管道" >> ${TEMP_CRON}

# 应用新的crontab配置
crontab ${TEMP_CRON}

# 删除临时文件
rm ${TEMP_CRON}

echo "crontab配置已更新，AkShare数据管道将在每天${CRON_TIME/0 /}自动运行"
echo "可以通过 'crontab -l' 命令查看当前配置"
echo "脚本路径: ${RUN_SCRIPT}"
