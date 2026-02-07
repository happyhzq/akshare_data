#!/bin/bash
# 设置crontab自动运行AkShare数据获取与更新的脚本
# 将此脚本设置为可执行: chmod +x setup_cron.sh

# ============= 配置项 - 请根据实际情况修改 =============
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"  # 自动获取脚本所在目录
RUN_SCRIPT="${PROJECT_DIR}/run_akshare_pipeline.sh"
CRON_TIME="0 8 * * *"  # 默认每天早上8点运行，可根据需要修改

# Conda配置
CONDA_ENV_NAME="akshare_data"  # Conda环境名称
# 自动检测conda路径（支持多种安装方式）
if [ -f "${HOME}/miniconda3/etc/profile.d/conda.sh" ]; then
    CONDA_INIT="${HOME}/miniconda3/etc/profile.d/conda.sh"
elif [ -f "${HOME}/anaconda3/etc/profile.d/conda.sh" ]; then
    CONDA_INIT="${HOME}/anaconda3/etc/profile.d/conda.sh"
elif [ -f "/opt/conda/etc/profile.d/conda.sh" ]; then
    CONDA_INIT="/opt/conda/etc/profile.d/conda.sh"
else
    # 尝试通过conda命令获取
    CONDA_BASE=$(conda info --base 2>/dev/null)
    if [ -n "${CONDA_BASE}" ]; then
        CONDA_INIT="${CONDA_BASE}/etc/profile.d/conda.sh"
    else
        echo "错误: 无法找到conda安装路径"
        echo "请手动设置CONDA_INIT变量"
        exit 1
    fi
fi

echo "检测到conda初始化脚本: ${CONDA_INIT}"
# ======================================================

# 验证conda初始化脚本是否存在
if [ ! -f "${CONDA_INIT}" ]; then
    echo "错误: conda初始化脚本不存在: ${CONDA_INIT}"
    echo "请检查CONDA_INIT变量配置"
    exit 1
fi

# 验证conda环境是否存在
source "${CONDA_INIT}"
if ! conda env list | grep -q "^${CONDA_ENV_NAME} "; then
    echo "错误: conda环境 '${CONDA_ENV_NAME}' 不存在"
    echo "请先创建环境或修改CONDA_ENV_NAME变量"
    echo "可用的环境列表:"
    conda env list
    exit 1
fi
echo "验证conda环境 '${CONDA_ENV_NAME}' 存在"

# 确保运行脚本有执行权限
chmod +x ${RUN_SCRIPT}

# 创建临时crontab文件
TEMP_CRON=$(mktemp)

# 导出当前crontab配置
crontab -l > ${TEMP_CRON} 2>/dev/null || echo "# 新建crontab配置" > ${TEMP_CRON}

# 检查是否已存在相同配置
if grep -q "${RUN_SCRIPT}" ${TEMP_CRON}; then
    echo "crontab配置已存在，将更新..."
    # 删除旧配置（删除相关的所有行，包括注释）
    sed -i "\|${RUN_SCRIPT}|d" ${TEMP_CRON}
    sed -i "/# AkShare数据自动获取与更新/d" ${TEMP_CRON}
fi

# 添加新的crontab配置（包含conda初始化信息）
cat >> ${TEMP_CRON} << EOF

# ========== AkShare数据自动获取与更新 ==========
# Conda环境: ${CONDA_ENV_NAME}
# 运行时间: ${CRON_TIME}
# 脚本路径: ${RUN_SCRIPT}
CONDA_INIT="${CONDA_INIT}"
CONDA_ENV="${CONDA_ENV_NAME}"
${CRON_TIME} /bin/bash -c 'source ${CONDA_INIT} && conda activate ${CONDA_ENV_NAME} && ${RUN_SCRIPT}' >> ${PROJECT_DIR}/logs/cron_akshare.log 2>&1
# ==============================================
EOF

# 应用新的crontab配置
crontab ${TEMP_CRON}

# 删除临时文件
rm ${TEMP_CRON}

# 显示配置信息
echo "============================================"
echo "✓ crontab配置已成功更新"
echo "============================================"
echo "项目目录: ${PROJECT_DIR}"
echo "运行脚本: ${RUN_SCRIPT}"
echo "Conda环境: ${CONDA_ENV_NAME}"
echo "Conda路径: ${CONDA_INIT}"
echo "运行时间: ${CRON_TIME}"
echo "日志文件: ${PROJECT_DIR}/logs/cron_akshare.log"
echo "============================================"
echo ""
echo "可以通过以下命令查看:"
echo "  查看crontab配置: crontab -l"
echo "  查看运行日志: tail -f ${PROJECT_DIR}/logs/cron_akshare.log"
echo "  手动测试运行: /bin/bash -c 'source ${CONDA_INIT} && conda activate ${CONDA_ENV_NAME} && ${RUN_SCRIPT}'"
echo "============================================"

# 创建日志目录（如果不存在）
mkdir -p ${PROJECT_DIR}/logs
echo "日志目录已准备: ${PROJECT_DIR}/logs"