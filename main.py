"""
主程序入口，协调各模块工作
"""
import os
import sys
import argparse
import yaml
import json
import logging
from datetime import datetime

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config_manager import ConfigManager
from core.logger import LoggerManager
from core.exceptions import AkSharePipelineError
from data_fetchers.fetcher_factory import FetcherFactory
from data_processors.data_cleaner import DataCleanerFactory
from data_processors.data_transformer import DataTransformer
from data_processors.data_comparator import DataComparator
from data_storage.db_connection import DBConnectionManager
from data_storage.data_store import DataStore
from task_manager.task_executor import TaskExecutor
from task_manager.task_registry import TaskRegistry


def init_system(config_path):
    """初始化系统"""
    # 加载配置
    config_manager = ConfigManager()
    config_manager.load_config(config_path)
    config = config_manager.get_config()
    
    # 初始化日志
    log_dir = config.get('log_dir', 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger_manager = LoggerManager()
    logger_manager.init(
        log_dir=log_dir,
        log_level=config.get('log_level', 'INFO')
    )
    
    logger = logger_manager.get_logger("Main")
    logger.info("系统初始化开始")
    
    # 初始化数据库连接
    if 'database' in config:
        db_manager = DBConnectionManager()
        db_manager.init_from_config(config_path)
        
        # 测试数据库连接
        if db_manager.test_connection():
            logger.info("数据库连接测试成功")
        else:
            logger.error("数据库连接测试失败")
            sys.exit(1)
    
    # 初始化任务注册表
    task_registry = TaskRegistry()
    task_registry_file = config.get('task_registry_file')
    if task_registry_file and os.path.exists(task_registry_file):
        task_registry.load_from_file(task_registry_file)
    
    logger.info("系统初始化完成")
    
    return config


def run_pipeline(config, pipeline_config):
    """运行数据管道"""
    logger = LoggerManager().get_logger("Main")
    logger.info(f"开始运行数据管道: {pipeline_config.get('pipeline_id', 'default')}")
    
    # 创建任务执行器
    executor = TaskExecutor(config)
    
    # 执行数据管道
    result = executor.execute_pipeline(pipeline_config)
    
    # 输出结果
    if result['status'] == 'completed':
        logger.info(f"数据管道执行成功: {result['pipeline_id']}")
        
        # 统计结果
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        for task in result['tasks']:
            if task['name'] == 'DataStoreTask':
                if task['result'].get('operation') == 'insert':
                    inserted_count += task['result'].get('inserted_count', 0)
                elif task['result'].get('operation') == 'update':
                    updated_count += task['result'].get('updated_count', 0)
                error_count += task['result'].get('error_count', 0)
        
        logger.info(f"数据管道执行结果: 插入 {inserted_count} 行, 更新 {updated_count} 行, 错误 {error_count} 行")
    else:
        logger.error(f"数据管道执行失败: {result['pipeline_id']}, 错误: {result.get('error', '未知错误')}")
    
    return result


def parse_params(params_str):
    """解析参数字符串为字典"""
    try:
        # 尝试解析为JSON
        return json.loads(params_str)
    except json.JSONDecodeError:
        # 尝试解析为字典字符串格式
        try:
            stripped = params_str.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                params = json.loads(
                    stripped
                    .replace("'", "\"")  # 替换单引号为双引号
                    .replace("None", "null")  # 替换Python None为JSON null
                )
                if isinstance(params, dict):
                    return params
                raise ValueError("参数必须是字典格式")
            raise ValueError("参数格式不正确")
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(
                f"无法解析参数: {params_str}，"
                f"请使用有效的JSON格式，如: '{{\"symbol\": \".INX\"}}'"
            ) from e


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='AkShare数据自动获取与更新程序')
    parser.add_argument('--config', type=str, default='config.yaml', help='配置文件路径')
    parser.add_argument('--pipeline', type=str, help='管道配置文件路径')
    parser.add_argument('--interface', type=str, help='AkShare接口名称')
    parser.add_argument('--params', type=str, help='接口参数，JSON格式，如: \'{"symbol": ".INX"}\'')
    parser.add_argument('--interface-info', type=str, help='获取指定AkShare接口的详细信息')
    parser.add_argument('--list-interfaces', action='store_true', help='列出所有可用的AkShare接口')
    parser.add_argument('--search', type=str, help='搜索AkShare接口')
    args = parser.parse_args()
    
    try:
        # 初始化系统
        config = init_system(args.config)
        logger = LoggerManager().get_logger("Main")
        
        # 创建AkShare获取器
        fetcher = FetcherFactory.create_fetcher('akshare', {})
        
        # 获取指定接口的详细信息
        if args.interface_info:
            try:
                interface_info = fetcher.get_interface_info(args.interface_info)
                print(f"\n接口信息: {args.interface_info}")
                print(f"\n文档:\n{interface_info['doc']}")
                
                print("\n参数列表:")
                for name, param_info in interface_info['parameters'].items():
                    required = "必需" if param_info['required'] else "可选"
                    default = f", 默认值: {param_info['default']}" if param_info['default'] else ""
                    annotation = f", 类型: {param_info['annotation']}" if param_info['annotation'] else ""
                    print(f"  - {name}: {required}{default}{annotation}")
                
                print(f"\n模块: {interface_info['module']}")
                return
            except Exception as e:
                logger.error(f"获取接口信息失败: {str(e)}")
                print(f"获取接口信息失败: {str(e)}")
                sys.exit(1)
        
        # 列出所有可用的AkShare接口
        if args.list_interfaces:
            interfaces = fetcher.get_available_interfaces()
            print(f"可用的AkShare接口 ({len(interfaces)}):")
            for interface in sorted(interfaces):
                print(f"  - {interface}")
            return
        
        # 搜索AkShare接口
        if args.search:
            interfaces = fetcher.search_interfaces(args.search)
            print(f"搜索结果 ({len(interfaces)}):")
            for interface in sorted(interfaces):
                print(f"  - {interface}")
            return
        
        # 运行指定的管道
        if args.pipeline:
            with open(args.pipeline, 'r') as f:
                pipeline_config = yaml.safe_load(f)
            run_pipeline(config, pipeline_config)
            return
        
        # 运行指定的接口
        if args.interface:
            # 解析接口参数
            interface_params = {}
            if args.params:
                try:
                    interface_params = parse_params(args.params)
                    logger.info(f"使用接口参数: {interface_params}")
                except ValueError as e:
                    logger.error(f"参数解析错误: {str(e)}")
                    print(f"参数解析错误: {str(e)}")
                    sys.exit(1)
            
            # 创建管道配置
            pipeline_config = {
                'pipeline_id': f"pipeline_{args.interface}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                'fetcher_type': 'akshare',
                'interface_name': args.interface,
                'interface_params': interface_params,
                'cleaner_type': 'standard',
                'cleaner_config': {},
                'transformer_config': {
                    'schema_mapping': config.get('schema_mapping', {})
                },
                'comparator_config': {
                    'key_columns': config.get('key_columns', {})
                },
                'store_config': {
                    'batch_size': config.get('batch_size', 1000),
                    'retry_count': config.get('retry_count', 3),
                    'unique_constraints': config.get('unique_constraints', {})
                }
            }
            run_pipeline(config, pipeline_config)
            return
        
        # 如果没有指定操作，显示帮助信息
        parser.print_help()
        
    except AkSharePipelineError as e:
        logger = LoggerManager().get_logger("Main")
        logger.error(f"程序执行错误: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger = LoggerManager().get_logger("Main")
        logger.error(f"未处理的异常: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
