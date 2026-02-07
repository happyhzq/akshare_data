"""
任务执行器模块，执行任务的核心逻辑
"""
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

from core.exceptions import TaskError
from core.logger import LoggerManager
from task_manager.task_definition import BaseTask, DataFetchTask, DataProcessTask, DataCompareTask, DataStoreTask


class TaskExecutor:
    """任务执行器类，执行任务的核心逻辑"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化任务执行器
        
        Args:
            config: 配置信息
        """
        self.config = config
        self.logger = LoggerManager().get_logger("TaskExecutor")
        self.tasks_history = []
    
    def execute_task(self, task: BaseTask) -> Dict[str, Any]:
        """
        执行单个任务
        
        Args:
            task: 任务对象
            
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        self.logger.info(f"开始执行任务: {task.task_id}, 类型: {task.__class__.__name__}")
        
        try:
            # 执行任务
            result = task.run()
            
            # 记录任务历史
            self.tasks_history.append(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"任务执行失败: {task.task_id}, 错误: {str(e)}")
            raise TaskError(f"任务执行失败: {task.task_id}", details=str(e))
    
    def execute_pipeline(self, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行完整的数据管道
        
        Args:
            pipeline_config: 管道配置，包含以下字段：
                - pipeline_id: 管道ID
                - fetcher_config: 获取器配置
                - cleaner_config: 清洗器配置
                - transformer_config: 转换器配置
                - comparator_config: 比对器配置
                - store_config: 存储器配置
                - interface_name: 接口名称
                - interface_params: 接口参数
                
        Returns:
            Dict[str, Any]: 管道执行结果
        """
        pipeline_id = pipeline_config.get('pipeline_id', f"pipeline_{datetime.now().strftime('%Y%m%d%H%M%S')}")
        self.logger.info(f"开始执行数据管道: {pipeline_id}")
        
        start_time = datetime.now()
        pipeline_results = {
            'pipeline_id': pipeline_id,
            'start_time': start_time,
            'tasks': [],
            'status': 'running'
        }
        
        try:
            # 1. 数据获取任务
            fetch_task_id = f"{pipeline_id}_fetch"
            fetch_task_config = {
                'fetcher_type': pipeline_config.get('fetcher_type', 'akshare'),
                'fetcher_config': pipeline_config.get('fetcher_config', {}),
                'interface_name': pipeline_config.get('interface_name'),
                'interface_params': pipeline_config.get('interface_params', {})
            }
            fetch_task = DataFetchTask(fetch_task_id, fetch_task_config)
            fetch_result = self.execute_task(fetch_task)
            pipeline_results['tasks'].append(fetch_result)
            
            # 获取数据和元数据
            raw_data = fetch_result['result']['data']
            metadata = fetch_result['result']['metadata']
            
            # 确保metadata中包含interface_params，用于参数注入
            if 'interface_params' not in metadata and pipeline_config.get('interface_params'):
                metadata['interface_params'] = pipeline_config.get('interface_params', {})
                self.logger.info(f"已将接口参数添加到metadata: {metadata['interface_params']}")
            
            # 2. 数据处理任务
            process_task_id = f"{pipeline_id}_process"
            
            # 从全局配置中获取是否启用转换器的设置
            enable_transformer = self.config.get('processor', {}).get('enable_transformer', True)
            
            process_task_config = {
                'cleaner_type': pipeline_config.get('cleaner_type', 'standard'),
                'cleaner_config': pipeline_config.get('cleaner_config', {}),
                'transformer_config': pipeline_config.get('transformer_config', {}),
                'input_data': raw_data,
                'input_metadata': metadata,
                'enable_transformer': enable_transformer  # 传递转换器开关配置
            }
            process_task = DataProcessTask(process_task_id, process_task_config)
            process_result = self.execute_task(process_task)
            pipeline_results['tasks'].append(process_result)
            
            # 获取处理后的数据
            processed_data = process_result['result']['table_data']
            print(processed_data)
            table_name = process_result['result']['table_name']
            updated_metadata = process_result['result']['metadata']
            
            # 从全局配置中获取存储配置，包括自动建表设置
            store_config = pipeline_config.get('store_config', {}).copy()
            
            # 将全局storage配置合并到store_config中
            if 'storage' in self.config:
                for key, value in self.config['storage'].items():
                    if key not in store_config:
                        store_config[key] = value
            
            # 3. 数据比对任务
            compare_task_id = f"{pipeline_id}_compare"
            key_columns = pipeline_config.get('key_columns', [])
            compare_task_config = {
                'comparator_config': pipeline_config.get('comparator_config', {}),
                'new_data': processed_data,
                'table_name': table_name,
                'db_config': store_config,  # 使用合并后的存储配置
                'key_columns': key_columns
            }
            compare_task = DataCompareTask(compare_task_id, compare_task_config)
            compare_result = self.execute_task(compare_task)
            pipeline_results['tasks'].append(compare_result)
            
            # 获取比对结果
            to_insert = compare_result['result']['to_insert']
            to_update = compare_result['result']['to_update']
            
            # 4. 数据插入任务
            if not to_insert.empty:
                insert_task_id = f"{pipeline_id}_insert"
                insert_task_config = {
                    'store_config': store_config,  # 使用合并后的存储配置
                    'data': to_insert,
                    'table_name': table_name,
                    'metadata': updated_metadata,
                    'operation': 'insert'
                }
                insert_task = DataStoreTask(insert_task_id, insert_task_config)
                insert_result = self.execute_task(insert_task)
                pipeline_results['tasks'].append(insert_result)
            
            # 5. 数据更新任务
            if not to_update.empty and key_columns:
                update_task_id = f"{pipeline_id}_update"
                update_task_config = {
                    'store_config': store_config,  # 使用合并后的存储配置
                    'data': to_update,
                    'table_name': table_name,
                    'metadata': updated_metadata,
                    'operation': 'update',
                    'key_columns': key_columns
                }
                update_task = DataStoreTask(update_task_id, update_task_config)
                update_result = self.execute_task(update_task)
                pipeline_results['tasks'].append(update_result)
            
            # 管道执行成功
            pipeline_results['status'] = 'completed'
            self.logger.info(f"数据管道执行成功: {pipeline_id}")
            
        except Exception as e:
            # 管道执行失败
            pipeline_results['status'] = 'failed'
            pipeline_results['error'] = str(e)
            self.logger.error(f"数据管道执行失败: {pipeline_id}, 错误: {str(e)}")
        
        # 计算执行时间
        end_time = datetime.now()
        pipeline_results['end_time'] = end_time
        pipeline_results['duration'] = (end_time - start_time).total_seconds()
        
        return pipeline_results
    
    def get_tasks_history(self) -> List[Dict[str, Any]]:
        """
        获取任务执行历史
        
        Returns:
            List[Dict[str, Any]]: 任务执行历史列表
        """
        return self.tasks_history
