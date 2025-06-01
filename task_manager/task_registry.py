"""
任务注册表模块，管理已注册的任务
"""
from typing import Dict, Any, List, Optional
import json
import os
from datetime import datetime

from core.exceptions import TaskError
from core.logger import LoggerManager


class TaskRegistry:
    """任务注册表类，管理已注册的任务"""
    
    _instance = None
    
    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(TaskRegistry, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化任务注册表"""
        if self._initialized:
            return
            
        self.tasks = {}
        self.logger = LoggerManager().get_logger("TaskRegistry")
        self._initialized = True
    
    def register_task(self, task_id: str, task_config: Dict[str, Any]) -> None:
        """
        注册任务
        
        Args:
            task_id: 任务ID
            task_config: 任务配置
        """
        if task_id in self.tasks:
            self.logger.warning(f"任务已存在，将被覆盖: {task_id}")
            
        self.tasks[task_id] = {
            'task_id': task_id,
            'config': task_config,
            'register_time': datetime.now(),
            'last_run_time': None,
            'run_count': 0,
            'status': 'registered'
        }
        
        self.logger.info(f"任务注册成功: {task_id}")
    
    def unregister_task(self, task_id: str) -> None:
        """
        注销任务
        
        Args:
            task_id: 任务ID
            
        Raises:
            TaskError: 任务不存在
        """
        if task_id not in self.tasks:
            raise TaskError(f"任务不存在: {task_id}")
            
        del self.tasks[task_id]
        self.logger.info(f"任务注销成功: {task_id}")
    
    def get_task(self, task_id: str) -> Dict[str, Any]:
        """
        获取任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            Dict[str, Any]: 任务信息
            
        Raises:
            TaskError: 任务不存在
        """
        if task_id not in self.tasks:
            raise TaskError(f"任务不存在: {task_id}")
            
        return self.tasks[task_id]
    
    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有任务
        
        Returns:
            Dict[str, Dict[str, Any]]: 所有任务信息
        """
        return self.tasks
    
    def update_task_status(self, task_id: str, status: str, result: Optional[Dict[str, Any]] = None) -> None:
        """
        更新任务状态
        
        Args:
            task_id: 任务ID
            status: 任务状态
            result: 任务结果
            
        Raises:
            TaskError: 任务不存在
        """
        if task_id not in self.tasks:
            raise TaskError(f"任务不存在: {task_id}")
            
        self.tasks[task_id]['status'] = status
        self.tasks[task_id]['last_run_time'] = datetime.now()
        self.tasks[task_id]['run_count'] += 1
        
        if result:
            self.tasks[task_id]['last_result'] = result
            
        self.logger.info(f"任务状态更新: {task_id}, 状态: {status}")
    
    def save_to_file(self, file_path: str) -> None:
        """
        保存任务注册表到文件
        
        Args:
            file_path: 文件路径
        """
        try:
            # 转换datetime对象为字符串
            tasks_copy = {}
            for task_id, task_info in self.tasks.items():
                task_copy = task_info.copy()
                if 'register_time' in task_copy:
                    task_copy['register_time'] = task_copy['register_time'].isoformat()
                if 'last_run_time' in task_copy and task_copy['last_run_time']:
                    task_copy['last_run_time'] = task_copy['last_run_time'].isoformat()
                tasks_copy[task_id] = task_copy
            
            with open(file_path, 'w') as f:
                json.dump(tasks_copy, f, indent=2)
                
            self.logger.info(f"任务注册表保存成功: {file_path}")
            
        except Exception as e:
            self.logger.error(f"保存任务注册表失败: {str(e)}")
            raise TaskError(f"保存任务注册表失败", details=str(e))
    
    def load_from_file(self, file_path: str) -> None:
        """
        从文件加载任务注册表
        
        Args:
            file_path: 文件路径
        """
        try:
            if not os.path.exists(file_path):
                self.logger.warning(f"任务注册表文件不存在: {file_path}")
                return
                
            with open(file_path, 'r') as f:
                tasks_data = json.load(f)
            
            # 转换字符串为datetime对象
            for task_id, task_info in tasks_data.items():
                if 'register_time' in task_info:
                    task_info['register_time'] = datetime.fromisoformat(task_info['register_time'])
                if 'last_run_time' in task_info and task_info['last_run_time']:
                    task_info['last_run_time'] = datetime.fromisoformat(task_info['last_run_time'])
            
            self.tasks = tasks_data
            self.logger.info(f"任务注册表加载成功: {file_path}, 任务数: {len(self.tasks)}")
            
        except Exception as e:
            self.logger.error(f"加载任务注册表失败: {str(e)}")
            raise TaskError(f"加载任务注册表失败", details=str(e))
