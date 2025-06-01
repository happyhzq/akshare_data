"""
日志管理器模块，提供统一的日志记录功能
"""
import logging
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any


class LoggerManager:
    """日志管理器类，提供统一的日志记录功能"""
    
    _instance = None
    _loggers: Dict[str, logging.Logger] = {}
    
    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化日志管理器"""
        if self._initialized:
            return
            
        self._log_dir = None
        self._log_level = logging.INFO
        self._log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        self._initialized = True
    
    def init(self, log_dir: str, log_level: str = 'INFO', log_format: Optional[str] = None) -> None:
        """
        初始化日志系统
        
        Args:
            log_dir: 日志目录
            log_level: 日志级别，默认为INFO
            log_format: 日志格式，默认为None（使用默认格式）
        """
        # 创建日志目录
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self._log_dir = log_dir
        
        # 设置日志级别
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        self._log_level = level_map.get(log_level.upper(), logging.INFO)
        
        # 设置日志格式
        if log_format:
            self._log_format = log_format
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        获取指定名称的日志器
        
        Args:
            name: 日志器名称
            
        Returns:
            logging.Logger: 日志器实例
        """
        if name in self._loggers:
            return self._loggers[name]
            
        # 创建日志器
        logger = logging.getLogger(name)
        logger.setLevel(self._log_level)
        logger.propagate = False  # 避免日志重复
        
        # 清除已有的处理器
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self._log_level)
        console_formatter = logging.Formatter(self._log_format)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # 创建文件处理器
        if self._log_dir:
            today = datetime.now().strftime('%Y-%m-%d')
            log_file = os.path.join(self._log_dir, f"{name}_{today}.log")
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(self._log_level)
            file_formatter = logging.Formatter(self._log_format)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        # 缓存日志器
        self._loggers[name] = logger
        
        return logger
