"""
自定义异常类模块，定义系统中使用的各类异常
"""
from typing import Optional, Any


class AkSharePipelineError(Exception):
    """AkShare数据管道基础异常类"""
    
    def __init__(self, message: str, code: Optional[int] = None, details: Optional[Any] = None):
        """
        初始化异常
        
        Args:
            message: 异常消息
            code: 异常代码
            details: 异常详情
        """
        self.message = message
        self.code = code
        self.details = details
        super().__init__(self.message)


class ConfigError(AkSharePipelineError):
    """配置相关异常"""
    pass


class DatabaseError(AkSharePipelineError):
    """数据库相关异常"""
    pass


class FetcherError(AkSharePipelineError):
    """数据获取相关异常"""
    pass


class ProcessorError(AkSharePipelineError):
    """数据处理相关异常"""
    pass


class TaskError(AkSharePipelineError):
    """任务执行相关异常"""
    pass


class ValidationError(AkSharePipelineError):
    """数据验证相关异常"""
    pass


class DuplicateError(AkSharePipelineError):
    """数据重复相关异常"""
    pass
