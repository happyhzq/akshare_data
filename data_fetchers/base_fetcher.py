"""
基础数据获取器模块，定义数据获取的基类和接口
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime


class BaseFetcher(ABC):
    """数据获取器基类，定义数据获取的通用接口"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据获取器
        
        Args:
            config: 配置信息
        """
        self.config = config
        self.name = self.__class__.__name__
    
    @abstractmethod
    def fetch(self, interface_name: str, **kwargs) -> Dict[str, Any]:
        """
        获取数据的抽象方法
        
        Args:
            interface_name: 接口名称
            **kwargs: 接口参数
            
        Returns:
            Dict[str, Any]: 包含获取结果的字典，格式为：
            {
                'data': 获取的数据,
                'metadata': {
                    'fetch_time': 获取时间,
                    'source': 数据来源,
                    'interface': 接口名称,
                    'parameters': 接口参数
                }
            }
        """
        pass
    
    @abstractmethod
    def get_available_interfaces(self) -> List[str]:
        """
        获取可用接口列表
        
        Returns:
            List[str]: 可用接口名称列表
        """
        pass
    
    @abstractmethod
    def get_interface_info(self, interface_name: str) -> Dict[str, Any]:
        """
        获取接口信息
        
        Args:
            interface_name: 接口名称
            
        Returns:
            Dict[str, Any]: 接口信息字典，包含接口描述、参数说明等
        """
        pass
    
    def _create_metadata(self, interface_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建元数据
        
        Args:
            interface_name: 接口名称
            parameters: 接口参数
            
        Returns:
            Dict[str, Any]: 元数据字典
        """
        return {
            'fetch_time': datetime.now(),
            'source': self.name,
            'interface': interface_name,
            'parameters': parameters
        }
