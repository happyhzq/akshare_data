"""
获取器工厂模块，根据配置创建相应的数据获取器实例
"""
from typing import Dict, Any, Optional

from core.exceptions import FetcherError
from data_fetchers.base_fetcher import BaseFetcher
from data_fetchers.akshare_fetcher import AkShareFetcher


class FetcherFactory:
    """数据获取器工厂类，负责创建各类数据获取器实例"""
    
    _fetcher_registry = {
        'akshare': AkShareFetcher
    }
    
    @classmethod
    def register_fetcher(cls, name: str, fetcher_class) -> None:
        """
        注册数据获取器类
        
        Args:
            name: 获取器名称
            fetcher_class: 获取器类，必须继承自BaseFetcher
        
        Raises:
            TypeError: 获取器类型错误
        """
        if not issubclass(fetcher_class, BaseFetcher):
            raise TypeError(f"获取器类必须继承自BaseFetcher: {fetcher_class.__name__}")
        
        cls._fetcher_registry[name] = fetcher_class
    
    @classmethod
    def create_fetcher(cls, fetcher_type: str, config: Dict[str, Any]) -> BaseFetcher:
        """
        创建数据获取器实例
        
        Args:
            fetcher_type: 获取器类型
            config: 配置信息
            
        Returns:
            BaseFetcher: 数据获取器实例
            
        Raises:
            FetcherError: 获取器类型不支持
        """
        if fetcher_type not in cls._fetcher_registry:
            raise FetcherError(f"不支持的获取器类型: {fetcher_type}")
        
        fetcher_class = cls._fetcher_registry[fetcher_type]
        return fetcher_class(config)
    
    @classmethod
    def get_supported_fetchers(cls) -> Dict[str, Any]:
        """
        获取支持的获取器类型
        
        Returns:
            Dict[str, Any]: 支持的获取器类型字典，格式为 {类型名: 类}
        """
        return cls._fetcher_registry.copy()
