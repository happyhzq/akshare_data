"""
配置管理器模块，负责加载和管理系统配置
"""
import os
import yaml
from typing import Dict, Any, Optional


class ConfigManager:
    """配置管理器类，负责加载和管理系统配置"""
    
    _instance = None
    
    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化配置管理器"""
        if self._initialized:
            return
            
        self._config = {}
        self._config_path = None
        self._initialized = True
    
    def load_config(self, config_path: str) -> None:
        """
        从指定路径加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Raises:
            FileNotFoundError: 配置文件不存在
            yaml.YAMLError: 配置文件格式错误
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
                self._config_path = config_path
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"配置文件格式错误: {str(e)}")
    
    def get_config(self) -> Dict[str, Any]:
        """
        获取完整配置
        
        Returns:
            Dict[str, Any]: 完整配置字典
        """
        return self._config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取指定键的配置值
        
        Args:
            key: 配置键，支持点号分隔的多级键，如 'database.host'
            default: 默认值，当键不存在时返回
            
        Returns:
            Any: 配置值或默认值
        """
        if not key:
            return default
            
        # 处理多级键
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
                
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        设置指定键的配置值
        
        Args:
            key: 配置键，支持点号分隔的多级键，如 'database.host'
            value: 配置值
        """
        if not key:
            return
            
        # 处理多级键
        keys = key.split('.')
        config = self._config
        
        # 遍历到倒数第二级
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
            
        # 设置最后一级的值
        config[keys[-1]] = value
    
    def save_config(self, config_path: Optional[str] = None) -> None:
        """
        保存配置到文件
        
        Args:
            config_path: 配置文件路径，如果为None则使用加载时的路径
            
        Raises:
            ValueError: 未指定配置文件路径
            IOError: 保存配置文件失败
        """
        save_path = config_path or self._config_path
        
        if not save_path:
            raise ValueError("未指定配置文件路径")
            
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                yaml.dump(self._config, f, default_flow_style=False, allow_unicode=True)
        except IOError as e:
            raise IOError(f"保存配置文件失败: {str(e)}")
