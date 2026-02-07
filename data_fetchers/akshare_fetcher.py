"""
AkShare数据获取器模块，实现从AkShare获取各类数据
"""
import inspect
import importlib
import pkgutil
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable, Union
import pandas as pd
import akshare as ak

from core.exceptions import FetcherError
from core.logger import LoggerManager
from data_fetchers.base_fetcher import BaseFetcher


class AkShareFetcher(BaseFetcher):
    """AkShare数据获取器，封装AkShare API调用"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化AkShare数据获取器
        
        Args:
            config: 配置信息，包含AkShare相关配置
        """
        super().__init__(config)
        self.logger = LoggerManager().get_logger("AkShareFetcher")
        self._interface_cache = {}  # 缓存接口信息
        self._init_interfaces()
    
    def _init_interfaces(self) -> None:
        """初始化接口信息，扫描AkShare模块中的函数"""
        self.logger.info("初始化AkShare接口信息")
        
        # 获取所有可调用的函数
        ak_functions = {}
        
        # 方法1：直接扫描所有可调用对象
        for name in dir(ak):
            obj = getattr(ak, name)
            if callable(obj) and not name.startswith('_') and not inspect.isclass(obj) and not inspect.ismodule(obj):
                ak_functions[name] = obj
        
        # 方法2：递归扫描子模块
        self._scan_module(ak, ak_functions)
        
        self.logger.info(f"发现{len(ak_functions)}个AkShare接口函数")
        self._interface_cache = ak_functions
    
    def _scan_module(self, module, functions_dict, prefix=''):
        """
        递归扫描模块及其子模块中的所有函数
        
        Args:
            module: 要扫描的模块
            functions_dict: 存储函数的字典
            prefix: 函数名前缀
        """
        try:
            # 扫描当前模块中的函数，只获取函数签名而不执行
            for name in dir(module):
                if name.startswith('_'):
                    continue
                
                try:
                    # 使用getattr获取属性，但捕获可能的异常
                    obj = getattr(module, name)
                    full_name = f"{prefix}{name}" if prefix else name
                    
                    # 只检查是否可调用，不尝试执行函数
                    if callable(obj) and not inspect.isclass(obj) and not inspect.ismodule(obj):
                        # 避免重复添加
                        if full_name not in functions_dict:
                            functions_dict[full_name] = obj
                except Exception:
                    # 忽略获取属性时的错误，继续扫描其他函数
                    continue
                    
            # 扫描子模块，但避免触发实际网络请求
            if hasattr(module, '__path__'):
                for _, submodule_name, is_pkg in pkgutil.iter_modules(module.__path__):
                    try:
                        # 避免循环导入和触发网络请求
                        if f"{module.__name__}.{submodule_name}" in sys.modules:
                            submodule = sys.modules[f"{module.__name__}.{submodule_name}"]
                            
                            # 递归扫描已加载的子模块
                            if is_pkg:
                                new_prefix = f"{prefix}{submodule_name}." if prefix else f"{submodule_name}."
                                self._scan_module(submodule, functions_dict, new_prefix)
                            else:
                                # 扫描非包模块中的函数
                                for func_name in dir(submodule):
                                    if func_name.startswith('_'):
                                        continue
                                    
                                    try:
                                        func_obj = getattr(submodule, func_name)
                                        if callable(func_obj) and not inspect.isclass(func_obj) and not inspect.ismodule(func_obj):
                                            full_func_name = f"{submodule_name}.{func_name}"
                                            if full_func_name not in functions_dict:
                                                functions_dict[full_func_name] = func_obj
                                    except Exception:
                                        # 忽略获取属性时的错误
                                        continue
                        else:
                            # 对于未加载的模块，尝试安全导入但不执行初始化代码
                            try:
                                # 仅记录模块名称，不实际导入可能触发网络请求的模块
                                full_module_name = f"{module.__name__}.{submodule_name}"
                                if is_pkg:
                                    # 只添加模块名称作为前缀，不实际导入
                                    new_prefix = f"{prefix}{submodule_name}." if prefix else f"{submodule_name}."
                                    # 记录模块名但不递归扫描未加载模块
                                    self.logger.debug(f"跳过未加载模块的扫描: {full_module_name}")
                            except Exception:
                                # 忽略导入错误
                                pass
                    except (ImportError, AttributeError) as e:
                        # 记录警告但继续扫描其他模块
                        self.logger.debug(f"扫描子模块 {submodule_name} 时出错: {str(e)}")
        except Exception as e:
            self.logger.warning(f"扫描模块时出错: {str(e)}")
    
    def fetch(self, interface_name: str, **kwargs) -> Dict[str, Any]:
        """
        从AkShare获取数据
        
        Args:
            interface_name: 接口名称，对应AkShare中的函数名
            **kwargs: 接口参数
            
        Returns:
            Dict[str, Any]: 包含获取结果的字典
            
        Raises:
            FetcherError: 获取数据失败
        """
        self.logger.info(f"开始获取数据: {interface_name}, 参数: {kwargs}")
        
        try:
            # 检查接口是否存在
            if interface_name not in self._interface_cache and not hasattr(ak, interface_name):
                # 尝试处理可能的子模块路径
                if '.' in interface_name:
                    module_path, func_name = interface_name.rsplit('.', 1)
                    try:
                        module = importlib.import_module(f"akshare.{module_path}")
                        if hasattr(module, func_name):
                            func = getattr(module, func_name)
                            self._interface_cache[interface_name] = func
                        else:
                            raise FetcherError(f"接口不存在: {interface_name}")
                    except ImportError:
                        raise FetcherError(f"接口不存在: {interface_name}")
                else:
                    raise FetcherError(f"接口不存在: {interface_name}")
            
            # 获取接口函数
            func = self._interface_cache.get(interface_name) or getattr(ak, interface_name)
            
            # 调用接口函数
            start_time = datetime.now()
            result = func(**kwargs)
            end_time = datetime.now()
            
            # 处理结果
            if result is None:
                raise FetcherError(f"接口返回空结果: {interface_name}")
            
            # 转换为DataFrame（如果不是）
            if not isinstance(result, pd.DataFrame):
                if isinstance(result, (list, dict)):
                    result = pd.DataFrame(result)
                else:
                    raise FetcherError(f"接口返回类型不支持: {type(result)}")
            
            # 创建元数据
            metadata = self._create_metadata(interface_name, kwargs)
            metadata['execution_time'] = (end_time - start_time).total_seconds()
            metadata['row_count'] = len(result)
            metadata['column_count'] = len(result.columns)
            
            self.logger.info(f"数据获取成功: {interface_name}, 行数: {metadata['row_count']}, 列数: {metadata['column_count']}")
            
            return {
                'data': result,
                'metadata': metadata
            }
            
        except Exception as e:
            error_msg = f"获取数据失败: {interface_name}, 错误: {str(e)}"
            self.logger.error(error_msg)
            raise FetcherError(error_msg, details=str(e))
    
    def get_available_interfaces(self) -> List[str]:
        """
        获取可用接口列表
        
        Returns:
            List[str]: 可用接口名称列表
        """
        return list(self._interface_cache.keys())
    
    def get_interface_info(self, interface_name: str) -> Dict[str, Any]:
        """
        获取接口信息
        
        Args:
            interface_name: 接口名称
            
        Returns:
            Dict[str, Any]: 接口信息字典
            
        Raises:
            FetcherError: 接口不存在
        """
        # 检查接口是否存在
        if interface_name not in self._interface_cache and not hasattr(ak, interface_name):
            # 尝试处理可能的子模块路径
            if '.' in interface_name:
                module_path, func_name = interface_name.rsplit('.', 1)
                try:
                    module = importlib.import_module(f"akshare.{module_path}")
                    if hasattr(module, func_name):
                        func = getattr(module, func_name)
                        self._interface_cache[interface_name] = func
                    else:
                        raise FetcherError(f"接口不存在: {interface_name}")
                except ImportError:
                    raise FetcherError(f"接口不存在: {interface_name}")
            else:
                raise FetcherError(f"接口不存在: {interface_name}")
        
        # 获取接口函数
        func = self._interface_cache.get(interface_name) or getattr(ak, interface_name)
        
        # 获取函数签名
        sig = inspect.signature(func)
        
        # 获取函数文档
        doc = inspect.getdoc(func) or "无文档"
        
        # 构建参数信息
        parameters = {}
        for name, param in sig.parameters.items():
            parameters[name] = {
                'name': name,
                'default': str(param.default) if param.default is not inspect.Parameter.empty else None,
                'annotation': str(param.annotation) if param.annotation is not inspect.Parameter.empty else None,
                'required': param.default is inspect.Parameter.empty
            }
        
        return {
            'name': interface_name,
            'doc': doc,
            'parameters': parameters,
            'module': func.__module__
        }
    
    def search_interfaces(self, keyword: str) -> List[str]:
        """
        搜索包含关键字的接口
        
        Args:
            keyword: 关键字
            
        Returns:
            List[str]: 匹配的接口名称列表
        """
        result = []
        for name in self._interface_cache.keys():
            if keyword.lower() in name.lower():
                result.append(name)
        return result
    
    def get_interface_categories(self) -> Dict[str, List[str]]:
        """
        获取接口分类
        
        Returns:
            Dict[str, List[str]]: 分类接口字典，格式为 {分类名: [接口名列表]}
        """
        categories = {}
        
        for name in self._interface_cache.keys():
            # 根据函数名或模块路径分类
            if '.' in name:
                # 对于子模块路径，使用第一级路径作为分类
                category = name.split('.')[0]
            else:
                # 对于直接函数，使用函数名前缀作为分类
                parts = name.split('_')
                if len(parts) > 1:
                    category = parts[0]  # 使用第一个部分作为分类
                else:
                    category = "其他"
            
            if category not in categories:
                categories[category] = []
            categories[category].append(name)
        
        return categories
