"""
数据清洗器模块，负责清洗和标准化原始数据
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime

from core.exceptions import ProcessorError
from core.logger import LoggerManager


class BaseDataCleaner(ABC):
    """数据清洗器基类，定义数据清洗的通用接口"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据清洗器
        
        Args:
            config: 配置信息
        """
        self.config = config
        self.name = self.__class__.__name__
        self.logger = LoggerManager().get_logger(self.name)
    
    @abstractmethod
    def clean(self, data: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
        """
        清洗数据的抽象方法
        
        Args:
            data: 原始数据DataFrame
            metadata: 数据元信息
            
        Returns:
            pd.DataFrame: 清洗后的数据
        """
        pass


class StandardDataCleaner(BaseDataCleaner):
    """标准数据清洗器，提供通用的数据清洗功能"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化标准数据清洗器
        
        Args:
            config: 配置信息，可包含以下字段：
                - column_mapping: 列名映射字典
                - date_columns: 日期列列表
                - numeric_columns: 数值列列表
                - categorical_columns: 分类列列表
                - text_columns: 文本列列表
                - drop_columns: 需要删除的列列表
                - fill_na: 是否填充缺失值
                - na_values: 自定义缺失值列表
                - add_params_as_columns: 是否将接口参数添加为列
        """
        super().__init__(config)
        self.column_mapping = config.get('column_mapping', {})
        self.date_columns = config.get('date_columns', [])
        self.numeric_columns = config.get('numeric_columns', [])
        self.categorical_columns = config.get('categorical_columns', [])
        self.text_columns = config.get('text_columns', [])
        self.drop_columns = config.get('drop_columns', [])
        self.fill_na = config.get('fill_na', True)
        self.na_values = config.get('na_values', ['-', 'N/A', 'n/a', 'null', 'NULL', 'None', ''])
        self.add_params_as_columns = config.get('add_params_as_columns', True)
    
    def clean(self, data: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
        """
        清洗数据
        
        Args:
            data: 原始数据DataFrame
            metadata: 数据元信息
            
        Returns:
            pd.DataFrame: 清洗后的数据
            
        Raises:
            ProcessorError: 数据清洗失败
        """
        try:
            self.logger.info(f"开始清洗数据，原始数据形状: {data.shape}")
            
            # 复制数据，避免修改原始数据
            df = data.copy()
            
            # 1. 重命名列
            if self.column_mapping:
                df = self._rename_columns(df)
            
            # 2. 删除不需要的列
            if self.drop_columns:
                df = self._drop_columns(df)
            
            # 3. 处理缺失值
            df = self._handle_missing_values(df)
            
            # 4. 转换数据类型
            df = self._convert_data_types(df)
            
            # 5. 标准化文本列
            df = self._standardize_text_columns(df)
            
            # 6. 处理异常值
            df = self._handle_outliers(df)
            
            # 7. 添加接口参数作为列
            if self.add_params_as_columns:
                df = self._add_params_as_columns(df, metadata)
            
            self.logger.info(f"数据清洗完成，清洗后数据形状: {df.shape}")
            
            return df
            
        except Exception as e:
            error_msg = f"数据清洗失败: {str(e)}"
            self.logger.error(error_msg)
            raise ProcessorError(error_msg, details=str(e))
    
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重命名列"""
        try:
            # 只重命名存在的列
            existing_columns = {k: v for k, v in self.column_mapping.items() if k in df.columns}
            if existing_columns:
                df = df.rename(columns=existing_columns)
                self.logger.debug(f"重命名列: {existing_columns}")
            return df
        except Exception as e:
            self.logger.warning(f"重命名列失败: {str(e)}")
            return df
    
    def _drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """删除不需要的列"""
        try:
            # 只删除存在的列
            columns_to_drop = [col for col in self.drop_columns if col in df.columns]
            if columns_to_drop:
                df = df.drop(columns=columns_to_drop)
                self.logger.debug(f"删除列: {columns_to_drop}")
            return df
        except Exception as e:
            self.logger.warning(f"删除列失败: {str(e)}")
            return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理缺失值"""
        try:
            # 替换自定义缺失值为NaN
            if self.na_values:
                df = df.replace(self.na_values, np.nan)
            
            # 统计缺失值
            missing_count = df.isna().sum()
            missing_cols = missing_count[missing_count > 0]
            if not missing_cols.empty:
                self.logger.debug(f"缺失值统计: {missing_cols.to_dict()}")
            
            # 填充缺失值
            if self.fill_na:
                # 数值列使用中位数填充
                for col in [c for c in self.numeric_columns if c in df.columns]:
                    if df[col].isna().any():
                        median_value = df[col].median()
                        df[col] = df[col].fillna(median_value)
                        self.logger.debug(f"列 {col} 使用中位数 {median_value} 填充缺失值")
                
                # 分类列使用众数填充
                for col in [c for c in self.categorical_columns if c in df.columns]:
                    if df[col].isna().any():
                        mode_value = df[col].mode()[0]
                        df[col] = df[col].fillna(mode_value)
                        self.logger.debug(f"列 {col} 使用众数 {mode_value} 填充缺失值")
                
                # 日期列使用前值填充
                for col in [c for c in self.date_columns if c in df.columns]:
                    if df[col].isna().any():
                        df[col] = df[col].fillna(method='ffill')
                        self.logger.debug(f"列 {col} 使用前值填充缺失值")
                
                # 文本列使用空字符串填充
                for col in [c for c in self.text_columns if c in df.columns]:
                    if df[col].isna().any():
                        df[col] = df[col].fillna('')
                        self.logger.debug(f"列 {col} 使用空字符串填充缺失值")
            
            return df
        except Exception as e:
            self.logger.warning(f"处理缺失值失败: {str(e)}")
            return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换数据类型"""
        try:
            # 转换日期列
            for col in [c for c in self.date_columns if c in df.columns]:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    self.logger.debug(f"列 {col} 转换为日期类型")
                except Exception as e:
                    self.logger.warning(f"列 {col} 转换为日期类型失败: {str(e)}")
            
            # 转换数值列
            for col in [c for c in self.numeric_columns if c in df.columns]:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    self.logger.debug(f"列 {col} 转换为数值类型")
                except Exception as e:
                    self.logger.warning(f"列 {col} 转换为数值类型失败: {str(e)}")
            
            # 转换分类列
            for col in [c for c in self.categorical_columns if c in df.columns]:
                try:
                    df[col] = df[col].astype('category')
                    self.logger.debug(f"列 {col} 转换为分类类型")
                except Exception as e:
                    self.logger.warning(f"列 {col} 转换为分类类型失败: {str(e)}")
            
            # 转换文本列
            for col in [c for c in self.text_columns if c in df.columns]:
                try:
                    df[col] = df[col].astype(str)
                    self.logger.debug(f"列 {col} 转换为文本类型")
                except Exception as e:
                    self.logger.warning(f"列 {col} 转换为文本类型失败: {str(e)}")
            
            return df
        except Exception as e:
            self.logger.warning(f"转换数据类型失败: {str(e)}")
            return df
    
    def _standardize_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化文本列"""
        try:
            for col in [c for c in self.text_columns if c in df.columns]:
                # 去除前后空格
                df[col] = df[col].str.strip()
                
                # 替换多个空格为单个空格
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                
                self.logger.debug(f"标准化文本列 {col}")
            
            return df
        except Exception as e:
            self.logger.warning(f"标准化文本列失败: {str(e)}")
            return df
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理异常值"""
        try:
            for col in [c for c in self.numeric_columns if c in df.columns]:
                # 使用IQR方法检测异常值
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # 统计异常值
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
                if not outliers.empty:
                    self.logger.debug(f"列 {col} 检测到 {len(outliers)} 个异常值")
                    
                    # 将异常值替换为边界值
                    df.loc[df[col] < lower_bound, col] = lower_bound
                    df.loc[df[col] > upper_bound, col] = upper_bound
                    self.logger.debug(f"列 {col} 异常值已替换为边界值")
            
            return df
        except Exception as e:
            self.logger.warning(f"处理异常值失败: {str(e)}")
            return df
    
    def _add_params_as_columns(self, df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
        """
        将接口参数添加为数据列
        
        Args:
            df: 数据DataFrame
            metadata: 数据元信息，包含接口参数
            
        Returns:
            pd.DataFrame: 添加参数列后的DataFrame
        """
        try:
            # 从metadata中获取接口参数
            interface_params = metadata.get('interface_params', {})
            
            if not interface_params:
                self.logger.debug("未找到接口参数，跳过添加参数列")
                return df
            
            self.logger.info(f"开始将接口参数添加为数据列: {interface_params}")
            
            # 为每个参数创建一个新列
            for param_name, param_value in interface_params.items():
                # 创建列名，添加param_前缀避免与现有列冲突
                column_name = f"param_{param_name}"
                
                # 如果列已存在，则跳过
                if column_name in df.columns:
                    self.logger.debug(f"列 {column_name} 已存在，跳过添加")
                    continue
                
                # 将参数值添加为新列，填充到所有行
                df[column_name] = param_value
                self.logger.info(f"添加参数列 {column_name} = {param_value}")
            
            return df
        except Exception as e:
            self.logger.warning(f"添加参数列失败: {str(e)}")
            return df


class DataCleanerFactory:
    """数据清洗器工厂类，负责创建各类数据清洗器实例"""
    
    _cleaner_registry = {
        'standard': StandardDataCleaner
    }
    
    @classmethod
    def register_cleaner(cls, name: str, cleaner_class) -> None:
        """
        注册数据清洗器类
        
        Args:
            name: 清洗器名称
            cleaner_class: 清洗器类，必须继承自BaseDataCleaner
        
        Raises:
            TypeError: 清洗器类型错误
        """
        if not issubclass(cleaner_class, BaseDataCleaner):
            raise TypeError(f"清洗器类必须继承自BaseDataCleaner: {cleaner_class.__name__}")
        
        cls._cleaner_registry[name] = cleaner_class
    
    @classmethod
    def create_cleaner(cls, cleaner_type: str, config: Dict[str, Any]) -> BaseDataCleaner:
        """
        创建数据清洗器实例
        
        Args:
            cleaner_type: 清洗器类型
            config: 配置信息
            
        Returns:
            BaseDataCleaner: 数据清洗器实例
            
        Raises:
            ProcessorError: 清洗器类型不支持
        """
        if cleaner_type not in cls._cleaner_registry:
            raise ProcessorError(f"不支持的清洗器类型: {cleaner_type}")
        
        cleaner_class = cls._cleaner_registry[cleaner_type]
        return cleaner_class(config)
    
    @classmethod
    def get_supported_cleaners(cls) -> Dict[str, Any]:
        """
        获取支持的清洗器类型
        
        Returns:
            Dict[str, Any]: 支持的清洗器类型字典，格式为 {类型名: 类}
        """
        return cls._cleaner_registry.copy()
