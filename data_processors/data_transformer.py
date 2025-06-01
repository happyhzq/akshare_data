"""
数据转换器模块，负责转换数据格式，适配数据库存储需求
"""
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime

from core.exceptions import ProcessorError
from core.logger import LoggerManager


class DataTransformer:
    """数据转换器类，负责转换数据格式，适配数据库存储需求"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据转换器
        
        Args:
            config: 配置信息，可包含以下字段：
                - schema_mapping: 数据模式映射，定义如何将数据映射到数据库表
                - add_metadata: 是否添加元数据列
                - batch_size: 批处理大小
        """
        self.config = config
        self.schema_mapping = config.get('schema_mapping', {})
        self.add_metadata = config.get('add_metadata', True)
        self.batch_size = config.get('batch_size', 1000)
        self.logger = LoggerManager().get_logger("DataTransformer")
    
    def transform(self, data: pd.DataFrame, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        转换数据
        
        Args:
            data: 清洗后的数据DataFrame
            metadata: 数据元信息
            
        Returns:
            Dict[str, Any]: 转换后的数据，格式为：
            {
                'table_data': 转换后的数据DataFrame,
                'table_name': 目标表名,
                'metadata': 更新后的元数据
            }
            
        Raises:
            ProcessorError: 数据转换失败
        """
        try:
            self.logger.info(f"开始转换数据，数据形状: {data.shape}")
            
            # 获取接口名称
            interface_name = metadata.get('interface', '')
            
            # 查找匹配的模式映射
            mapping = self._find_schema_mapping(interface_name)
            if not mapping:
                raise ProcessorError(f"未找到接口 {interface_name} 的模式映射")
            
            # 获取目标表名
            table_name = mapping.get('table_name', '')
            if not table_name:
                raise ProcessorError(f"接口 {interface_name} 的模式映射中未指定目标表名")
            
            # 复制数据，避免修改原始数据
            df = data.copy()
            
            # 应用列映射
            column_mapping = mapping.get('column_mapping', {})
            if column_mapping:
                # 只保留映射中的列
                columns_to_keep = list(column_mapping.keys())
                df = df[columns_to_keep].copy()
                
                # 重命名列
                df = df.rename(columns=column_mapping)
            
            # 添加元数据列
            if self.add_metadata:
                # 添加获取时间
                if 'fetch_time' not in df.columns and 'fetch_time' in metadata:
                    df['fetch_time'] = metadata['fetch_time']
                
                # 添加接口ID
                if 'interface_id' not in df.columns and mapping.get('interface_id'):
                    df['interface_id'] = mapping['interface_id']
            
            # 应用数据转换函数
            transformers = mapping.get('transformers', {})
            for column, transformer in transformers.items():
                if column in df.columns:
                    df[column] = df[column].apply(transformer)
            
            # 更新元数据
            metadata['table_name'] = table_name
            metadata['row_count_after_transform'] = len(df)
            metadata['transform_time'] = datetime.now()
            
            self.logger.info(f"数据转换完成，目标表: {table_name}, 转换后数据形状: {df.shape}")
            
            return {
                'table_data': df,
                'table_name': table_name,
                'metadata': metadata
            }
            
        except Exception as e:
            error_msg = f"数据转换失败: {str(e)}"
            self.logger.error(error_msg)
            raise ProcessorError(error_msg, details=str(e))
    
    def _find_schema_mapping(self, interface_name: str) -> Dict[str, Any]:
        """
        查找接口对应的模式映射
        
        Args:
            interface_name: 接口名称
            
        Returns:
            Dict[str, Any]: 模式映射字典，如果未找到则返回空字典
        """
        # 直接匹配
        if interface_name in self.schema_mapping:
            return self.schema_mapping[interface_name]
        
        # 模式匹配
        for pattern, mapping in self.schema_mapping.items():
            if '*' in pattern:
                # 将*替换为正则表达式
                import re
                regex_pattern = pattern.replace('*', '.*')
                if re.match(regex_pattern, interface_name):
                    return mapping
        
        return {}
    
    def batch_transform(self, data: pd.DataFrame, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        批量转换数据
        
        Args:
            data: 清洗后的数据DataFrame
            metadata: 数据元信息
            
        Returns:
            List[Dict[str, Any]]: 批量转换后的数据列表
            
        Raises:
            ProcessorError: 数据转换失败
        """
        try:
            self.logger.info(f"开始批量转换数据，数据形状: {data.shape}, 批处理大小: {self.batch_size}")
            
            # 计算批次数
            total_rows = len(data)
            batch_count = (total_rows + self.batch_size - 1) // self.batch_size
            
            result = []
            for i in range(batch_count):
                # 计算批次范围
                start_idx = i * self.batch_size
                end_idx = min((i + 1) * self.batch_size, total_rows)
                
                # 获取批次数据
                batch_data = data.iloc[start_idx:end_idx].copy()
                
                # 复制元数据并添加批次信息
                batch_metadata = metadata.copy()
                batch_metadata['batch_index'] = i
                batch_metadata['batch_start'] = start_idx
                batch_metadata['batch_end'] = end_idx
                
                # 转换批次数据
                transformed = self.transform(batch_data, batch_metadata)
                result.append(transformed)
                
                self.logger.debug(f"完成批次 {i+1}/{batch_count} 的转换")
            
            self.logger.info(f"批量转换完成，共 {batch_count} 个批次")
            
            return result
            
        except Exception as e:
            error_msg = f"批量转换数据失败: {str(e)}"
            self.logger.error(error_msg)
            raise ProcessorError(error_msg, details=str(e))
