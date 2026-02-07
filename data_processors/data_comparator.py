"""
数据比对器模块，负责比对新数据与已有数据的差异
"""
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd
from datetime import datetime

from core.exceptions import ProcessorError
from core.logger import LoggerManager


class DataComparator:
    """数据比对器类，负责比对新数据与已有数据的差异"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据比对器
        
        Args:
            config: 配置信息，可包含以下字段：
                - key_columns: 用于比对的键列字典，格式为 {表名: [列名列表]}
                - compare_all_columns: 是否比对所有列
                - batch_size: 批处理大小
        """
        self.config = config
        self.key_columns = config.get('key_columns', {})
        self.compare_all_columns = config.get('compare_all_columns', False)
        self.batch_size = config.get('batch_size', 1000)
        self.logger = LoggerManager().get_logger("DataComparator")
    
    def compare(self, new_data: pd.DataFrame, existing_data: pd.DataFrame, table_name: str) -> Dict[str, pd.DataFrame]:
        """
        比对新数据与已有数据
        
        Args:
            new_data: 新数据DataFrame
            existing_data: 已有数据DataFrame
            table_name: 表名
            
        Returns:
            Dict[str, pd.DataFrame]: 比对结果，包含以下键：
                - 'to_insert': 需要插入的数据
                - 'to_update': 需要更新的数据
                - 'unchanged': 无需变更的数据
            
        Raises:
            ProcessorError: 数据比对失败
        """
        try:
            self.logger.info(f"开始比对数据，表: {table_name}, 新数据: {len(new_data)}行, 已有数据: {len(existing_data)}行")
            
            # 如果已有数据为空，则所有新数据都需要插入
            if existing_data.empty:
                self.logger.info(f"已有数据为空，所有新数据将被插入")
                return {
                    'to_insert': new_data,
                    'to_update': pd.DataFrame(),
                    'unchanged': pd.DataFrame()
                }
            
            # 如果新数据为空，则无需任何操作
            if new_data.empty:
                self.logger.info(f"新数据为空，无需任何操作")
                return {
                    'to_insert': pd.DataFrame(),
                    'to_update': pd.DataFrame(),
                    'unchanged': pd.DataFrame()
                }
            
            # 获取比对键列
            keys = self._get_key_columns(table_name, new_data)
            if not keys:
                raise ProcessorError(f"未找到表 {table_name} 的比对键列")
            
            self.logger.debug(f"使用键列进行比对: {keys}")
            
            # 确保两个DataFrame都有这些键列
            for key in keys:
                if key not in new_data.columns:
                    raise ProcessorError(f"新数据中缺少键列: {key}")
                if key not in existing_data.columns:
                    raise ProcessorError(f"已有数据中缺少键列: {key}")
            
            # 创建键列组合
            new_data['_key'] = self._create_composite_key(new_data, keys)
            existing_data['_key'] = self._create_composite_key(existing_data, keys)
            
            # 找出需要插入的数据（在新数据中但不在已有数据中）
            to_insert_mask = ~new_data['_key'].isin(existing_data['_key'])
            to_insert = new_data[to_insert_mask].copy()

            # 找出可能需要更新的数据（在新数据中也在已有数据中）
            to_check_update = new_data[~to_insert_mask].copy()
            
            # 如果没有需要检查更新的数据，直接返回结果
            if to_check_update.empty:
                self.logger.info(f"所有新数据都需要插入，无需更新")
                # 删除临时键列
                to_insert = to_insert.drop(columns=['_key'])
                return {
                    'to_insert': to_insert,
                    'to_update': pd.DataFrame(),
                    'unchanged': pd.DataFrame()
                }
            
            # 将已有数据转换为字典，便于快速查找
            existing_dict = existing_data.set_index('_key').to_dict('index')
            
            # 比对所有列或指定列
            compare_columns = list(new_data.columns)
            if not self.compare_all_columns:
                # 排除键列和内部列
                compare_columns = [col for col in compare_columns if col not in keys and not col.startswith('_')]
            
            # 分别存储需要更新和无需变更的数据
            to_update_rows = []
            unchanged_rows = []
            
            # 逐行比对
            for _, row in to_check_update.iterrows():
                key = row['_key']
                existing_row = existing_dict.get(key, {})
                
                # 检查是否有差异
                has_diff = False
                for col in compare_columns:
                    if col not in existing_row:
                        continue
                        
                    # 对浮点数使用近似比较
                    if pd.api.types.is_float_dtype(pd.Series([row[col]])) or pd.api.types.is_float_dtype(pd.Series([existing_row[col]])):
                        try:
                            # 允许0.000001的误差（可根据实际情况调整）
                            if abs(float(row[col]) - float(existing_row[col])) > 0.000001:
                                has_diff = True
                                break
                        except (ValueError, TypeError):
                            # 处理无法转换为浮点数的情况
                            if row[col] != existing_row[col]:
                                has_diff = True
                                break
                    else:
                        # 其他类型使用精确比较
                        if row[col] != existing_row[col]:
                            has_diff = True
                            break
                
                if has_diff:
                    to_update_rows.append(row)
                else:
                    unchanged_rows.append(row)
            
            # 转换为DataFrame
            to_update = pd.DataFrame(to_update_rows) if to_update_rows else pd.DataFrame()
            unchanged = pd.DataFrame(unchanged_rows) if unchanged_rows else pd.DataFrame()
            
            # 删除临时键列
            to_insert = to_insert.drop(columns=['_key']) if not to_insert.empty else to_insert
            to_update = to_update.drop(columns=['_key']) if not to_update.empty else to_update
            unchanged = unchanged.drop(columns=['_key']) if not unchanged.empty else unchanged
            
            self.logger.info(f"数据比对完成，需插入: {len(to_insert)}行, 需更新: {len(to_update)}行, 无需变更: {len(unchanged)}行")
            print(to_update)
            return {
                'to_insert': to_insert,
                'to_update': to_update,
                'unchanged': unchanged
            }
            
        except Exception as e:
            error_msg = f"数据比对失败: {str(e)}"
            self.logger.error(error_msg)
            raise ProcessorError(error_msg, details=str(e))
    
    def _get_key_columns(self, table_name: str, data: pd.DataFrame) -> List[str]:
        """
        获取表的比对键列
        
        Args:
            table_name: 表名
            data: 数据DataFrame
            
        Returns:
            List[str]: 键列列表
        """
        # 从配置中获取
        if table_name in self.key_columns:
            return self.key_columns[table_name]
        
        # 默认使用所有非时间戳列作为键
        exclude_cols = ['insert_time', 'update_time', 'created_at', 'updated_at']
        return [col for col in data.columns if col not in exclude_cols]
    
    def _create_composite_key(self, df: pd.DataFrame, keys: List[str]) -> pd.Series:
        """
        创建组合键，使用更健壮的浮点数处理
        """
        # 创建一个临时DataFrame用于生成键
        key_df = df[keys].copy()
        
        # 对所有列进行标准化处理
        for col in keys:
            # 尝试将所有值转换为浮点数并格式化，无论列的原始类型如何
            try:
                # 先检查列是否包含可以转换为浮点数的值
                can_convert_to_float = True
                for val in df[col].dropna().unique():
                    try:
                        float(val)
                    except (ValueError, TypeError):
                        can_convert_to_float = False
                        break
                
                if can_convert_to_float:
                    # 如果所有值都可以转换为浮点数，则进行标准化格式化
                    key_df[col] = df[col].apply(
                        lambda x: f"{float(x):.8f}" if pd.notnull(x) and x != "" else ""
                    )
                    self.logger.debug(f"列 {col} 被识别为数值列，已进行浮点数标准化")
                else:
                    # 否则保持原样
                    key_df[col] = df[col]
                    self.logger.debug(f"列 {col} 被识别为非数值列，保持原样")
            except Exception as e:
                # 如果处理过程中出错，保持原样
                self.logger.warning(f"处理列 {col} 时出错: {str(e)}，保持原样")
                key_df[col] = df[col]
        
        # 将所有键列转换为字符串并连接
        return key_df.astype(str).agg('|'.join, axis=1)

    
    def batch_compare(self, new_data: pd.DataFrame, get_existing_data_func: callable, table_name: str) -> Dict[str, pd.DataFrame]:
        """
        批量比对数据
        
        Args:
            new_data: 新数据DataFrame
            get_existing_data_func: 获取已有数据的函数，接受键列值列表作为参数
            table_name: 表名
            
        Returns:
            Dict[str, pd.DataFrame]: 批量比对结果
            
        Raises:
            ProcessorError: 数据比对失败
        """
        try:
            self.logger.info(f"开始批量比对数据，表: {table_name}, 新数据: {len(new_data)}行, 批处理大小: {self.batch_size}")
            
            # 获取比对键列
            keys = self._get_key_columns(table_name, new_data)
            if not keys:
                raise ProcessorError(f"未找到表 {table_name} 的比对键列")
            
            # 计算批次数
            total_rows = len(new_data)
            batch_count = (total_rows + self.batch_size - 1) // self.batch_size
            
            # 初始化结果
            all_to_insert = []
            all_to_update = []
            all_unchanged = []
            
            for i in range(batch_count):
                # 计算批次范围
                start_idx = i * self.batch_size
                end_idx = min((i + 1) * self.batch_size, total_rows)
                
                # 获取批次数据
                batch_data = new_data.iloc[start_idx:end_idx].copy()
                
                # 提取键列值
                key_values = batch_data[keys].values.tolist()
                
                # 获取已有数据
                existing_data = get_existing_data_func(keys, key_values)
                
                # 比对数据
                result = self.compare(batch_data, existing_data, table_name)
                
                # 累加结果
                if not result['to_insert'].empty:
                    all_to_insert.append(result['to_insert'])
                if not result['to_update'].empty:
                    all_to_update.append(result['to_update'])
                if not result['unchanged'].empty:
                    all_unchanged.append(result['unchanged'])
                
                self.logger.debug(f"完成批次 {i+1}/{batch_count} 的比对")
            
            # 合并结果
            final_to_insert = pd.concat(all_to_insert) if all_to_insert else pd.DataFrame()
            final_to_update = pd.concat(all_to_update) if all_to_update else pd.DataFrame()
            final_unchanged = pd.concat(all_unchanged) if all_unchanged else pd.DataFrame()
            
            self.logger.info(f"批量比对完成，需插入: {len(final_to_insert)}行, 需更新: {len(final_to_update)}行, 无需变更: {len(final_unchanged)}行")
            
            return {
                'to_insert': final_to_insert,
                'to_update': final_to_update,
                'unchanged': final_unchanged
            }
            
        except Exception as e:
            error_msg = f"批量比对数据失败: {str(e)}"
            self.logger.error(error_msg)
            raise ProcessorError(error_msg, details=str(e))
