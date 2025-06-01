"""
数据存储器模块，处理数据的存储、更新和查询
"""
import os
import logging
from typing import Dict, Any, List, Tuple, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime
import json
import traceback

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, select, or_, and_, func, inspect, Numeric, Text
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError, OperationalError, NoSuchTableError, InternalError, IntegrityError
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import text

# 尝试导入PyMySQL特定异常，用于更精确的异常捕获
try:
    import pymysql
    from pymysql.err import OperationalError as PyMySQLOperationalError
    from pymysql.err import ProgrammingError as PyMySQLProgrammingError
    from pymysql.err import InternalError as PyMySQLInternalError
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False

from core.exceptions import DatabaseError
from core.logger import LoggerManager


class DataStore:
    """数据存储器，处理数据的存储、更新和查询"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据存储器
        
        Args:
            config: 配置信息，可包含以下字段：
                - driver: 数据库驱动
                - host: 数据库主机
                - port: 数据库端口
                - username: 数据库用户名
                - password: 数据库密码
                - database: 数据库名
                - url: 完整的数据库URL（如果提供，将忽略上述字段）
                - pool_size: 连接池大小
                - max_overflow: 最大溢出连接数
                - pool_recycle: 连接回收时间（秒）
                - connect_args: 连接参数
                - batch_size: 批处理大小
                - retry_count: 重试次数
                - auto_create_table: 是否自动创建表
                - auto_create_table_options: 自动创建表的选项
        """
        self.config = config
        self.name = self.__class__.__name__
        self.logger = LoggerManager().get_logger(self.name)
        
        # 初始化数据库连接
        self._init_db_connection()
        
        # 批处理大小
        self.batch_size = config.get('batch_size', 1000)
        
        # 重试次数
        self.retry_count = config.get('retry_count', 3)
        
        # 自动建表配置
        self.auto_create_table = config.get('auto_create_table', False)
        self.auto_create_table_options = config.get('auto_create_table_options', {
            'add_primary_key': True,
            'add_insert_time': True,
            'add_update_time': True
        })
        
        self.logger.debug(f"数据存储器初始化完成，自动建表: {self.auto_create_table}")
    
    def _init_db_connection(self) -> None:
        """
        初始化数据库连接
        
        Raises:
            DatabaseError: 数据库连接失败
        """
        try:
            # 如果提供了完整URL，直接使用
            if 'url' in self.config:
                self.db_url = self.config['url']
            else:
                # 否则，构建URL
                driver = self.config.get('driver', 'mysql+pymysql')
                host = self.config.get('host', 'localhost')
                port = self.config.get('port', 3306)
                username = self.config.get('username', 'root')
                password = self.config.get('password', )
                database = self.config.get('database', )
                
                self.db_url = f"{driver}://{username}:{password}@{host}:{port}/{database}"
            
            # 连接参数
            connect_args = self.config.get('connect_args', {})
            
            # 连接池配置
            pool_size = self.config.get('pool_size', 5)
            max_overflow = self.config.get('max_overflow', 10)
            pool_recycle = self.config.get('pool_recycle', 3600)
            
            # 创建引擎
            self.engine = create_engine(
                self.db_url,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=pool_recycle,
                connect_args=connect_args
            )
            
            # 创建元数据对象
            self.metadata = MetaData()
            
            # 创建会话工厂
            self.Session = sessionmaker(bind=self.engine)
            
            # 记录连接信息（不包含敏感信息）
            db_info = f"{self.config.get('host', 'localhost')}:{self.config.get('port', 3306)}/{self.config.get('database', '')}"
            self.logger.info(f"数据库连接初始化成功: {db_info}")
            
            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                self.logger.info("数据库连接测试成功")
            
        except Exception as e:
            error_msg = f"数据库连接初始化失败: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg, details=str(e))
    
    def query_data(self, table_name: str, conditions: Dict[str, Any] = None, limit: int = None) -> pd.DataFrame:
        """
        查询数据
        
        Args:
            table_name: 表名
            conditions: 查询条件，格式为 {列名: 值} 或 {列名: [值1, 值2, ...]}
            limit: 限制返回的行数
            
        Returns:
            pd.DataFrame: 查询结果DataFrame
            
        Raises:
            DatabaseError: 数据查询失败
        """
        self.logger.info(f"开始查询表 {table_name} 中的数据")
        
        try:
            # 创建会话
            session = self.Session()
            
            try:
                # 反射表结构
                try:
                    table = Table(table_name, self.metadata, autoload_with=self.engine)
                except (SQLAlchemyError, NoSuchTableError) as e:
                    # 捕获表不存在的异常
                    error_msg = str(e).lower()
                    if any(msg in error_msg for msg in ["doesn't exist", "no such table", "unknown table", "table not found", "table or view does not exist", "exist"]):
                        self.logger.warning(f"表 {table_name} 不存在，且未启用自动建表")
                        if self.auto_create_table:
                            self.logger.info(f"自动建表功能已启用，但需要数据来推断表结构。将返回空DataFrame。")
                        raise DatabaseError(f"表 {table_name} 不存在，且未启用自动建表")
                    else:
                        # 其他SQLAlchemy错误
                        raise DatabaseError(f"反射表结构失败: {str(e)}")
                
                # 构建查询
                query = select([table])
                
                # 添加条件
                if conditions:
                    condition_clauses = []
                    for column, value in conditions.items():
                        if isinstance(value, list):
                            # 如果值是列表，使用IN操作符
                            condition_clauses.append(table.c[column].in_(value))
                        else:
                            # 否则使用等于操作符
                            condition_clauses.append(table.c[column] == value)
                    
                    if condition_clauses:
                        query = query.where(and_(*condition_clauses))
                
                # 添加限制
                if limit:
                    query = query.limit(limit)
                
                # 执行查询
                result = session.execute(query)
                
                # 转换为DataFrame
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                self.logger.info(f"查询成功，返回 {len(df)} 行数据")
                
                return df
                
            except DatabaseError:
                # 重新抛出DatabaseError
                raise
            except Exception as e:
                # 捕获所有其他异常
                error_msg = f"数据查询失败: {table_name}"
                self.logger.error(f"{error_msg}: {str(e)}")
                
                # 检查是否是表不存在错误
                error_str = str(e).lower()
                if any(msg in error_str for msg in ["doesn't exist", "no such table", "unknown table", "table not found", "table or view does not exist"]):
                    self.logger.warning(f"表 {table_name} 不存在，将返回空DataFrame")
                    # 返回空DataFrame
                    return pd.DataFrame()
                
                # 其他错误，抛出异常
                raise DatabaseError(error_msg, details=str(e))
                
            finally:
                # 关闭会话
                session.close()
                
        except DatabaseError:
            # 重新抛出DatabaseError
            raise
        except Exception as e:
            # 捕获所有其他异常
            error_msg = f"数据查询失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            
            # 检查是否是表不存在错误
            error_str = str(e).lower()
            if any(msg in error_str for msg in ["doesn't exist", "no such table", "unknown table", "table not found", "table or view does not exist"]):
                self.logger.warning(f"表 {table_name} 不存在，将返回空DataFrame")
                # 返回空DataFrame
                return pd.DataFrame()
            
            # 其他错误，抛出异常
            raise DatabaseError(error_msg, details=str(e))
    
    def insert_data(self, data: pd.DataFrame, table_name: str, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        插入数据
        
        Args:
            data: 要插入的数据DataFrame
            table_name: 表名
            metadata: 数据元信息
            
        Returns:
            Dict[str, Any]: 插入结果
            
        Raises:
            DatabaseError: 数据插入失败
        """
        if data.empty:
            self.logger.warning(f"要插入的数据为空，跳过插入")
            return {'success': True, 'inserted': 0, 'failed': 0}
        
        self.logger.info(f"开始插入数据到表 {table_name}，数据行数: {len(data)}")
        
        try:
            # 创建会话
            session = self.Session()
            
            try:
                # 反射表结构
                try:
                    table = Table(table_name, self.metadata, autoload_with=self.engine)
                    self.logger.debug(f"表 {table_name} 已存在，使用现有表结构")
                except (SQLAlchemyError, NoSuchTableError) as e:
                    # 捕获表不存在的异常
                    error_msg = str(e).lower()
                    if any(msg in error_msg for msg in ["doesn't exist", "no such table", "unknown table", "table not found", "table or view does not exist"]):
                        # 如果启用了自动建表，则创建表
                        if self.auto_create_table:
                            self.logger.info(f"表 {table_name} 不存在，将自动创建")
                            table = self._create_table_from_dataframe(data, table_name, session)
                        else:
                            self.logger.error(f"表 {table_name} 不存在，且未启用自动建表")
                            raise DatabaseError(f"表 {table_name} 不存在，且未启用自动建表")
                    else:
                        # 其他SQLAlchemy错误
                        raise DatabaseError(f"反射表结构失败: {str(e)}")
                
                # 添加插入时间和更新时间
                df = data.copy()
                now = datetime.now()
                
                # 检查表是否有insert_time列
                if 'insert_time' in table.columns and 'insert_time' not in df.columns:
                    df['insert_time'] = now
                
                # 检查表是否有update_time列
                if 'update_time' in table.columns and 'update_time' not in df.columns:
                    df['update_time'] = now
                
                # 将数据分批插入
                total_rows = len(df)
                inserted = 0
                failed = 0
                
                for i in range(0, total_rows, self.batch_size):
                    batch = df.iloc[i:i+self.batch_size]
                    batch_size = len(batch)
                    
                    try:
                        # 转换为字典列表
                        records = batch.to_dict(orient='records')
                        
                        # 插入数据
                        session.execute(table.insert(), records)
                        
                        # 提交事务
                        session.commit()
                        
                        inserted += batch_size
                        self.logger.debug(f"批次 {i//self.batch_size + 1}/{(total_rows-1)//self.batch_size + 1} 插入成功: {batch_size}行")
                        
                    except Exception as e:
                        # 回滚事务
                        session.rollback()
                        
                        failed += batch_size
                        self.logger.error(f"批次 {i//self.batch_size + 1}/{(total_rows-1)//self.batch_size + 1} 插入失败: {str(e)}")
                        
                        # 记录详细错误信息
                        self.logger.debug(f"插入失败详情: {traceback.format_exc()}")
                        
                        # 如果是完整性错误（如唯一约束冲突），尝试逐行插入
                        if isinstance(e, IntegrityError) or "duplicate" in str(e).lower() or "unique constraint" in str(e).lower():
                            self.logger.info(f"检测到完整性错误，尝试逐行插入以跳过冲突记录")
                            
                            # 重置计数
                            failed = 0
                            inserted_in_retry = 0
                            
                            # 逐行插入
                            for _, row in batch.iterrows():
                                try:
                                    record = row.to_dict()
                                    session.execute(table.insert(), [record])
                                    session.commit()
                                    inserted_in_retry += 1
                                except Exception as e2:
                                    session.rollback()
                                    failed += 1
                                    self.logger.debug(f"行插入失败: {str(e2)}")
                            
                            inserted += inserted_in_retry
                            self.logger.info(f"逐行插入结果: 成功 {inserted_in_retry}行, 失败 {failed}行")
                
                result = {
                    'success': failed == 0,
                    'inserted': inserted,
                    'failed': failed,
                    'total': total_rows
                }
                
                self.logger.info(f"数据插入完成，成功: {inserted}行, 失败: {failed}行")
                
                return result
                
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            error_msg = f"数据插入失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def update_data(self, data: pd.DataFrame, table_name: str, key_columns: List[str], metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        更新数据
        
        Args:
            data: 要更新的数据DataFrame
            table_name: 表名
            key_columns: 键列列表，用于标识要更新的记录
            metadata: 数据元信息
            
        Returns:
            Dict[str, Any]: 更新结果
            
        Raises:
            DatabaseError: 数据更新失败
        """
        if data.empty:
            self.logger.warning(f"要更新的数据为空，跳过更新")
            return {'success': True, 'updated': 0, 'failed': 0}
        
        self.logger.info(f"开始更新表 {table_name} 中的数据，数据行数: {len(data)}")
        
        try:
            # 创建会话
            session = self.Session()
            
            try:
                # 反射表结构
                try:
                    table = Table(table_name, self.metadata, autoload_with=self.engine)
                except (SQLAlchemyError, NoSuchTableError) as e:
                    # 捕获表不存在的异常
                    error_msg = str(e).lower()
                    if any(msg in error_msg for msg in ["doesn't exist", "no such table", "unknown table", "table not found", "table or view does not exist"]):
                        # 如果启用了自动建表，则创建表
                        if self.auto_create_table:
                            self.logger.info(f"表 {table_name} 不存在，将自动创建")
                            table = self._create_table_from_dataframe(data, table_name, session)
                            # 对于新建表，执行插入而不是更新
                            session.close()
                            return self.insert_data(data, table_name, metadata)
                        else:
                            self.logger.error(f"表 {table_name} 不存在，且未启用自动建表")
                            raise DatabaseError(f"表 {table_name} 不存在，且未启用自动建表")
                    else:
                        # 其他SQLAlchemy错误
                        raise DatabaseError(f"反射表结构失败: {str(e)}")
                
                # 添加更新时间
                df = data.copy()
                now = datetime.now()
                
                # 检查表是否有update_time列
                if 'update_time' in table.columns and 'update_time' not in df.columns:
                    df['update_time'] = now
                
                # 将数据分批更新
                total_rows = len(df)
                updated = 0
                failed = 0
                
                for i in range(0, total_rows, self.batch_size):
                    batch = df.iloc[i:i+self.batch_size]
                    batch_size = len(batch)
                    
                    try:
                        # 逐行更新
                        for _, row in batch.iterrows():
                            # 构建更新条件
                            conditions = and_(*[table.c[key] == row[key] for key in key_columns])
                            
                            # 构建更新值
                            values = {col: row[col] for col in row.index if col in table.columns and col not in key_columns}
                            
                            # 执行更新
                            result = session.execute(
                                table.update().
                                where(conditions).
                                values(values)
                            )
                            
                            # 检查更新结果
                            if result.rowcount > 0:
                                updated += result.rowcount
                            else:
                                failed += 1
                        
                        # 提交事务
                        session.commit()
                        
                        self.logger.debug(f"批次 {i//self.batch_size + 1}/{(total_rows-1)//self.batch_size + 1} 更新完成: 成功 {updated}行, 失败 {failed}行")
                        
                    except Exception as e:
                        # 回滚事务
                        session.rollback()
                        
                        failed += batch_size
                        self.logger.error(f"批次 {i//self.batch_size + 1}/{(total_rows-1)//self.batch_size + 1} 更新失败: {str(e)}")
                
                result = {
                    'success': failed == 0,
                    'updated': updated,
                    'failed': failed,
                    'total': total_rows
                }
                
                self.logger.info(f"数据更新完成，成功: {updated}行, 失败: {failed}行")
                
                return result
                
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            error_msg = f"数据更新失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def upsert_data(self, data: pd.DataFrame, table_name: str, key_columns: List[str], metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        插入或更新数据（upsert）
        
        Args:
            data: 要插入或更新的数据DataFrame
            table_name: 表名
            key_columns: 键列列表，用于标识记录
            metadata: 数据元信息
            
        Returns:
            Dict[str, Any]: 操作结果
            
        Raises:
            DatabaseError: 数据操作失败
        """
        if data.empty:
            self.logger.warning(f"要操作的数据为空，跳过操作")
            return {'success': True, 'inserted': 0, 'updated': 0, 'failed': 0}
        
        self.logger.info(f"开始对表 {table_name} 执行插入或更新操作，数据行数: {len(data)}")
        
        try:
            # 创建会话
            session = self.Session()
            
            try:
                # 反射表结构
                try:
                    table = Table(table_name, self.metadata, autoload_with=self.engine)
                except (SQLAlchemyError, NoSuchTableError) as e:
                    # 捕获表不存在的异常
                    error_msg = str(e).lower()
                    if any(msg in error_msg for msg in ["doesn't exist", "no such table", "unknown table", "table not found", "table or view does not exist"]):
                        # 如果启用了自动建表，则创建表
                        if self.auto_create_table:
                            self.logger.info(f"表 {table_name} 不存在，将自动创建")
                            table = self._create_table_from_dataframe(data, table_name, session)
                            # 对于新建表，执行插入
                            session.close()
                            return self.insert_data(data, table_name, metadata)
                        else:
                            self.logger.error(f"表 {table_name} 不存在，且未启用自动建表")
                            raise DatabaseError(f"表 {table_name} 不存在，且未启用自动建表")
                    else:
                        # 其他SQLAlchemy错误
                        raise DatabaseError(f"反射表结构失败: {str(e)}")
                
                # 查询已存在的数据
                try:
                    existing_data = self._query_existing_data(session, table, data, key_columns)
                    self.logger.info(f"查询到已有数据: {len(existing_data)}行")
                except Exception as e:
                    # 如果查询失败，假设没有已存在的数据
                    self.logger.warning(f"查询已有数据失败，假设没有已存在的数据: {str(e)}")
                    existing_data = pd.DataFrame(columns=data.columns)
                
                # 分离需要插入和更新的数据
                to_insert, to_update = self._split_data_for_upsert(data, existing_data, key_columns)
                
                self.logger.info(f"数据分离完成，需插入: {len(to_insert)}行, 需更新: {len(to_update)}行")
                
                # 执行插入
                insert_result = {'inserted': 0, 'failed': 0}
                if not to_insert.empty:
                    insert_result = self.insert_data(to_insert, table_name, metadata)
                
                # 执行更新
                update_result = {'updated': 0, 'failed': 0}
                if not to_update.empty:
                    update_result = self.update_data(to_update, table_name, key_columns, metadata)
                
                # 合并结果
                result = {
                    'success': insert_result.get('success', True) and update_result.get('success', True),
                    'inserted': insert_result.get('inserted', 0),
                    'updated': update_result.get('updated', 0),
                    'failed': insert_result.get('failed', 0) + update_result.get('failed', 0),
                    'total': len(data)
                }
                
                self.logger.info(f"插入或更新操作完成，插入: {result['inserted']}行, 更新: {result['updated']}行, 失败: {result['failed']}行")
                
                return result
                
            finally:
                # 关闭会话
                session.close()
                
        except Exception as e:
            error_msg = f"插入或更新操作失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def _create_table_from_dataframe(self, data: pd.DataFrame, table_name: str, session: Session) -> Table:
        """
        根据DataFrame创建表
        
        Args:
            data: 数据DataFrame
            table_name: 表名
            session: 数据库会话
            
        Returns:
            Table: 创建的表对象
            
        Raises:
            DatabaseError: 创建表失败
        """
        self.logger.info(f"开始根据DataFrame创建表 {table_name}")
        
        try:
            # 创建表对象
            table = Table(table_name, self.metadata)
            
            # 添加主键
            if self.auto_create_table_options.get('add_primary_key', True):
                table.append_column(Column('id', Integer, primary_key=True, autoincrement=True))
            
            # 添加数据列
            for column_name, dtype in data.dtypes.items():
                # 获取SQLAlchemy类型
                column_type = self._get_sqlalchemy_type(data[column_name], dtype)
                
                # 添加列
                table.append_column(Column(column_name, column_type))
            
            # 添加插入时间列
            if self.auto_create_table_options.get('add_insert_time', True):
                table.append_column(Column('insert_time', DateTime, default=func.now()))
            
            # 添加更新时间列
            if self.auto_create_table_options.get('add_update_time', True):
                table.append_column(Column('update_time', DateTime, default=func.now(), onupdate=func.now()))
            
            # 创建表
            table.create(self.engine)
            
            self.logger.info(f"表 {table_name} 创建成功")
            
            return table
            
        except Exception as e:
            error_msg = f"创建表失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def _get_sqlalchemy_type(self, series: pd.Series, dtype) -> Any:
        """
        根据pandas数据类型获取对应的SQLAlchemy类型
        
        Args:
            series: 数据列
            dtype: pandas数据类型
            
        Returns:
            Any: SQLAlchemy类型
        """
        # 处理整数类型
        if pd.api.types.is_integer_dtype(dtype):
            try:
                # 检查数值范围
                min_val = series.min()
                max_val = series.max()
                
                # 如果有NaN，min和max会返回NaN
                if pd.isna(min_val) or pd.isna(max_val):
                    # 无法确定范围，保守使用BIGINT
                    self.logger.info(f"列 {series.name} 包含NaN值，无法确定范围，使用BIGINT类型")
                    return sqlalchemy.BIGINT
                
                # 检查是否超出INT范围
                if min_val < -2147483648 or max_val > 2147483647:
                    self.logger.info(f"列 {series.name} 的值超出INT范围 (最小值: {min_val}, 最大值: {max_val})，使用BIGINT类型")
                    return sqlalchemy.BIGINT
                else:
                    return Integer
            except Exception as e:
                # 如果无法确定范围，保守使用BIGINT
                self.logger.warning(f"无法确定列 {series.name} 的范围: {str(e)}，使用BIGINT类型")
                return sqlalchemy.BIGINT
        
        # 处理浮点类型
        elif pd.api.types.is_float_dtype(dtype):
            try:
                # 检查数值范围
                series_no_na = series.dropna()
                if series_no_na.empty:
                    return Float
                
                max_abs = max(abs(series_no_na.min()), abs(series_no_na.max()))
                
                # 检查小数位数
                decimal_places = series.apply(lambda x: len(str(x).split('.')[-1]) if pd.notna(x) and '.' in str(x) else 0).max()
                
                # 如果数值非常大或精度要求高，使用Numeric
                if max_abs > 1e10 or decimal_places > 6:
                    # 确定合适的精度和小数位数
                    precision = min(max(len(str(int(max_abs))) + decimal_places, 15), 38)
                    scale = min(decimal_places, 10)
                    self.logger.info(f"列 {series.name} 需要高精度 (最大绝对值: {max_abs}, 小数位数: {decimal_places})，使用Numeric({precision},{scale})类型")
                    return Numeric(precision, scale)
                else:
                    return Float
            except Exception:
                # 如果无法确定范围，使用标准Float
                return Float
        
        # 处理日期时间类型
        elif pd.api.types.is_datetime64_dtype(dtype):
            return DateTime
        
        # 处理布尔类型
        elif pd.api.types.is_bool_dtype(dtype):
            return Boolean
        
        # 处理字符串类型
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            try:
                # 估计字符串长度
                max_length = series.astype(str).str.len().max()
                # 设置一个合理的默认长度，避免过短或过长
                length = min(max(max_length * 2, 50), 255)
                
                # 如果字符串非常长，考虑使用Text类型
                if max_length > 255:
                    self.logger.info(f"列 {series.name} 包含长文本 (最大长度: {max_length})，使用Text类型")
                    from sqlalchemy import Text
                    return Text
                else:
                    return String(length)
            except Exception:
                # 如果无法确定长度，使用默认String(255)
                return String(255)
        
        # 其他类型默认为字符串
        else:
            return String(255)
    
    def _query_existing_data(self, session, table: Table, data: pd.DataFrame, key_columns: List[str]) -> pd.DataFrame:
        """
        查询已存在的数据
        
        Args:
            session: 数据库会话
            table: 表对象
            data: 数据DataFrame
            key_columns: 键列列表
            
        Returns:
            pd.DataFrame: 已存在的数据DataFrame
        """
        # 提取键列值
        key_values = {}
        for key in key_columns:
            key_values[key] = data[key].unique().tolist()
        
        # 构建查询条件
        conditions = []
        for key, values in key_values.items():
            conditions.append(table.c[key].in_(values))
        
        # 查询数据
        query = select([table]).where(or_(*conditions))
        
        try:
            result = session.execute(query)
            
            # 转换为DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            return df
        except Exception as e:
            # 捕获所有可能的SQL执行错误
            error_str = str(e).lower()
            if any(msg in error_str for msg in ["doesn't exist", "no such table", "unknown table", "table not found", "table or view does not exist"]):
                self.logger.warning(f"查询表 {table.name} 失败，表可能不存在: {str(e)}，返回空DataFrame")
                return pd.DataFrame(columns=data.columns)
            else:
                self.logger.error(f"查询已存在数据失败: {str(e)}")
                raise
    
    def _split_data_for_upsert(self, data: pd.DataFrame, existing_data: pd.DataFrame, key_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        分离需要插入和更新的数据
        
        Args:
            data: 新数据DataFrame
            existing_data: 已存在的数据DataFrame
            key_columns: 键列列表
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (需要插入的数据, 需要更新的数据)
        """
        if existing_data.empty:
            # 如果没有已存在的数据，全部插入
            return data, pd.DataFrame(columns=data.columns)
        
        # 创建键列组合
        def create_key(row):
            return tuple(row[key] for key in key_columns)
        
        # 创建已存在数据的键集合
        existing_keys = set(create_key(row) for _, row in existing_data.iterrows())
        
        # 分离需要插入和更新的数据
        to_insert = []
        to_update = []
        
        for _, row in data.iterrows():
            key = create_key(row)
            if key in existing_keys:
                to_update.append(row)
            else:
                to_insert.append(row)
        
        # 转换为DataFrame
        to_insert_df = pd.DataFrame(to_insert, columns=data.columns) if to_insert else pd.DataFrame(columns=data.columns)
        to_update_df = pd.DataFrame(to_update, columns=data.columns) if to_update else pd.DataFrame(columns=data.columns)
        
        return to_insert_df, to_update_df
