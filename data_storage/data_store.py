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
                - max_overflow: 连接池最大溢出
                - pool_timeout: 连接池超时时间
                - pool_recycle: 连接池回收时间
                - echo: 是否回显SQL
                - auto_create_table: 是否自动创建表
                - batch_size: 批处理大小
                - retry_count: 重试次数
                - retry_interval: 重试间隔
                - unique_constraints: 唯一约束字典，格式为 {表名: [列名列表]}
        """
        self.config = config
        self.logger = LoggerManager().get_logger("DataStore")
        
        # 创建数据库引擎
        self.engine = self._create_engine()
        
        # 创建元数据对象
        self.metadata = MetaData()
        
        # 创建会话工厂
        self.Session = sessionmaker(bind=self.engine)
        
        # 获取配置参数
        self.auto_create_table = config.get('auto_create_table', True)
        self.batch_size = config.get('batch_size', 1000)
        self.retry_count = config.get('retry_count', 3)
        self.retry_interval = config.get('retry_interval', 1)
        self.unique_constraints = config.get('unique_constraints', {})
        
        # 初始化数据库连接
        self._init_connection()
    
    def _create_engine(self) -> Engine:
        """
        创建数据库引擎
        
        Returns:
            Engine: SQLAlchemy引擎对象
            
        Raises:
            DatabaseError: 创建引擎失败
        """
        try:
            # 获取数据库URL
            if 'url' in self.config:
                db_url = self.config['url']
            else:
                driver = self.config.get('driver', 'mysql+pymysql')
                host = self.config.get('host', 'localhost')
                port = self.config.get('port', 3306)
                username = self.config.get('username', 'root')
                password = self.config.get('password', '')
                database = self.config.get('database', '')
                
                db_url = f"{driver}://{username}:{password}@{host}:{port}/{database}"
            
            # 获取连接池配置
            pool_size = self.config.get('pool_size', 5)
            max_overflow = self.config.get('max_overflow', 10)
            pool_timeout = self.config.get('pool_timeout', 30)
            pool_recycle = self.config.get('pool_recycle', 3600)
            echo = self.config.get('echo', False)
            
            # 获取其他连接参数
            connect_args = self.config.get('connect_args', {})
            
            # 创建引擎
            engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                echo=echo,
                connect_args=connect_args
            )
            
            self.logger.info(f"数据库连接初始化成功: {host}:{port}/{database}")
            
            return engine
            
        except Exception as e:
            error_msg = f"创建数据库引擎失败: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg, details=str(e))
    
    def _init_connection(self) -> None:
        """
        初始化数据库连接
        
        Raises:
            DatabaseError: 初始化连接失败
        """
        try:
            # 测试连接
            if self.test_connection():
                self.logger.info("数据库连接测试成功")
            else:
                self.logger.error("数据库连接测试失败")
                raise DatabaseError("数据库连接测试失败")
                
        except Exception as e:
            error_msg = f"初始化数据库连接失败: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg, details=str(e))
    
    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 执行简单查询
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {str(e)}")
            return False
    
    def _map_dtype_to_sqlalchemy(self, dtype) -> Any:
        """
        将pandas数据类型映射到SQLAlchemy类型
        
        Args:
            dtype: pandas数据类型
            
        Returns:
            Any: SQLAlchemy类型
        """
        dtype_str = str(dtype)
        
        if 'int' in dtype_str:
            return Integer
        elif 'float' in dtype_str:
            return Float
        elif 'datetime' in dtype_str:
            return DateTime
        elif 'bool' in dtype_str:
            return Boolean
        else:
            return String(255)

    def _compile_sql_type_for_ddl(self, col_type: Any) -> str:
        """
        将SQLAlchemy类型编译为当前数据库方言可执行的DDL类型字符串

        Args:
            col_type: SQLAlchemy类型（可能是类型类或类型实例）

        Returns:
            str: 可直接用于DDL语句的类型字符串
        """
        # _map_dtype_to_sqlalchemy 可能返回 Integer / DateTime 这类类型类
        if isinstance(col_type, type):
            col_type = col_type()

        return col_type.compile(dialect=self.engine.dialect)
    
    def _reflect_table_with_retry(self, table_name: str) -> Table:
        """
        反射表结构，带重试机制
        
        Args:
            table_name: 表名
            
        Returns:
            Table: 表对象
            
        Raises:
            NoSuchTableError: 表不存在
            DatabaseError: 反射表结构失败
        """
        # 尝试多次反射表结构
        for attempt in range(self.retry_count):
            try:
                # 尝试反射表结构
                table = Table(table_name, self.metadata, autoload_with=self.engine)
                return table
            except NoSuchTableError:
                # 表不存在，直接抛出异常
                self.logger.warning(f"表 {table_name} 不存在")
                raise
            except Exception as e:
                # 其他异常，记录日志并重试
                self.logger.warning(f"反射表 {table_name} 结构失败，尝试 {attempt+1}/{self.retry_count}: {str(e)}")
                
                # 如果是最后一次尝试，则抛出异常
                if attempt == self.retry_count - 1:
                    # 尝试使用不同的方法反射表结构
                    try:
                        # 使用inspect反射表结构
                        inspector = inspect(self.engine)
                        if table_name in inspector.get_table_names():
                            # 表存在，但反射失败，尝试手动创建Table对象
                            columns = inspector.get_columns(table_name)
                            table = Table(table_name, self.metadata)
                            for column in columns:
                                table.append_column(Column(
                                    column['name'],
                                    column['type'],
                                    primary_key=column.get('primary_key', False),
                                    nullable=column.get('nullable', True)
                                ))
                            return table
                        else:
                            # 表不存在
                            raise NoSuchTableError(table_name)
                    except Exception as e2:
                        # 再次尝试使用text直接查询表结构
                        try:
                            with self.engine.connect() as conn:
                                # 使用SHOW COLUMNS查询表结构
                                result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
                                columns = result.fetchall()
                                
                                if not columns:
                                    # 表不存在或没有列
                                    raise NoSuchTableError(table_name)
                                
                                # 手动创建Table对象
                                table = Table(table_name, self.metadata)
                                for column in columns:
                                    # column格式: (Field, Type, Null, Key, Default, Extra)
                                    name = column[0]
                                    type_str = column[1]
                                    nullable = column[2] == 'YES'
                                    is_primary = column[3] == 'PRI'
                                    
                                    # 映射MySQL类型到SQLAlchemy类型
                                    col_type = self._map_mysql_type_to_sqlalchemy(type_str)
                                    
                                    # 添加列
                                    table.append_column(Column(
                                        name,
                                        col_type,
                                        primary_key=is_primary,
                                        nullable=nullable
                                    ))
                                
                                return table
                        except Exception as e3:
                            self.logger.error(f"最终反射表结构失败: {str(e3)}")
                            raise DatabaseError(f"反射表结构失败: {table_name}", details=str(e3))
    
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
                # 首先尝试清除元数据缓存，确保获取最新的表结构
                self.metadata.clear()
                self.logger.debug(f"已清除元数据缓存，准备查询表 {table_name}")
                
                # 反射表结构（使用增强的反射方法）
                try:
                    table = self._reflect_table_with_retry(table_name)
                except NoSuchTableError:
                    # 表不存在，根据自动建表设置决定返回空DataFrame或抛出异常
                    if self.auto_create_table:
                        self.logger.info(f"表 {table_name} 不存在，自动建表功能已启用，但需要数据来推断表结构。将返回空DataFrame。")
                        return pd.DataFrame()
                    else:
                        self.logger.warning(f"表 {table_name} 不存在，且未启用自动建表")
                        raise DatabaseError(f"表 {table_name} 不存在，且未启用自动建表")
                except Exception as e:
                    # 捕获所有其他异常，尝试判断是否是表不存在或反射失败
                    error_str = str(e).lower()
                    
                    # 检查是否是表不存在错误
                    table_not_exists = any(msg in error_str for msg in [
                        "doesn't exist", "no such table", "unknown table", 
                        "table not found", "table or view does not exist",
                        "反射表结构失败"  # 捕获我们自定义的错误消息
                    ])
                    
                    if table_not_exists and self.auto_create_table:
                        self.logger.warning(f"反射表 {table_name} 结构失败，错误: {str(e)}，将返回空DataFrame")
                        return pd.DataFrame()
                    else:
                        # 其他错误或未启用自动建表，抛出异常
                        self.logger.error(f"反射表结构失败: {table_name}")
                        raise DatabaseError(f"反射表结构失败: {table_name}", details=str(e))
                
                # 构建查询 - 使用兼容SQLAlchemy新版本的语法
                query = select(table)
                
                # 添加条件
                # 添加条件
                if conditions:
                    condition_clauses = []
                    for column, value in conditions.items():
                        # 检查列是否存在于表中
                        if column not in table.columns:
                            self.logger.warning(f"列 {column} 不存在于表 {table_name} 中，跳过该条件")
                            continue

                        # 获取列类型
                        column_type = str(table.c[column].type).lower()    
                        # 检查是否是浮点数列
                        is_float_column = str(table.c[column].type).lower() in ['float', 'double', 'real', 'numeric', 'decimal']
                        # 调试信息：输出列类型
                        self.logger.info(f"列 {column} 类型: {column_type}, 是否浮点数: {is_float_column}")

                        if isinstance(value, list):
                            if is_float_column:
                                # 对浮点数列使用范围查询
                                or_conditions = []
                                for v in value:
                                    try:
                                        v_float = float(v)
                                        # 允许0.000001的误差
                                        or_conditions.append(and_(
                                            table.c[column] >= v_float - 0.000001,
                                            table.c[column] <= v_float + 0.000001
                                        ))
                                    except (ValueError, TypeError):
                                        # 处理无法转换为浮点数的情况
                                        or_conditions.append(table.c[column] == v)
                                condition_clauses.append(or_(*or_conditions))
                            else:
                                # 非浮点数列使用IN操作符
                                condition_clauses.append(table.c[column].in_(value))
                        else:
                            if is_float_column:
                                try:
                                    v_float = float(value)
                                    # 单个浮点数值使用范围查询
                                    condition_clauses.append(and_(
                                        table.c[column] >= v_float - 0.00001,
                                        table.c[column] <= v_float + 0.00001
                                    ))
                                except (ValueError, TypeError):
                                    # 处理无法转换为浮点数的情况
                                    condition_clauses.append(table.c[column] == value)
                            else:
                                # 非浮点数使用等于操作符
                                condition_clauses.append(table.c[column] == value)

                # 确保条件被正确应用
                if condition_clauses:
                    query = query.where(and_(*condition_clauses))
                    self.logger.debug(f"应用查询条件: {query}")
                else:
                    self.logger.warning(f"没有有效的查询条件，将返回所有数据")
                
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
                # 反射表结构（使用增强的反射方法）
                try:
                    # 首先尝试清除元数据缓存，确保获取最新的表结构
                    self.metadata.clear()
                    self.logger.debug(f"已清除元数据缓存，准备反射表 {table_name} 结构")
                    
                    table = self._reflect_table_with_retry(table_name)
                    self.logger.debug(f"表 {table_name} 已存在，使用现有表结构")
                except NoSuchTableError:
                    # 表不存在，根据自动建表设置决定创建表或抛出异常
                    if self.auto_create_table:
                        self.logger.info(f"表 {table_name} 不存在，将自动创建")
                        table = self._create_table_from_dataframe(data, table_name, session)
                    else:
                        self.logger.error(f"表 {table_name} 不存在，且未启用自动建表")
                        raise DatabaseError(f"表 {table_name} 不存在，且未启用自动建表")
                except Exception as e:
                    # 捕获所有其他异常，尝试判断是否是表不存在或反射失败
                    error_str = str(e).lower()
                    
                    # 检查是否是表不存在错误
                    table_not_exists = any(msg in error_str for msg in [
                        "doesn't exist", "no such table", "unknown table", 
                        "table not found", "table or view does not exist",
                        "反射表结构失败"  # 捕获我们自定义的错误消息
                    ])
                    
                    if table_not_exists and self.auto_create_table:
                        self.logger.warning(f"反射表 {table_name} 结构失败，错误: {str(e)}，将尝试创建新表")
                        try:
                            # 再次清除元数据缓存
                            self.metadata.clear()
                            # 尝试创建表
                            table = self._create_table_from_dataframe(data, table_name, session)
                        except Exception as create_error:
                            self.logger.error(f"创建表 {table_name} 失败: {str(create_error)}")
                            raise DatabaseError(f"反射表结构失败且无法创建新表: {table_name}", details=str(e))
                    else:
                        # 其他错误或未启用自动建表，抛出异常
                        self.logger.error(f"反射表结构失败: {table_name}")
                        raise DatabaseError(f"反射表结构失败: {table_name}", details=str(e))
                
                # 添加插入时间和更新时间
                df = data.copy()
                now = datetime.now()
                
                # 检查表是否有insert_time列
                if 'insert_time' in table.columns and 'insert_time' not in df.columns:
                    df['insert_time'] = now
                
                # 检查表是否有update_time列
                if 'update_time' in table.columns and 'update_time' not in df.columns:
                    df['update_time'] = now
                    
                # 检查并添加DataFrame中存在但表结构中不存在的列（自动schema演进）
                self._ensure_columns_exist(table, df, table_name, session)
                
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
                        self.logger.debug(f"成功插入 {batch_size} 行数据，总进度: {inserted}/{total_rows}")
                        
                    except Exception as e:
                        # 回滚事务
                        session.rollback()
                        
                        failed += batch_size
                        self.logger.error(f"插入数据失败，批次 {i//self.batch_size + 1}: {str(e)}")
                        
                        # 如果是唯一约束冲突，尝试逐行插入
                        error_str = str(e).lower()
                        if any(msg in error_str for msg in ["duplicate", "unique constraint", "integrity constraint", "违反唯一约束"]):
                            self.logger.warning(f"检测到唯一约束冲突，尝试逐行插入以跳过冲突行")
                            
                            # 逐行插入
                            for _, row in batch.iterrows():
                                try:
                                    # 转换为字典
                                    record = row.to_dict()
                                    
                                    # 插入数据
                                    session.execute(table.insert(), [record])
                                    
                                    # 提交事务
                                    session.commit()
                                    
                                    inserted += 1
                                    failed -= 1
                                    
                                except Exception as row_error:
                                    # 回滚事务
                                    session.rollback()
                                    
                                    # 记录错误但继续处理下一行
                                    self.logger.debug(f"插入行数据失败: {str(row_error)}")
                
                self.logger.info(f"数据插入完成，成功: {inserted} 行，失败: {failed} 行")
                
                return {
                    'success': True,
                    'operation': 'insert',
                    'table_name': table_name,
                    'inserted_count': inserted,
                    'failed_count': failed,
                    'total_count': total_rows
                }
                
            except DatabaseError:
                # 重新抛出DatabaseError
                raise
            except Exception as e:
                # 捕获所有其他异常
                error_msg = f"数据插入失败: {table_name}"
                self.logger.error(f"{error_msg}: {str(e)}")
                raise DatabaseError(error_msg, details=str(e))
                
            finally:
                # 关闭会话
                session.close()
                
        except DatabaseError:
            # 重新抛出DatabaseError
            raise
        except Exception as e:
            # 捕获所有其他异常
            error_msg = f"数据插入失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def update_data(self, data: pd.DataFrame, table_name: str, key_columns: List[str], metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        更新数据
        
        Args:
            data: 要更新的数据DataFrame
            table_name: 表名
            key_columns: 用于定位记录的键列列表
            metadata: 数据元信息
            
        Returns:
            Dict[str, Any]: 更新结果
            
        Raises:
            DatabaseError: 数据更新失败
        """
        if data.empty:
            self.logger.warning(f"要更新的数据为空，跳过更新")
            return {'success': True, 'updated': 0, 'failed': 0}
        
        if not key_columns:
            raise ValueError("更新操作必须指定键列")
        
        self.logger.info(f"开始更新表 {table_name} 中的数据，数据行数: {len(data)}")
        
        try:
            # 创建会话
            session = self.Session()
            
            try:
                # 反射表结构
                try:
                    # 首先尝试清除元数据缓存，确保获取最新的表结构
                    self.metadata.clear()
                    self.logger.debug(f"已清除元数据缓存，准备反射表 {table_name} 结构")
                    
                    table = self._reflect_table_with_retry(table_name)
                except NoSuchTableError:
                    # 表不存在，抛出异常
                    self.logger.error(f"表 {table_name} 不存在，无法更新数据")
                    raise DatabaseError(f"表 {table_name} 不存在，无法更新数据")
                except Exception as e:
                    # 捕获所有其他异常
                    self.logger.error(f"反射表结构失败: {table_name}")
                    raise DatabaseError(f"反射表结构失败: {table_name}", details=str(e))
                
                # 检查并添加DataFrame中存在但表结构中不存在的列（自动schema演进）
                self._ensure_columns_exist(table, data, table_name, session)
                
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
                    
                    # 逐行更新
                    for _, row in batch.iterrows():
                        try:
                            # 构建WHERE条件
                            where_clauses = []
                            for key in key_columns:
                                if key in row and key in table.columns:
                                    where_clauses.append(table.c[key] == row[key])
                            
                            if not where_clauses:
                                self.logger.warning(f"未找到有效的键列，跳过更新")
                                failed += 1
                                continue
                            
                            # 构建更新值
                            values = {}
                            for col in row.index:
                                if col in table.columns and col not in key_columns:
                                    values[table.c[col]] = row[col]
                            
                            if not values:
                                self.logger.warning(f"未找到需要更新的列，跳过更新")
                                failed += 1
                                continue
                            
                            # 执行更新
                            result = session.execute(
                                table.update().
                                where(and_(*where_clauses)).
                                values(values)
                            )
                            
                            # 提交事务
                            session.commit()
                            
                            # 检查更新行数
                            if result.rowcount > 0:
                                updated += 1
                            else:
                                failed += 1
                                self.logger.debug(f"未找到匹配的记录，更新失败")
                            
                        except Exception as e:
                            # 回滚事务
                            session.rollback()
                            
                            failed += 1
                            self.logger.debug(f"更新行数据失败: {str(e)}")
                    
                    self.logger.debug(f"批次 {i//self.batch_size + 1} 更新完成，总进度: {i+batch_size}/{total_rows}")
                
                self.logger.info(f"数据更新完成，成功: {updated} 行，失败: {failed} 行")
                
                return {
                    'success': True,
                    'operation': 'update',
                    'table_name': table_name,
                    'updated_count': updated,
                    'failed_count': failed,
                    'total_count': total_rows
                }
                
            except DatabaseError:
                # 重新抛出DatabaseError
                raise
            except Exception as e:
                # 捕获所有其他异常
                error_msg = f"数据更新失败: {table_name}"
                self.logger.error(f"{error_msg}: {str(e)}")
                raise DatabaseError(error_msg, details=str(e))
                
            finally:
                # 关闭会话
                session.close()
                
        except DatabaseError:
            # 重新抛出DatabaseError
            raise
        except Exception as e:
            # 捕获所有其他异常
            error_msg = f"数据更新失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def upsert_data(self, data: pd.DataFrame, table_name: str, key_columns: List[str], metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        插入或更新数据（upsert）
        
        Args:
            data: 要插入或更新的数据DataFrame
            table_name: 表名
            key_columns: 用于定位记录的键列列表
            metadata: 数据元信息
            
        Returns:
            Dict[str, Any]: 插入或更新结果
            
        Raises:
            DatabaseError: 数据插入或更新失败
        """
        if data.empty:
            self.logger.warning(f"要插入或更新的数据为空，跳过操作")
            return {'success': True, 'inserted': 0, 'updated': 0, 'failed': 0}
        
        if not key_columns:
            raise ValueError("插入或更新操作必须指定键列")
        
        self.logger.info(f"开始插入或更新表 {table_name} 中的数据，数据行数: {len(data)}")
        
        try:
            # 创建会话
            session = self.Session()
            
            try:
                # 反射表结构
                try:
                    # 首先尝试清除元数据缓存，确保获取最新的表结构
                    self.metadata.clear()
                    self.logger.debug(f"已清除元数据缓存，准备反射表 {table_name} 结构")
                    
                    table = self._reflect_table_with_retry(table_name)
                    self.logger.debug(f"表 {table_name} 已存在，使用现有表结构")
                except NoSuchTableError:
                    # 表不存在，根据自动建表设置决定创建表或抛出异常
                    if self.auto_create_table:
                        self.logger.info(f"表 {table_name} 不存在，将自动创建")
                        table = self._create_table_from_dataframe(data, table_name, session)
                    else:
                        self.logger.error(f"表 {table_name} 不存在，且未启用自动建表")
                        raise DatabaseError(f"表 {table_name} 不存在，且未启用自动建表")
                except Exception as e:
                    # 捕获所有其他异常
                    self.logger.error(f"反射表结构失败: {table_name}")
                    raise DatabaseError(f"反射表结构失败: {table_name}", details=str(e))
                
                # 检查并添加DataFrame中存在但表结构中不存在的列（自动schema演进）
                self._ensure_columns_exist(table, data, table_name, session)
                
                # 添加插入时间和更新时间
                df = data.copy()
                now = datetime.now()
                
                # 检查表是否有insert_time列
                if 'insert_time' in table.columns and 'insert_time' not in df.columns:
                    df['insert_time'] = now
                
                # 检查表是否有update_time列
                if 'update_time' in table.columns and 'update_time' not in df.columns:
                    df['update_time'] = now
                
                # 将数据分批处理
                total_rows = len(df)
                inserted = 0
                updated = 0
                failed = 0
                
                for i in range(0, total_rows, self.batch_size):
                    batch = df.iloc[i:i+self.batch_size]
                    batch_size = len(batch)
                    
                    # 逐行处理
                    for _, row in batch.iterrows():
                        try:
                            # 构建WHERE条件
                            where_clauses = []
                            for key in key_columns:
                                if key in row and key in table.columns:
                                    where_clauses.append(table.c[key] == row[key])
                            
                            if not where_clauses:
                                self.logger.warning(f"未找到有效的键列，跳过操作")
                                failed += 1
                                continue
                            
                            # 查询是否存在匹配记录
                            result = session.execute(
                                select(func.count()).select_from(table).where(and_(*where_clauses))
                            )
                            count = result.scalar()
                            
                            if count > 0:
                                # 记录存在，执行更新
                                # 构建更新值
                                values = {}
                                for col in row.index:
                                    if col in table.columns and col not in key_columns:
                                        values[table.c[col]] = row[col]
                                
                                if not values:
                                    self.logger.warning(f"未找到需要更新的列，跳过更新")
                                    failed += 1
                                    continue
                                
                                # 执行更新
                                update_result = session.execute(
                                    table.update().
                                    where(and_(*where_clauses)).
                                    values(values)
                                )
                                
                                # 提交事务
                                session.commit()
                                
                                # 检查更新行数
                                if update_result.rowcount > 0:
                                    updated += 1
                                else:
                                    failed += 1
                                    self.logger.debug(f"更新失败")
                            else:
                                # 记录不存在，执行插入
                                # 转换为字典
                                record = row.to_dict()
                                
                                # 插入数据
                                session.execute(table.insert(), [record])
                                
                                # 提交事务
                                session.commit()
                                
                                inserted += 1
                            
                        except Exception as e:
                            # 回滚事务
                            session.rollback()
                            
                            failed += 1
                            self.logger.debug(f"处理行数据失败: {str(e)}")
                    
                    self.logger.debug(f"批次 {i//self.batch_size + 1} 处理完成，总进度: {i+batch_size}/{total_rows}")
                
                self.logger.info(f"数据插入或更新完成，插入: {inserted} 行，更新: {updated} 行，失败: {failed} 行")
                
                return {
                    'success': True,
                    'operation': 'upsert',
                    'table_name': table_name,
                    'inserted_count': inserted,
                    'updated_count': updated,
                    'failed_count': failed,
                    'total_count': total_rows
                }
                
            except DatabaseError:
                # 重新抛出DatabaseError
                raise
            except Exception as e:
                # 捕获所有其他异常
                error_msg = f"数据插入或更新失败: {table_name}"
                self.logger.error(f"{error_msg}: {str(e)}")
                raise DatabaseError(error_msg, details=str(e))
                
            finally:
                # 关闭会话
                session.close()
                
        except DatabaseError:
            # 重新抛出DatabaseError
            raise
        except Exception as e:
            # 捕获所有其他异常
            error_msg = f"数据插入或更新失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def delete_data(self, table_name: str, conditions: Dict[str, Any]) -> Dict[str, Any]:
        """
        删除数据
        
        Args:
            table_name: 表名
            conditions: 删除条件，格式为 {列名: 值} 或 {列名: [值1, 值2, ...]}
            
        Returns:
            Dict[str, Any]: 删除结果
            
        Raises:
            DatabaseError: 数据删除失败
        """
        if not conditions:
            raise ValueError("删除操作必须指定条件，禁止无条件删除")
        
        self.logger.info(f"开始删除表 {table_name} 中的数据，条件: {conditions}")
        
        try:
            # 创建会话
            session = self.Session()
            
            try:
                # 反射表结构
                try:
                    # 首先尝试清除元数据缓存，确保获取最新的表结构
                    self.metadata.clear()
                    self.logger.debug(f"已清除元数据缓存，准备反射表 {table_name} 结构")
                    
                    table = self._reflect_table_with_retry(table_name)
                except NoSuchTableError:
                    # 表不存在，抛出异常
                    self.logger.error(f"表 {table_name} 不存在，无法删除数据")
                    raise DatabaseError(f"表 {table_name} 不存在，无法删除数据")
                except Exception as e:
                    # 捕获所有其他异常
                    self.logger.error(f"反射表结构失败: {table_name}")
                    raise DatabaseError(f"反射表结构失败: {table_name}", details=str(e))
                
                # 构建WHERE条件
                where_clauses = []
                for column, value in conditions.items():
                    if column not in table.columns:
                        self.logger.warning(f"列 {column} 不存在于表 {table_name} 中，跳过该条件")
                        continue
                        
                    if isinstance(value, list):
                        # 如果值是列表，使用IN操作符
                        where_clauses.append(table.c[column].in_(value))
                    else:
                        # 否则使用等于操作符
                        where_clauses.append(table.c[column] == value)
                
                if not where_clauses:
                    self.logger.error(f"未找到有效的条件列，禁止无条件删除")
                    raise DatabaseError(f"未找到有效的条件列，禁止无条件删除")
                
                # 执行删除
                result = session.execute(
                    table.delete().where(and_(*where_clauses))
                )
                
                # 提交事务
                session.commit()
                
                # 获取删除行数
                deleted = result.rowcount
                
                self.logger.info(f"数据删除完成，删除: {deleted} 行")
                
                return {
                    'success': True,
                    'operation': 'delete',
                    'table_name': table_name,
                    'deleted_count': deleted
                }
                
            except DatabaseError:
                # 重新抛出DatabaseError
                raise
            except Exception as e:
                # 捕获所有其他异常
                error_msg = f"数据删除失败: {table_name}"
                self.logger.error(f"{error_msg}: {str(e)}")
                raise DatabaseError(error_msg, details=str(e))
                
            finally:
                # 关闭会话
                session.close()
                
        except DatabaseError:
            # 重新抛出DatabaseError
            raise
        except Exception as e:
            # 捕获所有其他异常
            error_msg = f"数据删除失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def _create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, session) -> Table:
        """
        从DataFrame创建表
        
        Args:
            df: 数据DataFrame
            table_name: 表名
            session: 数据库会话
            
        Returns:
            Table: 创建的表对象
            
        Raises:
            DatabaseError: 创建表失败
        """
        try:
            self.logger.info(f"从DataFrame创建表 {table_name}")
            
            # 创建列定义
            columns = []
            
            # 添加自增主键
            columns.append(Column('id', Integer, primary_key=True))
            
            # 根据DataFrame的数据类型创建列
            for col_name, dtype in df.dtypes.items():
                # 跳过已添加的列
                if col_name == 'id':
                    continue
                
                # 根据数据类型映射到SQLAlchemy类型
                col_type = self._map_dtype_to_sqlalchemy(dtype)
                
                # 创建列
                column = Column(col_name, col_type)
                columns.append(column)
            
            # 添加时间戳列
            columns.append(Column('insert_time', DateTime))
            columns.append(Column('update_time', DateTime))
            
            # 创建表
            table = Table(table_name, self.metadata, *columns)
            table.create(self.engine)
            
            self.logger.info(f"表 {table_name} 创建成功")
            
            return table
            
        except Exception as e:
            error_msg = f"创建表失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
            
    def _ensure_columns_exist(self, table: Table, df: pd.DataFrame, table_name: str, session) -> None:
        """
        确保DataFrame中的所有列都存在于表结构中，如果不存在则自动添加
        
        Args:
            table: 表对象
            df: 数据DataFrame
            table_name: 表名
            session: 数据库会话
            
        Raises:
            DatabaseError: 添加列失败
        """
        try:
            # 获取表中已有的列名
            existing_columns = set(column.name for column in table.columns)
            
            # 获取DataFrame中的列名
            df_columns = set(df.columns)
            
            # 找出需要添加的列
            missing_columns = df_columns - existing_columns
            
            if missing_columns:
                self.logger.info(f"检测到表 {table_name} 缺少以下列: {missing_columns}，将自动添加")
                
                # 为每个缺失的列执行ALTER TABLE ADD COLUMN
                for col_name in missing_columns:
                    # 获取列的数据类型
                    dtype = df[col_name].dtype
                    col_type = self._map_dtype_to_sqlalchemy(dtype)
                    col_type_ddl = self._compile_sql_type_for_ddl(col_type)

                    preparer = self.engine.dialect.identifier_preparer
                    quoted_table_name = preparer.quote(table_name)
                    quoted_col_name = preparer.quote(col_name)
                    
                    # 构建ALTER TABLE语句
                    alter_stmt = f"ALTER TABLE {quoted_table_name} ADD COLUMN {quoted_col_name} {col_type_ddl}"
                    
                    self.logger.info(f"执行: {alter_stmt}")
                    
                    # 执行ALTER TABLE语句
                    session.execute(text(alter_stmt))
                    session.commit()
                
                # 清除元数据缓存并重新反射表结构
                self.metadata.clear()
                table = self._reflect_table_with_retry(table_name)
                
                self.logger.info(f"表 {table_name} 结构已更新，新增 {len(missing_columns)} 列")
                
        except Exception as e:
            error_msg = f"添加列失败: {table_name}"
            self.logger.error(f"{error_msg}: {str(e)}")
            raise DatabaseError(error_msg, details=str(e))
    
    def _map_mysql_type_to_sqlalchemy(self, type_str: str) -> Any:
        """
        将MySQL类型字符串映射到SQLAlchemy类型
        
        Args:
            type_str: MySQL类型字符串
            
        Returns:
            Any: SQLAlchemy类型
        """
        type_str = type_str.lower()
        
        if 'int' in type_str:
            return Integer
        elif 'float' in type_str or 'double' in type_str or 'decimal' in type_str:
            return Float
        elif 'datetime' in type_str:
            return DateTime
        elif 'date' in type_str:
            return DateTime
        elif 'time' in type_str:
            return String(20)
        elif 'char' in type_str or 'text' in type_str:
            # 提取长度
            import re
            match = re.search(r'\((\d+)\)', type_str)
            if match:
                length = int(match.group(1))
                return String(length)
            elif 'text' in type_str:
                return Text
            else:
                return String(255)
        elif 'blob' in type_str:
            return Text
        elif 'bool' in type_str:
            return Boolean
        else:
            return String(255)
