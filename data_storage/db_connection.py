"""
数据库连接管理模块，负责管理数据库连接池
"""
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import yaml
import os
from typing import Dict, Any, Optional
from urllib.parse import quote_plus

from core.exceptions import DatabaseError
from core.logger import LoggerManager


class DBConnectionManager:
    """数据库连接管理器，负责创建和管理数据库连接"""
    
    _instance = None
    
    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(DBConnectionManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化数据库连接管理器"""
        if self._initialized:
            return
        
        self._engine = None
        self._session_factory = None
        self._config = None
        self._initialized = True
        self.logger = LoggerManager().get_logger("DBConnectionManager")
    
    '''
    def init_from_config(self, config_path: str) -> None:
        """
        从配置文件初始化数据库连接
        
        Args:
            config_path: 配置文件路径
            
        Raises:
            DatabaseError: 初始化数据库连接失败
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            db_config = config['database']
            self._config = db_config
            
            # 构建数据库URL
            db_url = f"{db_config['driver']}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            
            # 创建引擎
            self._engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=db_config.get('pool_size', 5),
                max_overflow=db_config.get('max_overflow', 10),
                pool_recycle=db_config.get('pool_recycle', 3600)
            )
            
            # 创建会话工厂
            self._session_factory = scoped_session(sessionmaker(bind=self._engine))
            
            self.logger.info(f"数据库连接初始化成功: {db_config['host']}:{db_config['port']}/{db_config['database']}")
            
        except Exception as e:
            error_msg = f"初始化数据库连接失败: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg, details=str(e))
    '''
    def init_from_config(self, config_path: str) -> None:
        """
        从配置文件初始化数据库连接
        
        Args:
            config_path: 配置文件路径
            
        Raises:
            DatabaseError: 初始化数据库连接失败
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            db_config = config['database']
            self._config = db_config
            
            # URL编码密码
            encoded_password = quote_plus(db_config['password'])
            
            # 构建数据库URL（添加认证插件和连接参数）
            db_url = (
                f"{db_config['driver']}://{db_config['username']}:"
                f"{encoded_password}@{db_config['host']}:"
                f"{db_config['port']}/{db_config['database']}"
                "?charset=utf8mb4"
                "&connect_timeout=5"
            )
            
            self.logger.debug(f"连接URL: {db_url.split('@')[0]}***@{db_url.split('@')[1]}")

            # 创建引擎
            self._engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=db_config.get('pool_size', 5),
                max_overflow=db_config.get('max_overflow', 10),
                pool_recycle=db_config.get('pool_recycle', 3600),
                pool_pre_ping=True  # 添加连接健康检查
            )
            
            # 创建会话工厂
            self._session_factory = scoped_session(sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False
            ))
            
            # 立即测试连接
            if self.test_connection():
                self.logger.info(f"数据库连接初始化成功: {db_config['host']}:{db_config['port']}/{db_config['database']}")
            else:
                raise DatabaseError("数据库连接测试失败")
        
        except Exception as e:
            error_msg = f"初始化数据库连接失败: {str(e)}"
            self.logger.error(error_msg)
            # 添加更详细的错误日志
            import traceback
            self.logger.debug(f"完整错误信息: {traceback.format_exc()}")
            raise DatabaseError(error_msg, details=str(e))
    

    def get_engine(self):
        """
        获取数据库引擎
        
        Returns:
            sqlalchemy.engine.Engine: 数据库引擎
            
        Raises:
            DatabaseError: 数据库连接未初始化
        """
        if not self._engine:
            raise DatabaseError("数据库连接未初始化，请先调用init_from_config方法")
        return self._engine
    
    def get_session(self):
        """
        获取数据库会话
        
        Returns:
            sqlalchemy.orm.Session: 数据库会话
            
        Raises:
            DatabaseError: 数据库连接未初始化
        """
        if not self._session_factory:
            raise DatabaseError("数据库连接未初始化，请先调用init_from_config方法")
        return self._session_factory()
    
    def close_session(self, session) -> None:
        """
        关闭数据库会话
        
        Args:
            session: 数据库会话
        """
        if session:
            session.close()
    
    def get_config(self) -> Dict[str, Any]:
        """
        获取数据库配置
        
        Returns:
            Dict[str, Any]: 数据库配置
        """
        return self._config
    
    '''
    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            if not self._engine:
                return False
                
            # 执行简单查询测试连接
            with self._engine.connect() as conn:
                conn.execute("SELECT 1")
                
            self.logger.info("数据库连接测试成功")
            return True
            
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {str(e)}")
            return False
    
    '''
    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            if not self._engine:
                self.logger.error("测试连接失败: 引擎未初始化")
                return False
                
            self.logger.debug("尝试获取数据库连接...")
            with self._engine.connect() as conn:
                self.logger.debug("成功获取连接，执行测试查询...")
                # 使用 text() 构造器包装 SQL 语句
                result = conn.execute(text("SELECT 1"))
                if result.scalar() == 1:
                    self.logger.info("数据库连接测试成功")
                    return True
            return False
            
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {str(e)}")
            # 记录完整的堆栈跟踪
            import traceback
            self.logger.debug(f"详细错误: {traceback.format_exc()}")
            return False
    