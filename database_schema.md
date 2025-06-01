# 数据库模式设计

## 概述

本文档定义了AkShare数据自动获取与更新项目的数据库模式设计。数据库采用MySQL，通过SQLAlchemy ORM进行对象关系映射。设计目标是支持多种数据类型的存储、防止重复插入、记录数据插入时间，并保持良好的查询性能。

## 数据库设计原则

1. **模块化设计**：每类数据对应独立的表，便于扩展和维护
2. **统一元数据**：所有表包含统一的元数据字段（如插入时间、数据来源等）
3. **防重复机制**：通过唯一索引和业务键组合防止重复数据
4. **时间索引**：所有表都包含插入时间索引，支持时间范围查询
5. **数据溯源**：记录数据来源和版本信息，支持数据溯源

## 数据库表结构

### 1. 元数据表 (metadata)

用于记录数据库版本、更新历史等信息。

```sql
CREATE TABLE metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(50) NOT NULL,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. 数据源表 (data_sources)

记录所有数据来源的信息。

```sql
CREATE TABLE data_sources (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,  -- 'akshare', 'web', 'api', etc.
    description TEXT,
    config JSON,  -- 存储数据源的配置信息
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY (name, type)
);
```

### 3. 数据接口表 (data_interfaces)

记录所有数据接口的信息，与数据源表关联。

```sql
CREATE TABLE data_interfaces (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    function_name VARCHAR(100) NOT NULL,  -- AkShare函数名
    description TEXT,
    parameters JSON,  -- 接口参数定义
    return_fields JSON,  -- 返回字段定义
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES data_sources(id),
    UNIQUE KEY (source_id, name)
);
```

### 4. 数据表基本结构

每类数据都有独立的表，但共享以下基本结构：

```sql
CREATE TABLE {data_category}_{data_name} (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    interface_id INT NOT NULL,
    fetch_time TIMESTAMP NOT NULL,  -- 数据获取时间
    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 数据插入时间
    data_date DATE,  -- 数据日期（如适用）
    {specific_fields...},  -- 特定于该数据类型的字段
    raw_data JSON,  -- 原始数据的JSON存储（可选）
    FOREIGN KEY (interface_id) REFERENCES data_interfaces(id),
    INDEX (insert_time),
    INDEX (data_date),
    UNIQUE KEY {unique_constraint}  -- 特定于该数据类型的唯一约束
);
```

### 5. 具体数据表示例

#### 5.1 宏观经济数据表 (macro_economic_indicators)

```sql
CREATE TABLE macro_economic_indicators (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    interface_id INT NOT NULL,
    fetch_time TIMESTAMP NOT NULL,
    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_date DATE NOT NULL,
    indicator_name VARCHAR(100) NOT NULL,
    indicator_value DECIMAL(20,6),
    country VARCHAR(50) NOT NULL,
    frequency VARCHAR(20) NOT NULL,  -- 'daily', 'monthly', 'quarterly', 'yearly'
    unit VARCHAR(50),
    FOREIGN KEY (interface_id) REFERENCES data_interfaces(id),
    INDEX (insert_time),
    INDEX (data_date),
    INDEX (country, indicator_name),
    UNIQUE KEY (data_date, indicator_name, country, frequency)
);
```

#### 5.2 股票行情数据表 (stock_market_data)

```sql
CREATE TABLE stock_market_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    interface_id INT NOT NULL,
    fetch_time TIMESTAMP NOT NULL,
    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    trade_date DATE NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    open_price DECIMAL(20,6),
    high_price DECIMAL(20,6),
    low_price DECIMAL(20,6),
    close_price DECIMAL(20,6),
    volume BIGINT,
    amount DECIMAL(20,6),
    change_percent DECIMAL(10,4),
    exchange VARCHAR(50),
    FOREIGN KEY (interface_id) REFERENCES data_interfaces(id),
    INDEX (insert_time),
    INDEX (trade_date),
    INDEX (stock_code),
    UNIQUE KEY (trade_date, stock_code)
);
```

#### 5.3 公司基本信息表 (company_basic_info)

```sql
CREATE TABLE company_basic_info (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    interface_id INT NOT NULL,
    fetch_time TIMESTAMP NOT NULL,
    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_date DATE NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    company_name VARCHAR(100) NOT NULL,
    industry VARCHAR(100),
    sector VARCHAR(100),
    listing_date DATE,
    registered_capital DECIMAL(20,6),
    registered_address TEXT,
    business_scope TEXT,
    FOREIGN KEY (interface_id) REFERENCES data_interfaces(id),
    INDEX (insert_time),
    INDEX (update_date),
    INDEX (stock_code),
    UNIQUE KEY (update_date, stock_code)
);
```

#### 5.4 外汇汇率数据表 (forex_exchange_rates)

```sql
CREATE TABLE forex_exchange_rates (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    interface_id INT NOT NULL,
    fetch_time TIMESTAMP NOT NULL,
    insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rate_date DATE NOT NULL,
    rate_time TIME,
    from_currency VARCHAR(10) NOT NULL,
    to_currency VARCHAR(10) NOT NULL,
    exchange_rate DECIMAL(20,6) NOT NULL,
    bid_rate DECIMAL(20,6),
    ask_rate DECIMAL(20,6),
    source VARCHAR(50),
    FOREIGN KEY (interface_id) REFERENCES data_interfaces(id),
    INDEX (insert_time),
    INDEX (rate_date),
    INDEX (from_currency, to_currency),
    UNIQUE KEY (rate_date, rate_time, from_currency, to_currency, source)
);
```

## SQLAlchemy ORM模型

以下是对应的SQLAlchemy ORM模型定义，将保存在`data_storage/models.py`文件中：

```python
from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Boolean, Text, ForeignKey, BigInteger, Numeric, Time, JSON, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class Metadata(Base):
    __tablename__ = 'metadata'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(50), nullable=False)
    description = Column(Text)
    updated_at = Column(DateTime, default=func.now())

class DataSource(Base):
    __tablename__ = 'data_sources'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    type = Column(String(50), nullable=False)
    description = Column(Text)
    config = Column(JSON)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (UniqueConstraint('name', 'type', name='uix_data_source'),)

class DataInterface(Base):
    __tablename__ = 'data_interfaces'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey('data_sources.id'), nullable=False)
    name = Column(String(100), nullable=False)
    function_name = Column(String(100), nullable=False)
    description = Column(Text)
    parameters = Column(JSON)
    return_fields = Column(JSON)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (UniqueConstraint('source_id', 'name', name='uix_data_interface'),)

class MacroEconomicIndicator(Base):
    __tablename__ = 'macro_economic_indicators'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    interface_id = Column(Integer, ForeignKey('data_interfaces.id'), nullable=False)
    fetch_time = Column(DateTime, nullable=False)
    insert_time = Column(DateTime, default=func.now())
    data_date = Column(Date, nullable=False)
    indicator_name = Column(String(100), nullable=False)
    indicator_value = Column(Numeric(20, 6))
    country = Column(String(50), nullable=False)
    frequency = Column(String(20), nullable=False)
    unit = Column(String(50))
    
    __table_args__ = (
        Index('ix_macro_insert_time', 'insert_time'),
        Index('ix_macro_data_date', 'data_date'),
        Index('ix_macro_country_indicator', 'country', 'indicator_name'),
        UniqueConstraint('data_date', 'indicator_name', 'country', 'frequency', name='uix_macro_data')
    )

class StockMarketData(Base):
    __tablename__ = 'stock_market_data'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    interface_id = Column(Integer, ForeignKey('data_interfaces.id'), nullable=False)
    fetch_time = Column(DateTime, nullable=False)
    insert_time = Column(DateTime, default=func.now())
    trade_date = Column(Date, nullable=False)
    stock_code = Column(String(20), nullable=False)
    stock_name = Column(String(100))
    open_price = Column(Numeric(20, 6))
    high_price = Column(Numeric(20, 6))
    low_price = Column(Numeric(20, 6))
    close_price = Column(Numeric(20, 6))
    volume = Column(BigInteger)
    amount = Column(Numeric(20, 6))
    change_percent = Column(Numeric(10, 4))
    exchange = Column(String(50))
    
    __table_args__ = (
        Index('ix_stock_insert_time', 'insert_time'),
        Index('ix_stock_trade_date', 'trade_date'),
        Index('ix_stock_code', 'stock_code'),
        UniqueConstraint('trade_date', 'stock_code', name='uix_stock_data')
    )

class CompanyBasicInfo(Base):
    __tablename__ = 'company_basic_info'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    interface_id = Column(Integer, ForeignKey('data_interfaces.id'), nullable=False)
    fetch_time = Column(DateTime, nullable=False)
    insert_time = Column(DateTime, default=func.now())
    update_date = Column(Date, nullable=False)
    stock_code = Column(String(20), nullable=False)
    company_name = Column(String(100), nullable=False)
    industry = Column(String(100))
    sector = Column(String(100))
    listing_date = Column(Date)
    registered_capital = Column(Numeric(20, 6))
    registered_address = Column(Text)
    business_scope = Column(Text)
    
    __table_args__ = (
        Index('ix_company_insert_time', 'insert_time'),
        Index('ix_company_update_date', 'update_date'),
        Index('ix_company_stock_code', 'stock_code'),
        UniqueConstraint('update_date', 'stock_code', name='uix_company_data')
    )

class ForexExchangeRate(Base):
    __tablename__ = 'forex_exchange_rates'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    interface_id = Column(Integer, ForeignKey('data_interfaces.id'), nullable=False)
    fetch_time = Column(DateTime, nullable=False)
    insert_time = Column(DateTime, default=func.now())
    rate_date = Column(Date, nullable=False)
    rate_time = Column(Time)
    from_currency = Column(String(10), nullable=False)
    to_currency = Column(String(10), nullable=False)
    exchange_rate = Column(Numeric(20, 6), nullable=False)
    bid_rate = Column(Numeric(20, 6))
    ask_rate = Column(Numeric(20, 6))
    source = Column(String(50))
    
    __table_args__ = (
        Index('ix_forex_insert_time', 'insert_time'),
        Index('ix_forex_rate_date', 'rate_date'),
        Index('ix_forex_currency_pair', 'from_currency', 'to_currency'),
        UniqueConstraint('rate_date', 'rate_time', 'from_currency', 'to_currency', 'source', name='uix_forex_data')
    )
```

## 数据库初始化脚本

以下是数据库初始化脚本，将保存在`data_storage/db_init.py`文件中：

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Metadata, DataSource

def init_database(db_url):
    """初始化数据库，创建表结构并插入基础数据"""
    # 创建数据库引擎
    engine = create_engine(db_url)
    
    # 创建所有表
    Base.metadata.create_all(engine)
    
    # 创建会话
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 检查元数据表是否已有记录
        metadata_count = session.query(Metadata).count()
        if metadata_count == 0:
            # 插入初始元数据
            metadata = Metadata(
                version='1.0.0',
                description='Initial database schema for AkShare data pipeline'
            )
            session.add(metadata)
        
        # 检查数据源表是否已有AkShare记录
        akshare_source = session.query(DataSource).filter_by(name='AkShare').first()
        if not akshare_source:
            # 插入AkShare数据源
            akshare_source = DataSource(
                name='AkShare',
                type='api',
                description='AkShare金融数据接口',
                config={'version': '1.0.0'},
                is_active=True
            )
            session.add(akshare_source)
        
        # 提交事务
        session.commit()
        print("数据库初始化成功")
        
    except Exception as e:
        session.rollback()
        print(f"数据库初始化失败: {str(e)}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    # 数据库连接URL，格式：mysql+pymysql://username:password@host:port/database
    db_url = "mysql+pymysql://username:password@localhost:3306/akshare_data"
    init_database(db_url)
```

## 数据库配置

数据库配置将保存在`config.yaml`文件中：

```yaml
database:
  driver: mysql+pymysql
  host: localhost
  port: 3306
  username: username
  password: password
  database: akshare_data
  pool_size: 10
  max_overflow: 20
  pool_recycle: 3600
```

## 数据库连接管理

数据库连接管理代码将保存在`data_storage/db_connection.py`文件中：

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import yaml
import os

class DBConnectionManager:
    """数据库连接管理器，负责创建和管理数据库连接"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBConnectionManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._engine = None
        self._session_factory = None
        self._config = None
        self._initialized = True
    
    def init_from_config(self, config_path):
        """从配置文件初始化数据库连接"""
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
    
    def get_engine(self):
        """获取数据库引擎"""
        if not self._engine:
            raise RuntimeError("Database connection not initialized. Call init_from_config first.")
        return self._engine
    
    def get_session(self):
        """获取数据库会话"""
        if not self._session_factory:
            raise RuntimeError("Database connection not initialized. Call init_from_config first.")
        return self._session_factory()
    
    def close_session(self, session):
        """关闭数据库会话"""
        if session:
            session.close()
    
    def get_config(self):
        """获取数据库配置"""
        return self._config
```

## 总结

本数据库模式设计提供了一个灵活、可扩展的结构，用于存储从AkShare获取的各类投资数据。主要特点包括：

1. **模块化表结构**：每类数据有独立的表，便于扩展
2. **元数据管理**：通过元数据表和数据源表管理数据来源和版本
3. **防重复机制**：每个表都有唯一约束，防止重复插入
4. **时间索引**：所有表都包含插入时间索引，支持按时间查询
5. **ORM映射**：使用SQLAlchemy提供对象关系映射，简化数据操作

后续可以根据实际需求扩展更多的数据表，或者调整现有表的结构。
