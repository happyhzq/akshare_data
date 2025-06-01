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
