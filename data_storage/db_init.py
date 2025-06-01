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
