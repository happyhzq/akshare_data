"""
任务定义模块，定义各类数据获取和处理任务
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime

from core.exceptions import TaskError
from core.logger import LoggerManager


class BaseTask(ABC):
    """任务基类，定义任务的通用接口"""
    
    def __init__(self, task_id: str, config: Dict[str, Any]):
        """
        初始化任务
        
        Args:
            task_id: 任务ID
            config: 配置信息
        """
        self.task_id = task_id
        self.config = config
        self.name = self.__class__.__name__
        self.logger = LoggerManager().get_logger(f"Task_{self.name}")
        self.start_time = None
        self.end_time = None
        self.status = "initialized"
        self.result = None
        self.error = None
    
    @abstractmethod
    def execute(self) -> Dict[str, Any]:
        """
        执行任务的抽象方法
        
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        pass
    
    def run(self) -> Dict[str, Any]:
        """
        运行任务，包含任务执行前后的处理
        
        Returns:
            Dict[str, Any]: 任务运行结果
        """
        self.logger.info(f"开始执行任务: {self.task_id}")
        self.start_time = datetime.now()
        self.status = "running"
        
        try:
            # 执行任务
            self.result = self.execute()
            self.status = "completed"
            self.logger.info(f"任务执行成功: {self.task_id}")
            
        except Exception as e:
            self.status = "failed"
            self.error = str(e)
            self.logger.error(f"任务执行失败: {self.task_id}, 错误: {str(e)}")
            raise TaskError(f"任务执行失败: {self.task_id}", details=str(e))
            
        finally:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            self.logger.info(f"任务执行耗时: {duration:.2f}秒")
        
        return self.get_task_info()
    
    def get_task_info(self) -> Dict[str, Any]:
        """
        获取任务信息
        
        Returns:
            Dict[str, Any]: 任务信息字典
        """
        return {
            'task_id': self.task_id,
            'name': self.name,
            'status': self.status,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': (self.end_time - self.start_time).total_seconds() if self.end_time else None,
            'result': self.result,
            'error': self.error
        }


class DataFetchTask(BaseTask):
    """数据获取任务，负责从数据源获取数据"""
    
    def __init__(self, task_id: str, config: Dict[str, Any]):
        """
        初始化数据获取任务
        
        Args:
            task_id: 任务ID
            config: 配置信息，必须包含以下字段：
                - fetcher_type: 获取器类型
                - fetcher_config: 获取器配置
                - interface_name: 接口名称
                - interface_params: 接口参数
        """
        super().__init__(task_id, config)
        
        # 验证必要配置
        required_fields = ['fetcher_type', 'fetcher_config', 'interface_name']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"配置缺少必要字段: {field}")
        
        self.fetcher_type = config['fetcher_type']
        self.fetcher_config = config['fetcher_config']
        self.interface_name = config['interface_name']
        self.interface_params = config.get('interface_params', {})
    
    def execute(self) -> Dict[str, Any]:
        """
        执行数据获取任务
        
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        from data_fetchers.fetcher_factory import FetcherFactory
        
        self.logger.info(f"开始获取数据: {self.interface_name}")
        
        # 创建数据获取器
        fetcher = FetcherFactory.create_fetcher(self.fetcher_type, self.fetcher_config)
        
        # 获取数据
        result = fetcher.fetch(self.interface_name, **self.interface_params)
        
        self.logger.info(f"数据获取成功: {self.interface_name}, 行数: {len(result['data'])}")
        
        return result


class DataProcessTask(BaseTask):
    """数据处理任务，负责清洗和转换数据"""
    
    def __init__(self, task_id: str, config: Dict[str, Any]):
        """
        初始化数据处理任务
        
        Args:
            task_id: 任务ID
            config: 配置信息，必须包含以下字段：
                - cleaner_type: 清洗器类型
                - cleaner_config: 清洗器配置
                - transformer_config: 转换器配置
                - input_data: 输入数据
                - input_metadata: 输入元数据
                - enable_transformer: 是否启用转换器（可选，默认为True）
        """
        super().__init__(task_id, config)
        
        # 验证必要配置
        required_fields = ['cleaner_type', 'cleaner_config', 'transformer_config', 'input_data', 'input_metadata']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"配置缺少必要字段: {field}")
        
        self.cleaner_type = config['cleaner_type']
        self.cleaner_config = config['cleaner_config']
        self.transformer_config = config['transformer_config']
        self.input_data = config['input_data']
        self.input_metadata = config['input_metadata']
        self.enable_transformer = config.get('enable_transformer', True)
    
    def execute(self) -> Dict[str, Any]:
        """
        执行数据处理任务
        
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        from data_processors.data_cleaner import DataCleanerFactory
        from data_processors.data_transformer import DataTransformer
        
        self.logger.info(f"开始处理数据，输入数据行数: {len(self.input_data)}")
        
        # 创建数据清洗器
        cleaner = DataCleanerFactory.create_cleaner(self.cleaner_type, self.cleaner_config)
        
        # 清洗数据
        cleaned_data = cleaner.clean(self.input_data, self.input_metadata)
        
        self.logger.info(f"数据清洗完成，清洗后数据行数: {len(cleaned_data)}")
        
        # 根据配置决定是否执行转换
        if self.enable_transformer:
            # 创建数据转换器
            transformer = DataTransformer(self.transformer_config)
            
            # 转换数据
            transformed = transformer.transform(cleaned_data, self.input_metadata)
            
            self.logger.info(f"数据转换完成，目标表: {transformed['table_name']}, 转换后数据行数: {len(transformed['table_data'])}")
            
            return transformed
        else:
            # 跳过转换，直接使用清洗后的数据
            self.logger.info("数据转换已禁用，将直接使用清洗后的数据")
            
            # 获取接口名称作为默认表名
            interface_name = self.input_metadata.get('interface', 'data')
            table_name = f"raw_{interface_name}"
            
            # 更新元数据
            metadata = self.input_metadata.copy()
            metadata['table_name'] = table_name
            metadata['row_count_after_transform'] = len(cleaned_data)
            metadata['transform_time'] = datetime.now()
            metadata['transform_skipped'] = True
            
            return {
                'table_data': cleaned_data,
                'table_name': table_name,
                'metadata': metadata
            }


class DataCompareTask(BaseTask):
    """数据比对任务，负责比对新数据与已有数据"""
    
    def __init__(self, task_id: str, config: Dict[str, Any]):
        """
        初始化数据比对任务
        
        Args:
            task_id: 任务ID
            config: 配置信息，必须包含以下字段：
                - comparator_config: 比对器配置
                - new_data: 新数据
                - table_name: 表名
                - db_config: 数据库配置
        """
        super().__init__(task_id, config)
        
        # 验证必要配置
        required_fields = ['comparator_config', 'new_data', 'table_name', 'db_config']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"配置缺少必要字段: {field}")
        
        self.comparator_config = config['comparator_config']
        self.new_data = config['new_data']
        self.table_name = config['table_name']
        self.db_config = config['db_config']
        self.key_columns = config.get('key_columns', [])
    
    def execute(self) -> Dict[str, Any]:
        """
        执行数据比对任务
        
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        from data_processors.data_comparator import DataComparator
        from data_storage.data_store import DataStore
        import pandas as pd
        from core.exceptions import DatabaseError
        
        self.logger.info(f"开始比对数据，表: {self.table_name}, 新数据行数: {len(self.new_data)}")
        
        # 创建数据存储器
        data_store = DataStore(self.db_config)
        
        # 获取已有数据
        if not self.key_columns:
            # 如果未指定键列，则使用比对器的默认键列
            comparator = DataComparator(self.comparator_config)
            self.key_columns = comparator._get_key_columns(self.table_name, self.new_data)
        
        # 构建查询条件
        conditions = {}
        for key in self.key_columns:
            if key in self.new_data.columns:
                conditions[key] = self.new_data[key].unique().tolist()
        
        # 查询已有数据，处理表不存在的情况
        try:
            existing_data = data_store.query_data(self.table_name, conditions)
            self.logger.info(f"查询到已有数据: {len(existing_data)}行")
        except DatabaseError as e:
            # 如果是表不存在错误，则使用空DataFrame
            if "表不存在" in str(e):
                self.logger.warning(f"表 {self.table_name} 不存在，这可能是首次运行。将使用空DataFrame进行比对。")
                existing_data = pd.DataFrame(columns=self.new_data.columns)
            else:
                # 其他数据库错误，继续抛出
                raise
        
        # 创建数据比对器
        comparator = DataComparator(self.comparator_config)
        
        # 比对数据
        result = comparator.compare(self.new_data, existing_data, self.table_name)
        
        self.logger.info(f"数据比对完成，需插入: {len(result['to_insert'])}行, 需更新: {len(result['to_update'])}行, 无需变更: {len(result['unchanged'])}行")
        
        return result


class DataStoreTask(BaseTask):
    """数据存储任务，负责将数据存入数据库"""
    
    def __init__(self, task_id: str, config: Dict[str, Any]):
        """
        初始化数据存储任务
        
        Args:
            task_id: 任务ID
            config: 配置信息，必须包含以下字段：
                - store_config: 存储器配置
                - data: 要存储的数据
                - table_name: 表名
                - metadata: 数据元信息
                - operation: 操作类型，可选值: 'insert', 'update', 'upsert'
        """
        super().__init__(task_id, config)
        
        # 验证必要配置
        required_fields = ['store_config', 'data', 'table_name', 'metadata']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"配置缺少必要字段: {field}")
        
        self.store_config = config['store_config']
        self.data = config['data']
        self.table_name = config['table_name']
        self.metadata = config['metadata']
        self.operation = config.get('operation', 'insert')
        self.key_columns = config.get('key_columns', [])
    
    def execute(self) -> Dict[str, Any]:
        """
        执行数据存储任务
        
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        from data_storage.data_store import DataStore
        
        self.logger.info(f"开始存储数据，表: {self.table_name}, 操作: {self.operation}, 数据行数: {len(self.data)}")
        
        # 创建数据存储器
        data_store = DataStore(self.store_config)
        
        # 根据操作类型执行相应的存储操作
        if self.operation == 'insert':
            result = data_store.insert_data(self.data, self.table_name, self.metadata)
            
        elif self.operation == 'update':
            if not self.key_columns:
                raise ValueError("更新操作必须指定键列")
            result = data_store.update_data(self.data, self.table_name, self.key_columns, self.metadata)
            
        elif self.operation == 'upsert':
            if not self.key_columns:
                raise ValueError("插入或更新操作必须指定键列")
            result = data_store.upsert_data(self.data, self.table_name, self.key_columns, self.metadata)
            
        else:
            raise ValueError(f"不支持的操作类型: {self.operation}")
        
        self.logger.info(f"数据存储完成，表: {self.table_name}, 操作: {self.operation}, 结果: {result}")
        
        return result
