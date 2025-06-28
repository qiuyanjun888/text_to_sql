"""
数据库连接模块，负责连接数据库和执行SQL查询。
"""

import time
from typing import Dict, List, Any, Optional, Tuple, Union
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.inspection import inspect

from text_to_sql.config.settings import config
from text_to_sql.monitoring.logger import get_logger, get_sql_query_logger

logger = get_logger(__name__)

class DatabaseConnector:
    """
    数据库连接器，负责连接数据库和执行SQL查询
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        初始化数据库连接器
        
        Args:
            connection_string: 数据库连接字符串，如果为None则使用配置文件中的连接信息
        """
        self.connection_string = connection_string or config.database.connection_string
        self._engine = None
        self.sql_logger = get_sql_query_logger()
        
    @property
    def engine(self) -> Engine:
        """获取数据库引擎实例"""
        if self._engine is None:
            try:
                logger.info(f"正在连接数据库: {self.connection_string.replace(config.database.password, '****')}")
                self._engine = create_engine(self.connection_string)
                logger.info("数据库连接成功")
            except Exception as e:
                logger.error(f"数据库连接失败: {str(e)}")
                raise e
        return self._engine
        
    def execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, float]:
        """
        执行SQL查询
        
        Args:
            sql: SQL查询语句
            params: 查询参数
            
        Returns:
            (结果数据框, 执行时间)
        """
        start_time = time.time()
        logger.info(f"执行SQL查询: {sql}")
        
        try:
            with self.engine.connect() as connection:
                if params:
                    result = connection.execute(text(sql), params)
                else:
                    result = connection.execute(text(sql))
                
                # 获取结果
                columns = result.keys()
                data = result.fetchall()
                
                # 转换为DataFrame
                df = pd.DataFrame(data, columns=columns)
                
                execution_time = time.time() - start_time
                logger.info(f"查询执行完成，耗时: {execution_time:.2f}秒，返回 {len(df)} 行结果")
                
                return df, execution_time
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"查询执行失败: {str(e)}"
            logger.error(error_msg)
            
            # 记录错误
            self.sql_logger.log_query(
                user_query="",  # 在Agent中调用时会填充
                generated_sql=sql,
                execution_time=execution_time,
                error=error_msg
            )
            
            raise SQLAlchemyError(error_msg)
    
    def get_schema_info(self) -> Dict[str, Any]:
        """
        获取数据库模式信息
        
        Returns:
            数据库模式信息字典
        """
        try:
            inspector = inspect(self.engine)
            schema_info = {}
            
            # 获取所有表
            tables = inspector.get_table_names()
            schema_info["tables"] = []
            
            for table in tables:
                table_info = {
                    "name": table,
                    "columns": []
                }
                
                # 获取列信息
                columns = inspector.get_columns(table)
                for column in columns:
                    table_info["columns"].append({
                        "name": column["name"],
                        "type": str(column["type"]),
                        "nullable": column["nullable"]
                    })
                
                # 获取主键
                pk = inspector.get_pk_constraint(table)
                if pk and "constrained_columns" in pk:
                    table_info["primary_key"] = pk["constrained_columns"]
                
                # 获取外键
                fks = inspector.get_foreign_keys(table)
                if fks:
                    table_info["foreign_keys"] = []
                    for fk in fks:
                        table_info["foreign_keys"].append({
                            "constrained_columns": fk["constrained_columns"],
                            "referred_table": fk["referred_table"],
                            "referred_columns": fk["referred_columns"]
                        })
                
                schema_info["tables"].append(table_info)
            
            logger.info(f"已获取数据库模式信息: {len(tables)} 个表")
            return schema_info
        except Exception as e:
            logger.error(f"获取数据库模式信息失败: {str(e)}")
            return {"error": str(e)}
    
    def export_schema_to_file(self, file_path: str) -> bool:
        """
        将数据库模式导出到文件
        
        Args:
            file_path: 导出文件路径
            
        Returns:
            是否成功
        """
        try:
            schema_info = self.get_schema_info()
            
            if "error" in schema_info:
                logger.error(f"导出模式失败: {schema_info['error']}")
                return False
                
            with open(file_path, 'w', encoding='utf-8') as f:
                # 写入表信息
                f.write("# 数据库表结构\n\n")
                
                for table in schema_info["tables"]:
                    f.write(f"## 表: {table['name']}\n\n")
                    
                    # 写入列信息
                    f.write("| 列名 | 类型 | 可空 | 主键 |\n")
                    f.write("|------|------|------|------|\n")
                    
                    for column in table["columns"]:
                        is_pk = "是" if "primary_key" in table and column["name"] in table["primary_key"] else ""
                        nullable = "是" if column["nullable"] else "否"
                        f.write(f"| {column['name']} | {column['type']} | {nullable} | {is_pk} |\n")
                    
                    f.write("\n")
                    
                    # 写入外键信息
                    if "foreign_keys" in table and table["foreign_keys"]:
                        f.write("### 外键关系\n\n")
                        for fk in table["foreign_keys"]:
                            f.write(f"- {', '.join(fk['constrained_columns'])} -> {fk['referred_table']}({', '.join(fk['referred_columns'])})\n")
                        f.write("\n")
            
            logger.info(f"数据库模式已导出到: {file_path}")
            return True
        except Exception as e:
            logger.error(f"导出数据库模式失败: {str(e)}")
            return False
        
    def test_connection(self) -> bool:
        """
        测试数据库连接
        
        Returns:
            是否连接成功
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("数据库连接测试成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接测试失败: {str(e)}")
            return False

# 全局数据库连接器实例
_connector_instance = None

def get_db_connector() -> DatabaseConnector:
    """获取全局数据库连接器实例"""
    global _connector_instance
    if _connector_instance is None:
        _connector_instance = DatabaseConnector()
    return _connector_instance
