"""
日志记录模块，包含监控和记录系统运行情况的功能。
"""

import os
import logging
import time
from pathlib import Path
from typing import Dict, Optional, Union, Any
from datetime import datetime
import mlflow
import json
import uuid

from text_to_sql.config.settings import config

# 设置日志格式
LOGGER_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DIR = config.monitoring.log_path

# 确保日志目录存在
os.makedirs(LOG_DIR, exist_ok=True)

# 设置基础日志配置
logging.basicConfig(
    level=logging.INFO,
    format=LOGGER_FORMAT,
    handlers=[
        logging.StreamHandler(),  # 输出到控制台
    ]
)

def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(name)
    
    # 如果已经配置了handler，避免重复添加
    if not logger.handlers:
        # 创建文件处理器，每天一个日志文件
        log_file = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(LOGGER_FORMAT))
        logger.addHandler(file_handler)
        
    # 设置日志级别
    logger.setLevel(logging.DEBUG if config.debug else logging.INFO)
    
    return logger

# 主日志记录器
logger = get_logger("text_to_sql")

class SQLQueryLogger:
    """SQL查询日志记录器，用于跟踪和记录SQL查询的执行情况"""
    
    def __init__(self):
        """初始化SQL查询日志记录器"""
        self.logger = get_logger("sql_query")
        self.query_log_file = LOG_DIR / "sql_queries.jsonl"
        
        # 初始化MLFlow
        mlflow.set_tracking_uri(config.monitoring.mlflow_tracking_uri)
        mlflow.set_experiment(config.monitoring.experiment_name)
        
    def log_query(self, 
                 user_query: str, 
                 generated_sql: str, 
                 execution_time: float,
                 db_result: Optional[Any] = None,
                 error: Optional[str] = None,
                 feedback: Optional[int] = None
                 ) -> str:
        """
        记录SQL查询详情
        
        Args:
            user_query: 用户的原始查询
            generated_sql: 生成的SQL
            execution_time: 执行时间
            db_result: 数据库返回结果
            error: 如果有错误，记录错误信息
            feedback: 用户反馈（1-5，5表示非常满意）
            
        Returns:
            查询ID
        """
        query_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        # 记录到日志文件
        log_entry = {
            "id": query_id,
            "timestamp": timestamp,
            "user_query": user_query,
            "generated_sql": generated_sql,
            "execution_time": execution_time,
            "error": error,
            "feedback": feedback
        }
        
        # 写入JSONL文件
        with open(self.query_log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        
        # 记录到MLFlow
        with mlflow.start_run(run_name=f"query_{query_id[:8]}", nested=True):
            mlflow.log_param("user_query", user_query)
            mlflow.log_param("generated_sql", generated_sql)
            mlflow.log_metric("execution_time", execution_time)
            
            if error:
                mlflow.log_param("error", error)
            
            if feedback is not None:
                mlflow.log_metric("user_feedback", feedback)
        
        # 同时记录到常规日志
        if error:
            self.logger.error(f"查询失败 [ID: {query_id}]: {error}")
        else:
            self.logger.info(f"查询成功 [ID: {query_id}], 执行时间: {execution_time:.2f}秒")
            
        return query_id
    
    def log_feedback(self, query_id: str, feedback: int, comment: Optional[str] = None) -> None:
        """
        记录用户对查询结果的反馈
        
        Args:
            query_id: 查询ID
            feedback: 反馈分数（1-5）
            comment: 用户评论
        """
        # 更新JSONL文件中的记录
        updated_entries = []
        found = False
        
        try:
            with open(self.query_log_file, "r", encoding="utf-8") as f:
                for line in f:
                    entry = json.loads(line)
                    if entry["id"] == query_id:
                        entry["feedback"] = feedback
                        if comment:
                            entry["feedback_comment"] = comment
                        found = True
                    updated_entries.append(entry)
                    
            if found:
                with open(self.query_log_file, "w", encoding="utf-8") as f:
                    for entry in updated_entries:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                
                # 记录到MLFlow
                with mlflow.start_run(run_name=f"feedback_{query_id[:8]}", nested=True):
                    mlflow.log_metric("user_feedback", feedback)
                    if comment:
                        mlflow.log_param("feedback_comment", comment)
                
                self.logger.info(f"已记录用户反馈 [ID: {query_id}]: {feedback}/5")
            else:
                self.logger.warning(f"未找到查询记录 [ID: {query_id}]")
        except Exception as e:
            self.logger.error(f"记录反馈失败: {str(e)}")
            
class UserInteractionLogger:
    """用户交互日志记录器，记录和分析用户与系统的交互"""
    
    def __init__(self):
        """初始化用户交互日志记录器"""
        self.logger = get_logger("user_interaction")
        self.interaction_log_file = LOG_DIR / "user_interactions.jsonl"
        
    def log_interaction(self,
                      session_id: str,
                      user_input: str,
                      system_response: str,
                      duration: float,
                      metadata: Optional[Dict[str, Any]] = None
                      ) -> None:
        """
        记录用户交互
        
        Args:
            session_id: 会话ID
            user_input: 用户输入
            system_response: 系统响应
            duration: 响应耗时
            metadata: 额外元数据
        """
        timestamp = datetime.now().isoformat()
        
        # 记录到日志文件
        log_entry = {
            "timestamp": timestamp,
            "session_id": session_id,
            "user_input": user_input,
            "system_response": system_response,
            "duration": duration,
        }
        
        if metadata:
            log_entry.update(metadata)
        
        # 写入JSONL文件
        with open(self.interaction_log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            
        self.logger.info(f"用户交互 [会话ID: {session_id}], 响应时间: {duration:.2f}秒")
        
    def get_session_history(self, session_id: str) -> list:
        """
        获取指定会话的历史记录
        
        Args:
            session_id: 会话ID
            
        Returns:
            会话历史记录列表
        """
        history = []
        try:
            with open(self.interaction_log_file, "r", encoding="utf-8") as f:
                for line in f:
                    entry = json.loads(line)
                    if entry["session_id"] == session_id:
                        history.append(entry)
            return sorted(history, key=lambda x: x["timestamp"])
        except Exception as e:
            self.logger.error(f"获取会话历史失败: {str(e)}")
            return []

# 全局日志记录器实例
_sql_query_logger = None
_user_interaction_logger = None

def get_sql_query_logger() -> SQLQueryLogger:
    """获取SQL查询日志记录器实例"""
    global _sql_query_logger
    if _sql_query_logger is None:
        _sql_query_logger = SQLQueryLogger()
    return _sql_query_logger

def get_user_interaction_logger() -> UserInteractionLogger:
    """获取用户交互日志记录器实例"""
    global _user_interaction_logger
    if _user_interaction_logger is None:
        _user_interaction_logger = UserInteractionLogger()
    return _user_interaction_logger
