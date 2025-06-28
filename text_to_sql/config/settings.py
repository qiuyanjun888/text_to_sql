"""
项目配置文件，包含LLM、数据库、RAG等全局配置。
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field

# 项目根目录
ROOT_DIR = Path(__file__).parent.parent.parent.absolute()

# 环境变量
def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """获取环境变量，如果不存在则返回默认值"""
    return os.environ.get(key, default)

# 模型配置
class ModelConfig(BaseModel):
    """LLM模型配置"""
    model_name: str = Field(default="Qwen/Qwen3-8B-AWQ")
    tensor_parallel_size: int = Field(default=1)
    max_tokens: int = Field(default=4096)
    temperature: float = Field(default=0.01)
    top_p: float = Field(default=0.9)
    gpu_memory_utilization: float = Field(default=0.9)
    max_model_len: int = Field(default=8192)
    max_num_seqs: int = Field(default=256)

# 数据库配置
class DatabaseConfig(BaseModel):
    """数据库连接配置"""
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    username: str = Field(default="postgres")
    password: str = Field(default="password")
    database: str = Field(default="text2sql")
    dialect: str = Field(default="postgresql")
    driver: str = Field(default="psycopg2")
    
    @property
    def connection_string(self) -> str:
        """构建数据库连接字符串"""
        return f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

# RAG配置
class RAGConfig(BaseModel):
    """RAG检索配置"""
    embedding_model: str = Field(default="sentence-transformers/all-MiniLM-L6-v2")
    vector_store_path: Path = Field(default=ROOT_DIR / "knowledge" / "vector_store")
    chunk_size: int = Field(default=512)
    chunk_overlap: int = Field(default=50)
    top_k: int = Field(default=5)

# 监控配置
class MonitoringConfig(BaseModel):
    """监控系统配置"""
    log_path: Path = Field(default=ROOT_DIR / "logs")
    mlflow_tracking_uri: str = Field(default="sqlite:///mlruns.db")
    experiment_name: str = Field(default="text2sql")
    
# UI配置
class UIConfig(BaseModel):
    """用户界面配置"""
    title: str = Field(default="文本转SQL智能体系统")
    description: str = Field(default="输入自然语言问题，智能体将为您生成SQL查询并执行")
    theme: str = Field(default="default")
    server_port: int = Field(default=7860)
    server_name: str = Field(default="0.0.0.0")
    
# 全局配置
class Config(BaseModel):
    """全局配置集合"""
    model: ModelConfig = Field(default_factory=ModelConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    rag: RAGConfig = Field(default_factory=RAGConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    
    debug: bool = Field(default=False)
    knowledge_dir: Path = Field(default=ROOT_DIR / "knowledge" / "documents")
    example_queries_path: Path = Field(default=ROOT_DIR / "knowledge" / "example_queries.json")
    
    # 加载环境变量覆盖默认配置
    def load_from_env(self) -> "Config":
        """从环境变量加载配置"""
        # 数据库配置
        if db_host := get_env("DB_HOST"):
            self.database.host = db_host
        if db_port := get_env("DB_PORT"):
            self.database.port = int(db_port)
        if db_user := get_env("DB_USER"):
            self.database.username = db_user
        if db_pass := get_env("DB_PASS"):
            self.database.password = db_pass
        if db_name := get_env("DB_NAME"):
            self.database.database = db_name
        
        # 模型配置
        if model_name := get_env("MODEL_NAME"):
            self.model.model_name = model_name
        if model_tp_size := get_env("MODEL_TP_SIZE"):
            self.model.tensor_parallel_size = int(model_tp_size)
        if gpu_mem_util := get_env("GPU_MEMORY_UTILIZATION"):
            self.model.gpu_memory_utilization = float(gpu_mem_util)
        if max_num_seqs := get_env("MAX_NUM_SEQS"):
            self.model.max_num_seqs = int(max_num_seqs)
        
        # 调试模式
        if debug := get_env("DEBUG"):
            self.debug = debug.lower() in ("true", "1", "yes")
            
        return self

# 全局配置实例
config = Config().load_from_env()

# 创建必要的目录
def ensure_directories():
    """确保配置中的目录存在"""
    os.makedirs(config.knowledge_dir, exist_ok=True)
    os.makedirs(config.rag.vector_store_path, exist_ok=True)
    os.makedirs(config.monitoring.log_path, exist_ok=True)

ensure_directories()
