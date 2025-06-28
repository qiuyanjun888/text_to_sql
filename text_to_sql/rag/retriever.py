"""
RAG文档检索模块，用于从文档库中检索相关文档，以辅助SQL生成。
"""

import os
import json
from typing import List, Dict, Optional, Tuple, Any, Union
from pathlib import Path

from langchain_core.documents import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS, Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.retrievers import BaseRetriever
from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import LLMChainExtractor

from text_to_sql.config.settings import config
from text_to_sql.models.llm import get_llm
from text_to_sql.monitoring.logger import get_logger

logger = get_logger(__name__)

class DatabaseSchemaRetriever:
    """
    数据库模式检索器，用于检索数据库表结构、关系和业务知识
    """
    
    def __init__(self, vector_store_path: Optional[Path] = None):
        """
        初始化检索器
        
        Args:
            vector_store_path: 向量数据库存储路径
        """
        self.vector_store_path = vector_store_path or config.rag.vector_store_path
        self._embeddings = None
        self._vector_store = None
        self._retriever = None
        self._compression_retriever = None
        self._example_queries = {}
        
        # 加载示例查询
        self._load_example_queries()
        
    def _load_example_queries(self) -> None:
        """加载示例查询"""
        example_queries_path = config.example_queries_path
        if example_queries_path.exists():
            try:
                with open(example_queries_path, 'r', encoding='utf-8') as f:
                    self._example_queries = json.load(f)
                logger.info(f"已加载 {len(self._example_queries)} 个示例查询")
            except Exception as e:
                logger.error(f"加载示例查询失败: {str(e)}")
        
    @property
    def embeddings(self):
        """获取嵌入模型"""
        if self._embeddings is None:
            logger.info(f"加载嵌入模型: {config.rag.embedding_model}")
            self._embeddings = HuggingFaceEmbeddings(
                model_name=config.rag.embedding_model,
                cache_folder=str(Path.home() / ".cache" / "huggingface")
            )
        return self._embeddings
        
    def _initialize_vector_store(self) -> None:
        """初始化向量存储"""
        if self._vector_store is not None:
            return
            
        if os.path.exists(os.path.join(self.vector_store_path, "index.faiss")):
            logger.info(f"从 {self.vector_store_path} 加载FAISS向量库")
            try:
                self._vector_store = FAISS.load_local(
                    folder_path=str(self.vector_store_path),
                    embeddings=self.embeddings
                )
            except Exception as e:
                logger.error(f"加载向量库失败: {str(e)}")
                self._vector_store = None
        else:
            logger.warning(f"向量库不存在: {self.vector_store_path}")
    
    @property
    def retriever(self) -> BaseRetriever:
        """获取检索器"""
        if self._retriever is None:
            self._initialize_vector_store()
            if self._vector_store is None:
                raise ValueError("向量库未初始化")
            self._retriever = self._vector_store.as_retriever(
                search_kwargs={"k": config.rag.top_k}
            )
        return self._retriever
    
    @property
    def compression_retriever(self) -> ContextualCompressionRetriever:
        """获取压缩检索器"""
        if self._compression_retriever is None:
            llm = get_llm()
            compressor = LLMChainExtractor.from_llm(llm)
            self._compression_retriever = ContextualCompressionRetriever(
                base_compressor=compressor,
                base_retriever=self.retriever
            )
        return self._compression_retriever
    
    def index_documents(self, documents_dir: Optional[Path] = None) -> None:
        """
        索引文档目录下的所有文档
        
        Args:
            documents_dir: 文档目录
        """
        documents_dir = documents_dir or config.knowledge_dir
        if not os.path.exists(documents_dir):
            logger.error(f"文档目录不存在: {documents_dir}")
            return
            
        logger.info(f"开始索引文档目录: {documents_dir}")
        
        # 收集所有文档
        docs = []
        for root, _, files in os.walk(documents_dir):
            for file in files:
                if file.endswith(('.txt', '.md', '.json', '.yaml', '.yml', '.sql')):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        # 提取相对路径作为元数据
                        rel_path = os.path.relpath(file_path, documents_dir)
                        
                        # 确定文档类型
                        doc_type = "other"
                        if "table" in rel_path.lower() or "schema" in rel_path.lower():
                            doc_type = "schema"
                        elif "relation" in rel_path.lower():
                            doc_type = "relation"
                        elif "example" in rel_path.lower() or "query" in rel_path.lower():
                            doc_type = "example_query"
                        elif "business" in rel_path.lower() or "logic" in rel_path.lower():
                            doc_type = "business_logic"
                            
                        docs.append(Document(
                            page_content=content,
                            metadata={
                                "source": rel_path,
                                "file_name": file,
                                "doc_type": doc_type
                            }
                        ))
                        logger.debug(f"已加载文档: {rel_path}")
                    except Exception as e:
                        logger.error(f"读取文档失败 {file_path}: {str(e)}")
        
        if not docs:
            logger.warning("未找到文档")
            return
            
        # 分割文档
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.rag.chunk_size,
            chunk_overlap=config.rag.chunk_overlap
        )
        
        chunks = text_splitter.split_documents(docs)
        logger.info(f"文档分割完成，共 {len(chunks)} 个块")
        
        # 创建向量存储
        os.makedirs(self.vector_store_path, exist_ok=True)
        
        self._vector_store = FAISS.from_documents(
            documents=chunks,
            embedding=self.embeddings
        )
        
        # 保存向量存储
        self._vector_store.save_local(str(self.vector_store_path))
        logger.info(f"向量库已保存到: {self.vector_store_path}")
        
        # 更新检索器
        self._retriever = self._vector_store.as_retriever(
            search_kwargs={"k": config.rag.top_k}
        )
        self._compression_retriever = None  # 重置压缩检索器
    
    def retrieve_database_schema(self, query: str) -> List[Document]:
        """
        检索与查询相关的数据库模式信息
        
        Args:
            query: 用户查询
            
        Returns:
            相关文档列表
        """
        try:
            self._initialize_vector_store()
            if self._vector_store is None:
                logger.warning("向量库未初始化，无法进行检索")
                return []
                
            logger.info(f"检索相关文档: {query}")
            return self.retriever.get_relevant_documents(query)
        except Exception as e:
            logger.error(f"检索失败: {str(e)}")
            return []
    
    def get_schema_context(self, query: str) -> str:
        """
        获取与查询相关的模式上下文
        
        Args:
            query: 用户查询
            
        Returns:
            上下文字符串
        """
        docs = self.retrieve_database_schema(query)
        
        if not docs:
            return "未找到相关的数据库模式信息。"
            
        # 组织上下文信息
        schema_docs = [d for d in docs if d.metadata.get("doc_type") == "schema"]
        relation_docs = [d for d in docs if d.metadata.get("doc_type") == "relation"]
        business_docs = [d for d in docs if d.metadata.get("doc_type") == "business_logic"]
        example_docs = [d for d in docs if d.metadata.get("doc_type") == "example_query"]
        other_docs = [d for d in docs if d.metadata.get("doc_type") == "other"]
        
        context_parts = []
        
        if schema_docs:
            context_parts.append("## 数据库表结构\n\n" + "\n\n".join([d.page_content for d in schema_docs]))
            
        if relation_docs:
            context_parts.append("## 表关联关系\n\n" + "\n\n".join([d.page_content for d in relation_docs]))
            
        if business_docs:
            context_parts.append("## 业务规则\n\n" + "\n\n".join([d.page_content for d in business_docs]))
            
        if example_docs:
            context_parts.append("## 相似查询示例\n\n" + "\n\n".join([d.page_content for d in example_docs]))
            
        if other_docs:
            context_parts.append("## 其他相关信息\n\n" + "\n\n".join([d.page_content for d in other_docs]))
            
        # 查找示例查询中的相关示例
        matching_examples = self._find_matching_examples(query)
        if matching_examples:
            examples_str = "\n\n".join([f"问题: {q}\nSQL: {sql}" for q, sql in matching_examples])
            context_parts.append("## 相关查询示例\n\n" + examples_str)
            
        return "\n\n".join(context_parts)
    
    def _find_matching_examples(self, query: str) -> List[Tuple[str, str]]:
        """
        从预定义的示例查询中找出与当前查询相关的示例
        
        Args:
            query: 用户查询
            
        Returns:
            相关示例列表，每个元素为(问题, SQL)元组
        """
        if not self._example_queries:
            return []
            
        # 如果示例数量少，可以直接使用全部示例
        if len(self._example_queries) <= 5:
            return [(q, sql) for q, sql in self._example_queries.items()]
            
        # 否则使用向量相似度检索相关示例
        try:
            query_embedding = self.embeddings.embed_query(query)
            
            example_scores = []
            for example_q, example_sql in self._example_queries.items():
                example_embedding = self.embeddings.embed_query(example_q)
                
                # 计算余弦相似度
                similarity = sum(a * b for a, b in zip(query_embedding, example_embedding))
                example_scores.append((example_q, example_sql, similarity))
                
            # 按相似度排序并返回前3个
            example_scores.sort(key=lambda x: x[2], reverse=True)
            return [(q, sql) for q, sql, _ in example_scores[:3]]
        except Exception as e:
            logger.error(f"查找匹配示例失败: {str(e)}")
            return []

# 全局检索器实例
_retriever_instance = None

def get_retriever() -> DatabaseSchemaRetriever:
    """获取全局检索器实例"""
    global _retriever_instance
    if _retriever_instance is None:
        _retriever_instance = DatabaseSchemaRetriever()
    return _retriever_instance
