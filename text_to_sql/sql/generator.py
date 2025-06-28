"""
SQL生成模块，负责将用户自然语言转换为SQL。
"""

import re
import time
from typing import Dict, Optional, Tuple, List, Any, Union

from langchain.prompts import PromptTemplate
from langchain.output_parsers import RegexParser
from langchain_core.pydantic_v1 import BaseModel, Field

from text_to_sql.config.settings import config
from text_to_sql.models.llm import get_llm
from text_to_sql.rag.retriever import get_retriever
from text_to_sql.monitoring.logger import get_logger, get_sql_query_logger

logger = get_logger(__name__)

# SQL生成的Prompt模板
SQL_GENERATION_PROMPT = """你是一个数据库专家，负责将自然语言问题转换为精确的SQL查询语句。
请仔细分析用户的问题，并基于提供的数据库结构生成最合适的SQL查询。

### 数据库相关信息:
{schema_context}

### 用户查询:
{user_query}

### 安全规则:
1. 只生成SELECT查询，绝不生成INSERT、UPDATE、DELETE、DROP、CREATE等修改数据或结构的SQL
2. 避免使用子查询和复杂函数，尽量使用JOIN和简单聚合函数
3. 限制返回行数（使用LIMIT），避免返回过多数据
4. 避免全表扫描操作，确保使用适当的索引列
5. 所有的查询应当有良好的格式和缩进，便于阅读

### 你的回答:
请仔细思考后，提供格式良好的SQL查询。如果有多种可能的解释，请选择最可能的一种。
确保SQL语法正确且与用户查询需求匹配。
你需要在最终SQL前解释你的思考过程。

思考过程: 

SQL: 
```sql
"""

# SQL验证的Prompt模板
SQL_VALIDATION_PROMPT = """你是一个数据库安全专家，负责验证SQL查询的安全性和效率。
请仔细检查下面的SQL查询，并评估是否存在安全或性能问题。

### 要验证的SQL:
```sql
{sql}
```

### 检查以下方面:
1. 是否包含非SELECT语句（如INSERT、UPDATE、DELETE、DROP、CREATE等）
2. 是否存在SQL注入风险
3. 是否可能导致全表扫描或性能问题
4. 是否缺少必要的LIMIT条件
5. 语法是否正确
6. 是否符合用户的原始意图: {user_query}

### 请提供验证结果:
"""

class SQLResult(BaseModel):
    """SQL查询结果"""
    sql: str = Field(description="生成的SQL查询语句")
    explanation: str = Field(description="SQL生成的解释")
    is_safe: bool = Field(description="SQL是否安全")
    safety_issues: Optional[List[str]] = Field(default=None, description="安全问题列表")
    
class SQLGenerator:
    """SQL生成器，负责将用户自然语言转换为SQL"""
    
    def __init__(self):
        """初始化SQL生成器"""
        self.llm = get_llm()
        self.retriever = get_retriever()
        self.sql_logger = get_sql_query_logger()
        
    def generate_sql(self, user_query: str, session_context: Optional[List[Dict]] = None) -> SQLResult:
        """
        生成SQL查询
        
        Args:
            user_query: 用户的查询
            session_context: 会话上下文，用于多轮对话
            
        Returns:
            生成的SQL结果
        """
        start_time = time.time()
        
        # 检索相关模式信息
        schema_context = self.retriever.get_schema_context(user_query)
        logger.info(f"为查询检索到相关信息: {len(schema_context)} 字符")
        
        # 生成SQL查询
        prompt = SQL_GENERATION_PROMPT.format(
            schema_context=schema_context,
            user_query=user_query
        )
        
        if session_context:
            # 添加会话上下文
            context_str = "### 会话历史:\n"
            for item in session_context[-3:]:  # 只使用最近3轮对话
                context_str += f"用户: {item['user']}\n"
                if "sql" in item:
                    context_str += f"SQL: {item['sql']}\n"
                if "result" in item:
                    context_str += f"结果: {str(item['result'])[:200]}...(已省略)\n"
            prompt = prompt.replace("### 用户查询:", f"{context_str}\n### 用户查询:")
        
        try:
            # 生成SQL
            logger.info("开始生成SQL")
            response = self.llm(prompt)
            logger.debug(f"LLM原始响应: {response}")
            
            # 解析SQL和思考过程
            sql = self._extract_sql(response)
            explanation = self._extract_explanation(response)
            
            if not sql:
                logger.warning("未从响应中提取到SQL")
                error_msg = "生成SQL失败，未能从模型响应中提取SQL语句"
                self.sql_logger.log_query(
                    user_query=user_query,
                    generated_sql="",
                    execution_time=time.time() - start_time,
                    error=error_msg
                )
                return SQLResult(
                    sql="",
                    explanation=explanation or "生成SQL失败",
                    is_safe=False,
                    safety_issues=["生成SQL失败"]
                )
                
            # 验证SQL安全性
            is_safe, safety_issues = self._validate_sql(sql, user_query)
            
            # 记录SQL生成
            self.sql_logger.log_query(
                user_query=user_query,
                generated_sql=sql,
                execution_time=time.time() - start_time,
                error=None if is_safe else "SQL存在安全问题"
            )
            
            if not is_safe:
                logger.warning(f"生成的SQL存在安全问题: {safety_issues}")
                
            return SQLResult(
                sql=sql,
                explanation=explanation,
                is_safe=is_safe,
                safety_issues=safety_issues
            )
        except Exception as e:
            error_msg = f"生成SQL过程出错: {str(e)}"
            logger.error(error_msg)
            
            # 记录错误
            self.sql_logger.log_query(
                user_query=user_query,
                generated_sql="",
                execution_time=time.time() - start_time,
                error=error_msg
            )
            
            return SQLResult(
                sql="",
                explanation=f"生成SQL时发生错误: {str(e)}",
                is_safe=False,
                safety_issues=["处理过程发生错误"]
            )
    
    def _extract_sql(self, text: str) -> str:
        """从文本中提取SQL查询"""
        # 使用正则表达式匹配SQL代码块
        sql_pattern = r"```sql\s*(.*?)\s*```"
        matches = re.findall(sql_pattern, text, re.DOTALL)
        
        if matches:
            return matches[0].strip()
        
        # 如果没有标准SQL代码块，尝试寻找其他代码块或SQL:之后的内容
        alt_pattern = r"```\s*(.*?)\s*```"
        matches = re.findall(alt_pattern, text, re.DOTALL)
        
        if matches:
            return matches[0].strip()
            
        # 尝试查找"SQL:"后面的内容
        sql_label_pattern = r"SQL:\s*(.*?)(?:\n\s*$|$)"
        matches = re.findall(sql_label_pattern, text, re.DOTALL)
        
        if matches:
            return matches[0].strip()
            
        return ""
    
    def _extract_explanation(self, text: str) -> str:
        """从文本中提取思考过程解释"""
        # 尝试匹配"思考过程:"和"SQL:"之间的内容
        explanation_pattern = r"思考过程:\s*(.*?)(?:\n\s*SQL:|$)"
        matches = re.findall(explanation_pattern, text, re.DOTALL)
        
        if matches:
            return matches[0].strip()
            
        # 如果没找到，返回SQL前的所有内容
        sql_index = text.find("SQL:")
        if sql_index > 0:
            return text[:sql_index].strip()
            
        return ""
    
    def _validate_sql(self, sql: str, user_query: str) -> Tuple[bool, Optional[List[str]]]:
        """
        验证SQL安全性和合规性
        
        Args:
            sql: 要验证的SQL
            user_query: 原始用户查询
            
        Returns:
            (是否安全, 安全问题列表)
        """
        # 基本安全检查
        non_select_patterns = [
            r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE)\b",
            r"\bINTO\s+OUTFILE\b",
            r"\bLOAD_FILE\b",
            r"--",
            r"/\*.*?\*/",
            r";\s*\w+"  # 多条SQL语句
        ]
        
        issues = []
        
        # 检查是否以SELECT开头
        if not re.match(r"^\s*SELECT", sql, re.IGNORECASE):
            issues.append("查询不是以SELECT开头")
            
        # 检查是否包含危险操作
        for pattern in non_select_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                issues.append(f"包含潜在危险操作: {pattern}")
        
        # 检查是否有LIMIT
        if not re.search(r"\bLIMIT\s+\d+\b", sql, re.IGNORECASE) and not "COUNT(" in sql.upper():
            issues.append("查询缺少LIMIT子句，可能返回过多数据")
        
        # 使用LLM进行深度验证
        if not issues:
            validation_prompt = SQL_VALIDATION_PROMPT.format(
                sql=sql,
                user_query=user_query
            )
            
            try:
                validation_response = self.llm(validation_prompt)
                
                # 检查验证响应中的问题指示
                issue_indicators = [
                    "存在安全问题", "不安全", "有风险", "SQL注入",
                    "性能问题", "全表扫描", "缺少限制", "语法错误"
                ]
                
                for indicator in issue_indicators:
                    if indicator in validation_response:
                        # 提取问题描述
                        context = validation_response[validation_response.find(indicator)-30:validation_response.find(indicator)+100]
                        issues.append(f"LLM验证发现问题: {context}")
                        break
            except Exception as e:
                logger.error(f"LLM验证SQL时发生错误: {str(e)}")
        
        is_safe = len(issues) == 0
        return is_safe, issues if issues else None

# 全局SQL生成器实例
_generator_instance = None

def get_sql_generator() -> SQLGenerator:
    """获取全局SQL生成器实例"""
    global _generator_instance
    if _generator_instance is None:
        _generator_instance = SQLGenerator()
    return _generator_instance
