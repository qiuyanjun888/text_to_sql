"""
Agent模块，负责协调LLM、RAG、SQL生成和执行，实现大模型驱动的智能体系统。
"""

import time
import uuid
import json
from typing import Dict, List, Any, Optional, Tuple, Union, Callable, Sequence
from pydantic import BaseModel, Field, ConfigDict
import pandas as pd

from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.tools import Tool, BaseTool
from langchain.prompts import PromptTemplate, ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain.tools.render import render_text_description
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tool_node

from text_to_sql.config.settings import config
from text_to_sql.models.llm import get_llm
from text_to_sql.rag.retriever import get_retriever
from text_to_sql.sql.generator import get_sql_generator, SQLResult
from text_to_sql.database.connector import get_db_connector
from text_to_sql.monitoring.logger import get_logger, get_user_interaction_logger, get_sql_query_logger

logger = get_logger(__name__)

# Agent状态类型
class AgentState(BaseModel):
    """Agent状态"""
    query: str
    session_id: str
    conversation_history: List[Dict[str, Any]] = Field(default_factory=list)
    current_sql: Optional[str] = None
    current_results: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    response: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)

# Agent工具
class TextToSQLTools:
    """TextToSQL Agent使用的工具集"""
    
    @staticmethod
    def generate_sql(query: str, session_id: str, conversation_history: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        生成SQL工具
        
        Args:
            query: 用户查询
            session_id: 会话ID
            conversation_history: 对话历史
            
        Returns:
            包含SQL的字典
        """
        try:
            logger.info(f"[会话 {session_id[:8]}] 生成SQL: {query}")
            sql_generator = get_sql_generator()
            
            # 调用SQL生成器
            result = sql_generator.generate_sql(query, session_context=conversation_history)
            
            if not result.is_safe:
                issues = ",".join(result.safety_issues or ["未知安全问题"])
                return {
                    "success": False, 
                    "error": f"生成的SQL存在安全问题: {issues}",
                    "sql": result.sql,
                    "explanation": result.explanation
                }
            
            return {
                "success": True,
                "sql": result.sql,
                "explanation": result.explanation
            }
        except Exception as e:
            logger.error(f"[会话 {session_id[:8]}] SQL生成失败: {str(e)}")
            return {"success": False, "error": f"SQL生成错误: {str(e)}"}

    @staticmethod
    def execute_sql(sql: str, session_id: str, original_query: str) -> Dict[str, Any]:
        """
        执行SQL工具
        
        Args:
            sql: SQL查询
            session_id: 会话ID
            original_query: 原始用户查询
            
        Returns:
            执行结果
        """
        try:
            logger.info(f"[会话 {session_id[:8]}] 执行SQL: {sql}")
            db_connector = get_db_connector()
            sql_logger = get_sql_query_logger()
            
            # 执行查询
            df, execution_time = db_connector.execute_query(sql)
            
            # 记录结果
            query_id = sql_logger.log_query(
                user_query=original_query,
                generated_sql=sql,
                execution_time=execution_time
            )
            
            # 将DataFrame转为字典格式
            result = {
                "success": True,
                "rows": len(df),
                "columns": list(df.columns),
                "execution_time": execution_time,
                "query_id": query_id,
                "data": df.head(50).to_dict(orient='records')  # 限制返回的行数
            }
            
            logger.info(f"[会话 {session_id[:8]}] SQL执行完成，返回 {len(df)} 行结果")
            return result
        except Exception as e:
            logger.error(f"[会话 {session_id[:8]}] SQL执行失败: {str(e)}")
            return {"success": False, "error": f"SQL执行错误: {str(e)}"}
    
    @staticmethod
    def get_database_schema(session_id: str) -> Dict[str, Any]:
        """
        获取数据库模式工具
        
        Args:
            session_id: 会话ID
            
        Returns:
            数据库模式信息
        """
        try:
            logger.info(f"[会话 {session_id[:8]}] 获取数据库模式")
            db_connector = get_db_connector()
            schema_info = db_connector.get_schema_info()
            
            if "error" in schema_info:
                return {"success": False, "error": schema_info["error"]}
            
            return {
                "success": True,
                "schema": schema_info
            }
        except Exception as e:
            logger.error(f"[会话 {session_id[:8]}] 获取数据库模式失败: {str(e)}")
            return {"success": False, "error": f"获取数据库模式错误: {str(e)}"}

    @staticmethod
    def retrieve_relevant_knowledge(query: str, session_id: str) -> Dict[str, Any]:
        """
        检索相关知识工具
        
        Args:
            query: 用户查询
            session_id: 会话ID
            
        Returns:
            检索结果
        """
        try:
            logger.info(f"[会话 {session_id[:8]}] 检索相关知识: {query}")
            retriever = get_retriever()
            
            # 检索相关文档
            docs = retriever.retrieve_database_schema(query)
            
            if not docs:
                return {
                    "success": True,
                    "documents": [],
                    "message": "未找到相关知识"
                }
            
            # 构建文档摘要
            documents = []
            for doc in docs:
                documents.append({
                    "content": doc.page_content,
                    "source": doc.metadata.get("source", ""),
                    "type": doc.metadata.get("doc_type", "other")
                })
            
            return {
                "success": True,
                "documents": documents,
                "message": f"找到 {len(documents)} 个相关文档"
            }
        except Exception as e:
            logger.error(f"[会话 {session_id[:8]}] 检索相关知识失败: {str(e)}")
            return {"success": False, "error": f"检索相关知识错误: {str(e)}"}


# 构建Agent工具集
def get_agent_tools() -> Sequence[BaseTool]:
    """
    获取Agent可用的工具列表
    
    Returns:
        工具列表
    """
    # 定义参数模型
    class GenerateSQLInput(BaseModel):
        query: str = Field(description="用户查询")
        session_id: str = Field(description="当前会话ID")
        conversation_history: Optional[List[Dict[str, Any]]] = Field(default=None, description="可选的对话历史")
    
    class ExecuteSQLInput(BaseModel):
        sql: str = Field(description="要执行的SQL查询")
        session_id: str = Field(description="当前会话ID")
        original_query: str = Field(description="用户的原始查询")
    
    class GetDBSchemaInput(BaseModel):
        session_id: str = Field(description="当前会话ID")
    
    class RetrieveKnowledgeInput(BaseModel):
        query: str = Field(description="用户查询")
        session_id: str = Field(description="当前会话ID")
    
    tools = [
        Tool(
            name="generate_sql",
            func=TextToSQLTools.generate_sql,
            description="根据用户查询和对话历史生成SQL查询。当你需要查询数据库以回答问题时使用。输入应为用户的自然语言查询。",
            args_schema=GenerateSQLInput
        ),
        Tool(
            name="execute_sql",
            func=TextToSQLTools.execute_sql,
            description="执行SQL查询并返回结果。只有在生成了有效的SQL后才能使用此工具。输入应为有效的SQL查询字符串。",
            args_schema=ExecuteSQLInput
        ),
        Tool(
            name="get_database_schema",
            func=TextToSQLTools.get_database_schema,
            description="获取当前数据库的模式信息，包括表、列和它们的关系。当不清楚表结构时使用。",
            args_schema=GetDBSchemaInput
        ),
        Tool(
            name="retrieve_relevant_knowledge",
            func=TextToSQLTools.retrieve_relevant_knowledge,
            description="根据用户查询从知识库中检索相关的业务规则、指标定义或表结构信息。在生成SQL之前使用，以获取更多上下文。",
            args_schema=RetrieveKnowledgeInput
        )
    ]
    return tools


# Agent系统提示
AGENT_SYSTEM_PROMPT = """你是一个基于大语言模型的文本到SQL智能体，名为"Text-to-SQL Agent"。你可以帮助用户将自然语言问题转换为SQL查询，并执行这些查询来获取数据库中的信息。

你有以下能力:
1. 理解用户的自然语言问题
2. 生成精确的SQL查询
3. 执行SQL查询并返回结果
4. 解释你的推理过程和结果
5. 检索数据库结构和知识库信息
6. 进行多轮对话，基于前面的查询进行后续分析

请遵循以下原则:
1. 安全第一: 只生成并执行SELECT查询，不执行任何修改数据或结构的操作
2. 清晰准确: 提供清晰的解释和准确的SQL查询
3. 效率优先: 生成高效的查询，避免不必要的复杂性
4. 实用导向: 关注用户的实际需求，提供有用的信息

工作流程:
1. 当收到用户查询时，首先理解用户意图
2. 检索相关的数据库结构和知识库信息
3. 生成符合用户需求的SQL查询
4. 执行查询并获取结果
5. 向用户展示结果并提供解释
6. 追踪对话上下文，支持后续的对话和查询

请使用自然、专业的语言与用户交流，保持对话流畅和有帮助。
"""

class TextToSQLAgent:
    """
    文本到SQL智能体，负责协调各个组件并实现端到端的文本到SQL功能
    """
    
    def __init__(self):
        """初始化智能体"""
        self.llm = get_llm()
        self.tools = get_agent_tools()
        self.interaction_logger = get_user_interaction_logger()
        self._graph = None
        
        # 初始化智能体图
        self._init_agent_graph()
        
    def _init_agent_graph(self) -> None:
        """初始化智能体工作图"""
        logger.info("初始化智能体工作流程")
        
        # 工作流节点
        def agent_node(state: AgentState) -> Dict[str, Any]:
            """智能体主节点，处理用户输入并决定下一步操作"""
            # 构建系统提示
            system_message = SystemMessage(
                role="system", 
                content="You are a database expert. Please generate a user-friendly response based on the query results."
            )
            
            # 动态构建对话历史
            messages: List[Union[SystemMessage, HumanMessage, AIMessage]] = [system_message]
            if state.conversation_history:
                for item in state.conversation_history:
                    if item["role"] == "user":
                        messages.append(HumanMessage(content=item["content"]))
                    elif item["role"] == "assistant":
                        messages.append(AIMessage(content=item["content"]))
            
            # 添加当前查询
            messages.append(HumanMessage(content=state.query))
            
            try:
                # 让LLM决定使用什么工具
                tool_call_text = render_text_description(list(self.tools))
                response = self.llm.invoke(
                    [
                        SystemMessage(content=f"{AGENT_SYSTEM_PROMPT}\n\n你可以使用以下工具:\n{tool_call_text}"),
                        *messages
                    ]
                )
                logger.debug(f"智能体响应: {response}")
                
                # 解析工具调用
                if "使用工具 generate_sql" in response or "我将使用 generate_sql" in response:
                    return {"next": "generate_sql"}
                elif "使用工具 execute_sql" in response or "我将使用 execute_sql" in response:
                    return {"next": "execute_sql"}
                elif "使用工具 get_database_schema" in response or "我将使用 get_database_schema" in response:
                    return {"next": "get_database_schema"}
                elif "使用工具 retrieve_relevant_knowledge" in response or "我将使用 retrieve_relevant_knowledge" in response:
                    return {"next": "retrieve_relevant_knowledge"}
                else:
                    # 如果没有明确的工具调用，先检索知识
                    return {"next": "retrieve_relevant_knowledge"}
            except Exception as e:
                logger.error(f"Agent决策失败: {str(e)}")
                state.error = f"智能体决策失败: {str(e)}"
                state.response = f"很抱歉，处理您的请求时出现了错误: {str(e)}"
                return {"next": "respond"}
        
        def generate_sql_node(state: AgentState) -> Dict[str, Any]:
            """生成SQL节点"""
            try:
                logger.info(f"[会话 {state.session_id[:8]}] 调用SQL生成工具")
                result = TextToSQLTools.generate_sql(
                    query=state.query, 
                    session_id=state.session_id,
                    conversation_history=state.conversation_history
                )
                
                if result["success"]:
                    state.current_sql = result["sql"]
                    # 自动执行SQL
                    return {"next": "execute_sql"}
                else:
                    state.error = result.get("error", "SQL生成失败")
                    state.response = f"我无法为您的查询生成有效的SQL。\n\n错误: {state.error}"
                    if "explanation" in result:
                        state.response += f"\n\n分析: {result['explanation']}"
                    return {"next": "respond"}
            except Exception as e:
                logger.error(f"SQL生成节点失败: {str(e)}")
                state.error = f"SQL生成失败: {str(e)}"
                state.response = f"很抱歉，生成SQL查询时出现了错误: {str(e)}"
                return {"next": "respond"}
        
        def execute_sql_node(state: AgentState) -> Dict[str, Any]:
            """执行SQL节点"""
            try:
                if not state.current_sql:
                    state.error = "没有SQL可执行"
                    state.response = "抱歉，我没有找到需要执行的SQL查询。请提供一个明确的查询需求。"
                    return {"next": "respond"}
                
                logger.info(f"[会话 {state.session_id[:8]}] 调用SQL执行工具")
                result = TextToSQLTools.execute_sql(
                    sql=state.current_sql,
                    session_id=state.session_id,
                    original_query=state.query
                )
                
                if result["success"]:
                    if "data" in result:
                        # 转换为DataFrame方便处理
                        state.current_results = pd.DataFrame(result["data"])
                    
                    # 更新元数据
                    state.metadata["execution_time"] = result.get("execution_time", 0)
                    state.metadata["rows"] = result.get("rows", 0)
                    state.metadata["columns"] = result.get("columns", [])
                    state.metadata["query_id"] = result.get("query_id", "")
                    
                    # 自动生成响应
                    return {"next": "generate_response"}
                else:
                    state.error = result.get("error", "SQL执行失败")
                    state.response = f"我无法执行生成的SQL查询。\n\n错误: {state.error}\n\nSQL: \n```sql\n{state.current_sql}\n```"
                    return {"next": "respond"}
            except Exception as e:
                logger.error(f"SQL执行节点失败: {str(e)}")
                state.error = f"SQL执行失败: {str(e)}"
                state.response = f"很抱歉，执行SQL查询时出现了错误: {str(e)}\n\nSQL: \n```sql\n{state.current_sql or '未生成'}\n```"
                return {"next": "respond"}
        
        def get_database_schema_node(state: AgentState) -> Dict[str, Any]:
            """获取数据库模式节点"""
            try:
                logger.info(f"[会话 {state.session_id[:8]}] 调用数据库模式获取工具")
                result = TextToSQLTools.get_database_schema(session_id=state.session_id)
                
                if result["success"]:
                    # 设置上下文
                    state.context = {"schema": result["schema"]}
                    # 进入SQL生成节点
                    return {"next": "generate_sql"}
                else:
                    state.error = result.get("error", "获取数据库模式失败")
                    state.response = f"我无法获取数据库模式信息。\n\n错误: {state.error}"
                    return {"next": "respond"}
            except Exception as e:
                logger.error(f"获取数据库模式节点失败: {str(e)}")
                state.error = f"获取数据库模式失败: {str(e)}"
                state.response = f"很抱歉，获取数据库结构信息时出现了错误: {str(e)}"
                return {"next": "respond"}
        
        def retrieve_relevant_knowledge_node(state: AgentState) -> Dict[str, Any]:
            """检索相关知识节点"""
            try:
                logger.info(f"[会话 {state.session_id[:8]}] 调用知识检索工具")
                result = TextToSQLTools.retrieve_relevant_knowledge(
                    query=state.query,
                    session_id=state.session_id
                )
                
                if result["success"]:
                    # 设置上下文
                    state.context = {"documents": result.get("documents", [])}
                    # 进入SQL生成节点
                    return {"next": "generate_sql"}
                else:
                    state.error = result.get("error", "检索知识失败")
                    # 尝试直接生成SQL
                    return {"next": "generate_sql"}
            except Exception as e:
                logger.error(f"检索知识节点失败: {str(e)}")
                state.error = f"检索知识失败: {str(e)}"
                # 尝试直接生成SQL
                return {"next": "generate_sql"}
        
        def generate_response_node(state: AgentState) -> Dict[str, Any]:
            """生成响应节点"""
            try:
                logger.info(f"[会话 {state.session_id[:8]}] 生成结果响应")
                
                # 构建系统提示
                system_prompt = f"""你是一个数据分析专家，负责解释SQL查询结果并提供见解。
请基于用户的原始查询和SQL执行结果，生成一个清晰、信息丰富的响应。

原始查询: {state.query}
执行的SQL: 
```sql
{state.current_sql}
```

查询结果:
- 返回行数: {state.metadata.get('rows', 0)}
- 返回列: {', '.join(state.metadata.get('columns', []))}
- 执行时间: {state.metadata.get('execution_time', 0):.2f}秒

在你的回答中:
1. 简要解释SQL查询的作用
2. 总结查询结果的关键信息和见解
3. 如有必要，建议后续可能的分析方向
4. 保持专业、简洁的语言风格
"""

                # 添加结果数据到提示
                if state.current_results is not None and not state.current_results.empty:
                    result_str = state.current_results.to_string(index=False, max_rows=10)
                    system_prompt += f"\n\n查询结果数据 (最多10行):\n{result_str}\n"
                    
                    if len(state.current_results) > 10:
                        system_prompt += f"\n注: 共返回 {len(state.current_results)} 行，上面只显示前10行。"
                
                # 调用LLM生成响应
                response = self.llm.invoke(
                    [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content="请解释这些查询结果并提供见解。")
                    ]
                )
                
                # 构建完整响应
                full_response = f"""### 查询结果

{response}

### 执行的SQL
```sql
{state.current_sql}
```

### 统计信息
- 返回行数: {state.metadata.get('rows', 0)}
- 执行时间: {state.metadata.get('execution_time', 0):.2f}秒
"""
                
                state.response = full_response
                return {"next": "respond"}
                
            except Exception as e:
                logger.error(f"生成响应节点失败: {str(e)}")
                state.error = f"生成响应失败: {str(e)}"
                state.response = f"查询执行成功，但我在解释结果时遇到了问题: {str(e)}\n\nSQL: \n```sql\n{state.current_sql}\n```"
                return {"next": "respond"}
        
        def respond_node(state: AgentState) -> Dict[str, Any]:
            """最终响应节点，记录日志并返回结果"""
            end_time = time.time()
            duration = end_time - state.metadata.get("start_time", end_time)
            
            # 记录用户交互
            interaction_logger = get_user_interaction_logger()
            interaction_logger.log_interaction(
                session_id=state.session_id,
                user_input=state.query,
                system_response=state.response or "",
                duration=duration,
                metadata={
                    "sql": state.current_sql,
                    "error": state.error,
                    "result_rows": len(state.current_results) if state.current_results is not None else 0
                }
            )
            return {"response": state.response}
        
        # 定义工作图
        workflow = StateGraph(AgentState)
        
        # 添加节点
        workflow.add_node("agent", agent_node)
        workflow.add_node("generate_sql", generate_sql_node)
        workflow.add_node("execute_sql", execute_sql_node)
        workflow.add_node("get_database_schema", get_database_schema_node)
        workflow.add_node("retrieve_relevant_knowledge", retrieve_relevant_knowledge_node)
        workflow.add_node("generate_response", generate_response_node)
        workflow.add_node("respond", respond_node)
        
        # 设置起始节点
        workflow.set_entry_point("agent")
        
        # 添加边
        workflow.add_edge("agent", "generate_sql")
        workflow.add_edge("agent", "execute_sql")
        workflow.add_edge("agent", "get_database_schema")
        workflow.add_edge("agent", "retrieve_relevant_knowledge")
        
        workflow.add_edge("generate_sql", "execute_sql")
        workflow.add_edge("generate_sql", "respond")
        
        workflow.add_edge("execute_sql", "generate_response")
        workflow.add_edge("execute_sql", "respond")
        
        workflow.add_edge("get_database_schema", "generate_sql")
        workflow.add_edge("get_database_schema", "respond")
        
        workflow.add_edge("retrieve_relevant_knowledge", "generate_sql")
        workflow.add_edge("retrieve_relevant_knowledge", "respond")
        
        workflow.add_edge("generate_response", "respond")
        
        # 编译工作图
        self._graph = workflow.compile()
        
    def process_query(self, query: str, session_id: Optional[str] = None, conversation_history: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        处理用户查询
        
        Args:
            query: 用户查询
            session_id: 会话ID
            conversation_history: 对话历史
            
        Returns:
            处理结果
        """
        if not session_id:
            session_id = str(uuid.uuid4())
        
        initial_state = {
            "query": query,
            "session_id": session_id,
            "conversation_history": conversation_history or [],
            "metadata": {"start_time": time.time()}
        }
        
        if not self._graph:
            raise RuntimeError("Agent executor is not initialized.")
            
        # 调用工作流
        final_state_dict = self._graph.invoke(initial_state)
        
        # 将最终状态字典转换为AgentState对象以便于安全访问
        final_state = AgentState.model_validate(final_state_dict)

        # 返回最终结果
        return {
            "session_id": session_id,
            "response": final_state.response,
            "conversation_history": final_state.conversation_history,
            "error": final_state.error,
            "metadata": final_state.metadata,
            "intermediate_steps": {
                "sql_query": final_state.current_sql,
                "sql_result_preview": final_state.current_results.head().to_dict(orient='records') if final_state.current_results is not None and not final_state.current_results.empty else None
            }
        }

    def provide_feedback(self, query_id: str, feedback_score: int, comment: Optional[str] = None) -> bool:
        """
        提供反馈
        
        Args:
            query_id: 查询ID
            feedback_score: 反馈分数（1-5）
            comment: 反馈评论
            
        Returns:
            是否成功
        """
        logger.info(f"为查询 {query_id} 提供反馈: {feedback_score}/5")
        sql_logger = get_sql_query_logger()
        
        try:
            sql_logger.log_feedback(query_id, feedback_score, comment)
            return True
        except Exception as e:
            logger.error(f"提供反馈失败: {str(e)}")
            return False

# 全局Agent实例
_agent_instance = None

def get_agent() -> TextToSQLAgent:
    """获取全局Agent实例"""
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = TextToSQLAgent()
    return _agent_instance
