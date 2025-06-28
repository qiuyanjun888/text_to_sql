"""
用户界面模块，提供Gradio-based Web界面和交互功能。
"""

import os
import uuid
import json
import time
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

import gradio as gr

from text_to_sql.config.settings import config
from text_to_sql.agent.agent import get_agent
from text_to_sql.rag.retriever import get_retriever
from text_to_sql.database.connector import get_db_connector
from text_to_sql.monitoring.logger import get_logger

logger = get_logger(__name__)

class TextToSQLInterface:
    """文本到SQL用户界面，使用Gradio实现"""
    
    def __init__(self):
        """初始化界面"""
        self.agent = get_agent()
        self.db_connector = get_db_connector()
        self.retriever = get_retriever()
        self.title = config.ui.title
        self.description = config.ui.description
        self.theme = gr.Theme()  # 使用默认主题
        self.interface = None
        
        # 会话管理
        self.sessions_dir = os.path.join(os.path.dirname(config.knowledge_dir), "sessions")
        os.makedirs(self.sessions_dir, exist_ok=True)
        self.active_sessions = {}
        
    def _load_session(self, session_id: str) -> Dict[str, Any]:
        """加载会话状态"""
        if session_id not in self.active_sessions:
            self.active_sessions[session_id] = {
                "session_id": session_id,
                "conversation_history": [],
                "last_query_time": time.time()
            }
        return self.active_sessions[session_id]
    
    def _save_session(self, session_id: str, data: Dict[str, Any]) -> None:
        """保存会话状态"""
        self.active_sessions[session_id] = data
        self.active_sessions[session_id]["last_query_time"] = time.time()
        
    def _cleanup_old_sessions(self, max_age_hours: int = 24) -> None:
        """清理旧会话"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        to_delete = []
        for session_id, session_data in self.active_sessions.items():
            if current_time - session_data.get("last_query_time", 0) > max_age_seconds:
                to_delete.append(session_id)
                
        for session_id in to_delete:
            del self.active_sessions[session_id]
            
        if to_delete:
            logger.info(f"已清理 {len(to_delete)} 个过期会话")
    
    def process_query(
        self,
        query: str,
        session_id: Optional[str] = None,
        chatbot: Optional[List[Tuple[str, str]]] = None
    ) -> Tuple[Optional[str], List[Tuple[str, str]], Dict, Dict]:
        """
        处理用户查询
        
        Args:
            query: 用户查询
            session_id: 会话ID
            chatbot: 聊天历史
            
        Returns:
            (会话ID, 更新后的聊天历史, 结果, 查询元数据)
        """
        if not query.strip():
            return session_id, chatbot or [], {}, {}
            
        # 处理会话ID
        if not session_id:
            session_id = str(uuid.uuid4())
            chatbot = []
            
        # 准备会话状态
        session_data = self._load_session(session_id)
        
        try:
            # 调用智能体处理查询
            result = self.agent.process_query(
                query=query,
                session_id=session_id,
                conversation_history=session_data.get("conversation_history", [])
            )
            
            # 更新会话历史
            if "conversation_history" in result:
                session_data["conversation_history"] = result["conversation_history"]
                
            # 更新聊天界面
            chatbot = chatbot or []
            chatbot.append((query, result["response"]))
            
            # 提取结果数据
            results = {}
            if "results" in result:
                results = {
                    "columns": result["results"]["columns"],
                    "data": result["results"]["data"],
                    "rows": result["results"]["rows"],
                }
            
            # 提取元数据
            metadata = {
                "query_id": result.get("metadata", {}).get("query_id", ""),
                "execution_time": result.get("metadata", {}).get("execution_time", 0),
                "sql": result.get("sql", ""),
                "error": result.get("error", None)
            }
            
            # 保存会话数据
            self._save_session(session_id, session_data)
            
            # 定期清理旧会话
            if len(self.active_sessions) > 50:  # 当会话数超过50个时执行清理
                self._cleanup_old_sessions()
                
            return session_id, chatbot, results, metadata
            
        except Exception as e:
            logger.error(f"处理查询失败: {str(e)}")
            chatbot = chatbot or []
            chatbot.append((query, f"处理请求时发生错误: {str(e)}"))
            return session_id, chatbot, {}, {"error": str(e)}
    
    def provide_feedback(self, query_id: str, feedback_score: int, feedback_comment: str) -> str:
        """
        提供反馈
        
        Args:
            query_id: 查询ID
            feedback_score: 反馈得分
            feedback_comment: 反馈评论
            
        Returns:
            处理结果消息
        """
        if not query_id:
            return "无法提交反馈：未找到查询ID"
            
        try:
            score = int(feedback_score)
            if score < 1 or score > 5:
                return "反馈分数必须在1-5之间"
                
            success = self.agent.provide_feedback(query_id, score, feedback_comment)
            
            if success:
                return f"感谢您的反馈！（评分: {score}/5）"
            else:
                return "提交反馈时出现错误"
        except Exception as e:
            logger.error(f"提交反馈失败: {str(e)}")
            return f"提交反馈时出现错误: {str(e)}"
    
    def upload_document(self, file) -> str:
        """
        上传知识文档
        
        Args:
            file: 上传的文件
            
        Returns:
            处理结果消息
        """
        if not file:
            return "未选择文件"
            
        try:
            # 确保知识目录存在
            os.makedirs(config.knowledge_dir, exist_ok=True)
            
            # 保存文件
            file_path = os.path.join(config.knowledge_dir, file.name)
            with open(file_path, "wb") as f:
                f.write(file)
                
            # 重新索引知识库
            self.retriever.index_documents()
            
            return f"文档 '{file.name}' 上传成功并已添加到知识库"
        except Exception as e:
            logger.error(f"上传文档失败: {str(e)}")
            return f"上传文档失败: {str(e)}"
    
    def export_database_schema(self) -> str:
        """
        导出数据库模式
        
        Returns:
            处理结果消息
        """
        try:
            output_file = os.path.join(config.knowledge_dir, "database_schema.md")
            success = self.db_connector.export_schema_to_file(output_file)
            
            if success:
                return f"数据库模式已导出到: {output_file}"
            else:
                return "导出数据库模式失败"
        except Exception as e:
            logger.error(f"导出数据库模式失败: {str(e)}")
            return f"导出数据库模式失败: {str(e)}"
    
    def test_database_connection(self) -> str:
        """
        测试数据库连接
        
        Returns:
            连接状态消息
        """
        try:
            success = self.db_connector.test_connection()
            
            if success:
                return "✅ 数据库连接正常"
            else:
                return "❌ 数据库连接失败"
        except Exception as e:
            logger.error(f"测试数据库连接失败: {str(e)}")
            return f"❌ 数据库连接失败: {str(e)}"
    
    def create_ui(self) -> gr.Blocks:
        """创建Gradio界面"""
        with gr.Blocks(title=self.title, theme=self.theme) as interface:
            # 状态变量
            session_id = gr.State("")
            
            with gr.Row():
                with gr.Column(scale=4):
                    gr.Markdown(f"# {self.title}")
                    gr.Markdown(self.description)
                
                with gr.Column(scale=1):
                    connection_status = gr.Textbox(
                        label="数据库连接状态", 
                        value="点击测试连接",
                        interactive=False
                    )
                    test_conn_button = gr.Button("测试连接")
                    test_conn_button.click(
                        fn=self.test_database_connection,
                        outputs=connection_status
                    )
            
            with gr.Tabs():
                with gr.TabItem("文本转SQL"):
                    with gr.Row():
                        with gr.Column(scale=2):
                            chatbot = gr.Chatbot(
                                label="对话",
                                show_copy_button=True,
                                type="messages"
                            )
                            with gr.Row():
                                query_input = gr.Textbox(
                                    label="输入查询",
                                    placeholder="请用自然语言描述您的查询需求...",
                                    lines=2
                                )
                                submit_btn = gr.Button("发送", variant="primary")
                            
                            with gr.Accordion("查询元数据", open=False):
                                with gr.Row():
                                    with gr.Column():
                                        query_id_display = gr.Textbox(
                                            label="查询ID",
                                            interactive=False
                                        )
                                        execution_time_display = gr.Textbox(
                                            label="执行时间",
                                            interactive=False
                                        )
                                    with gr.Column():
                                        feedback_score = gr.Slider(
                                            label="反馈评分",
                                            minimum=1,
                                            maximum=5,
                                            step=1,
                                            value=5
                                        )
                                        feedback_comment = gr.Textbox(
                                            label="反馈评论",
                                            placeholder="请提供您对这个查询结果的反馈..."
                                        )
                                        feedback_btn = gr.Button("提交反馈")
                                feedback_result = gr.Textbox(
                                    label="反馈结果",
                                    interactive=False,
                                    visible=False
                                )
                            
                        with gr.Column(scale=1):
                            sql_display = gr.Code(
                                label="生成的SQL",
                                language="sql",
                                interactive=False,
                                lines=8
                            )
                            result_table = gr.Dataframe(
                                label="查询结果",
                                interactive=False,
                                wrap=True
                            )
                            error_display = gr.Textbox(
                                label="错误信息",
                                interactive=False,
                                visible=False
                            )
                
                with gr.TabItem("知识管理"):
                    with gr.Row():
                        with gr.Column():
                            doc_upload = gr.File(
                                label="上传知识文档",
                                file_count="single",
                                file_types=[".txt", ".md", ".json", ".yaml", ".yml", ".sql"]
                            )
                            upload_btn = gr.Button("上传并索引")
                            upload_result = gr.Textbox(
                                label="上传结果",
                                interactive=False
                            )
                        
                        with gr.Column():
                            export_schema_btn = gr.Button("导出数据库模式")
                            export_result = gr.Textbox(
                                label="导出结果",
                                interactive=False
                            )
            
            # 绑定事件
            submit_btn.click(
                fn=self.process_query,
                inputs=[query_input, session_id, chatbot],
                outputs=[session_id, chatbot, result_table, gr.JSON()],
                api_name="query"
            ).then(
                fn=lambda x: x.get("sql", ""),
                inputs=gr.JSON(),
                outputs=sql_display,
            ).then(
                fn=lambda x: (
                    x.get("query_id", ""),
                    f"{x.get('execution_time', 0):.2f} 秒",
                    gr.update(visible=bool(x.get("error", "")), value=x.get("error", ""))
                ),
                inputs=gr.JSON(),
                outputs=[query_id_display, execution_time_display, error_display]
            )
            
            feedback_btn.click(
                fn=self.provide_feedback,
                inputs=[query_id_display, feedback_score, feedback_comment],
                outputs=feedback_result
            ).then(
                fn=lambda: gr.update(visible=True),
                outputs=feedback_result
            )
            
            upload_btn.click(
                fn=self.upload_document,
                inputs=doc_upload,
                outputs=upload_result
            )
            
            export_schema_btn.click(
                fn=self.export_database_schema,
                outputs=export_result
            )
            
            # 清空输入框
            submit_btn.click(
                fn=lambda: "",
                outputs=query_input
            )
            
        self.interface = interface
        return interface
    
    def launch(self, **kwargs):
        """启动服务"""
        if not self.interface:
            self.create_ui()
            
        # 合并默认参数和用户提供的参数
        default_kwargs = {
            "server_name": config.ui.server_name,
            "server_port": config.ui.server_port,
            "share": False,
            "debug": config.debug
        }
        
        # 用户参数优先
        launch_kwargs = {**default_kwargs, **kwargs}
        
        # 启动服务
        logger.info(f"启动UI服务: {launch_kwargs}")
        if self.interface:
            self.interface.launch(**launch_kwargs)
        
# 全局UI实例
_ui_instance = None

def get_ui() -> TextToSQLInterface:
    """获取全局UI实例"""
    global _ui_instance
    if _ui_instance is None:
        _ui_instance = TextToSQLInterface()
    return _ui_instance
