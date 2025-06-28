"""
应用程序入口文件，用于启动文本到SQL系统。
"""

import os
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()
# 设置CUDA内存分配器策略，允许使用可扩展分段以降低显存碎片化导致的OOM
os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")

# 避免版本冲突和循环导入
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["PYTHONPATH"] = str(Path(__file__).parent.parent)

from text_to_sql.config.settings import config
from text_to_sql.ui.interface import get_ui
from text_to_sql.monitoring.logger import get_logger
from text_to_sql.rag.retriever import get_retriever
from text_to_sql.database.connector import get_db_connector
from text_to_sql.models.llm import get_llm

logger = get_logger(__name__)

def setup_arg_parser() -> argparse.ArgumentParser:
    """设置命令行参数解析器"""
    parser = argparse.ArgumentParser(description="文本到SQL智能体系统")
    
    parser.add_argument(
        "--port", 
        type=int, 
        default=config.ui.server_port,
        help="Web服务端口"
    )
    
    parser.add_argument(
        "--host", 
        type=str, 
        default=config.ui.server_name,
        help="Web服务主机地址"
    )
    
    parser.add_argument(
        "--debug", 
        action="store_true",
        default=config.debug,
        help="调试模式"
    )
    
    parser.add_argument(
        "--share", 
        action="store_true",
        default=False,
        help="是否公开分享"
    )
    
    parser.add_argument(
        "--init-db-schema",
        action="store_true",
        default=False,
        help="导出数据库模式到知识库。如果同时提供了ER图，此选项将被忽略。"
    )
    
    parser.add_argument(
        "--er-diagram",
        type=str,
        default=None,
        help="使用指定的ER图文件路径作为数据库模式知识。优先于 --init-db-schema。"
    )
    
    parser.add_argument(
        "--index-documents",
        action="store_true",
        default=False,
        help="索引知识库文档"
    )
    
    return parser

def init_system(args) -> None:
    """
    初始化系统组件
    
    Args:
        args: 命令行参数
    """
    # 配置日志级别
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("调试模式已启用")
    
    # --- Step 1: Prepare database schema knowledge source ---
    schema_target_file = os.path.join(config.knowledge_dir, "database_schema.md")
    os.makedirs(config.knowledge_dir, exist_ok=True)

    # Option 1: Use ER Diagram (takes precedence)
    if args.er_diagram:
        if args.init_db_schema:
            logger.warning("同时提供了 --er-diagram 和 --init-db-schema。ER图优先，后者将被忽略。")
        
        if os.path.exists(args.er_diagram):
            logger.info(f"使用ER图 '{args.er_diagram}' 作为数据库模式知识源。")
            try:
                with open(args.er_diagram, 'r', encoding='utf-8') as f_source:
                    content = f_source.read()
                with open(schema_target_file, 'w', encoding='utf-8') as f_target:
                    f_target.write(content)
                logger.info(f"已将ER图内容复制到: {schema_target_file}")
            except Exception as e:
                logger.error(f"处理ER图文件 '{args.er_diagram}' 失败: {str(e)}")
        else:
            logger.error(f"指定的ER图文件不存在: {args.er_diagram}")

    # Option 2: Export from DB
    elif args.init_db_schema:
        logger.info("准备从数据库导出模式作为知识源...")
        try:
            db_connector = get_db_connector()
            if db_connector.test_connection():
                logger.info("数据库连接成功，正在导出模式...")
                if db_connector.export_schema_to_file(schema_target_file):
                    logger.info(f"数据库模式已成功导出到: {schema_target_file}")
                else:
                    logger.error("导出数据库模式失败。")
            else:
                logger.warning("数据库连接测试失败，无法导出模式。")
        except Exception as e:
            logger.error(f"连接数据库或导出模式时出错: {str(e)}")
    
    # Option 3: No action, use existing file or warn
    else:
        if os.path.exists(schema_target_file):
            logger.info(f"找到已存在的数据库模式文件 '{schema_target_file}'，将用其作为知识源。")
        else:
            logger.warning(
                f"知识库中缺少数据库模式文件 ({schema_target_file}) "
                "且未提供 --er-diagram 或 --init-db-schema。RAG系统可能无法准确生成SQL。"
            )

    # --- Step 2: Test DB connection for executor ---
    try:
        db_connector = get_db_connector()
        if db_connector.test_connection():
            logger.info("数据库连接成功，SQL执行器已就绪。")
        else:
            logger.warning("数据库连接测试失败，SQL执行器可能无法工作。")
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
    
    # --- Step 3: Index knowledge documents ---
    if args.index_documents:
        try:
            logger.info("正在索引知识库文档...")
            retriever = get_retriever()
            retriever.index_documents()
            logger.info("文档索引完成")
        except Exception as e:
            logger.error(f"索引文档失败: {str(e)}")
    
    # --- Step 4: Pre-warm LLM model ---
    try:
        logger.info("正在预热LLM模型...")
        llm = get_llm()
        response = llm("Hello, world!")
        logger.info("LLM模型预热完成")
    except Exception as e:
        logger.error(f"LLM模型预热失败: {str(e)}")

def main():
    """应用程序入口点"""
    # 解析命令行参数
    parser = setup_arg_parser()
    args = parser.parse_args()
    
    # 初始化系统
    init_system(args)
    
    try:
        # 启动UI
        logger.info("正在启动UI服务...")
        ui = get_ui()
        ui.launch(
            server_name=args.host,
            server_port=args.port,
            share=args.share,
            debug=args.debug
        )
    except Exception as e:
        logger.error(f"启动UI服务失败: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
