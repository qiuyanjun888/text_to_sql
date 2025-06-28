# Text-to-SQL 智能体系统

基于大语言模型的文本转SQL智能体系统，利用RAG技术和Agent框架将用户自然语言查询转换为SQL语句并执行。

## 功能特点

- 自然语言转SQL查询
- 基于RAG的企业知识库集成
- 多轮对话支持
- 安全SQL生成
- 智能体自动执行数据库操作
- 实时监控与反馈系统

## 技术架构

- 基础模型：Qwen3-8B-AWQ
- 向量检索：FAISS
- Agent框架：LangChain
- 数据库连接：SQLAlchemy
- 前端界面：Gradio
- 监控系统：MLflow

## 安装与使用

### 环境要求

- Python 3.12+
- PyTorch
- vLLM

### 安装

```bash
# 创建虚拟环境
conda create -n text2sql python=3.12
conda activate text2sql

# 安装依赖
pip install -r requirements.txt
```

### 配置

1. 在`config`目录中配置数据库连接信息
2. 导入企业知识文档到`knowledge`目录

### 启动服务

```bash
python -m text_to_sql.app
```

## 项目结构

```
text_to_sql/
├── app.py              # 主应用入口
├── agent/              # 智能体实现
├── config/             # 配置文件
├── database/           # 数据库连接
├── knowledge/          # 知识库管理
├── models/             # 模型加载与推理
├── monitoring/         # 监控系统
├── rag/                # RAG实现
├── sql/                # SQL处理
├── ui/                 # 用户界面
└── utils/              # 工具函数
```
