"""
LLM模型加载和推理的相关功能。
使用vLLM加载和推理Qwen3-8B-AWQ模型。
"""

import time
from typing import List, Dict, Any, Optional, Callable, Union

import torch
from vllm import LLM, SamplingParams
from langchain.llms.base import LLM as LangChainLLM
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.outputs import GenerationChunk
from pydantic import Field

from text_to_sql.config.settings import config
from text_to_sql.monitoring.logger import get_logger

logger = get_logger(__name__)

class TextToSQLLLM(LangChainLLM):
    """
    基于vLLM的LLM封装，支持LangChain接口。
    使用Qwen3-8B-AWQ模型进行推理。
    """
    model_name: str = Field(default_factory=lambda: config.model.model_name)
    
    _llm: Optional[LLM] = None
    _default_params: Dict[str, Any] = {
        "temperature": config.model.temperature,
        "top_p": config.model.top_p,
        "max_tokens": config.model.max_tokens,
    }
    
    def __init__(
        self,
        **kwargs
    ):
        """初始化LLM模型"""
        super().__init__(**kwargs)
        self._init_llm()
        
    def _init_llm(self) -> None:
        """初始化vLLM模型"""
        logger.info(f"正在加载模型: {self.model_name}")
        start_time = time.time()
        
        try:
            self._llm = LLM(
                model=self.model_name,
                tensor_parallel_size=config.model.tensor_parallel_size,
                gpu_memory_utilization=config.model.gpu_memory_utilization,
                max_model_len=config.model.max_model_len,
                max_num_seqs=config.model.max_num_seqs,
            )
            logger.info(f"模型加载完成，耗时: {time.time() - start_time:.2f}秒")
        except Exception as e:
            logger.error(f"模型加载失败: {str(e)}")
            raise e
    
    @property
    def _llm_type(self) -> str:
        """返回LLM类型"""
        return "text2sql_llm"
        
    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs
    ) -> str:
        """LLM模型推理方法"""
        if not self._llm:
            raise ValueError("模型未初始化")
            
        # 合并默认参数和用户提供的参数
        params = {**self._default_params, **kwargs}
        if stop:
            params["stop"] = stop
            
        sampling_params = SamplingParams(**params)
        logger.debug(f"推理开始，参数: {sampling_params}")
        
        try:
            outputs = self._llm.generate(prompt, sampling_params)
            if not outputs:
                return ""
            
            generated_text = outputs[0].outputs[0].text
            logger.debug(f"推理完成，生成长度: {len(generated_text)}")
            return generated_text
        except Exception as e:
            logger.error(f"推理失败: {str(e)}")
            raise e
            
    def _stream(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs
    ) -> Any:
        """流式推理，支持实时输出预测结果"""
        if not self._llm:
            raise ValueError("模型未初始化")
            
        params = {**self._default_params, **kwargs}
        if stop:
            params["stop"] = stop
            
        sampling_params = SamplingParams(**params)
        
        try:
            outputs = self._llm.generate(prompt, sampling_params, use_tqdm=False)
            if not outputs:
                return
                
            # 模拟流式输出
            text = outputs[0].outputs[0].text
            for i in range(0, len(text), 4):  # 每次yield 4个字符
                chunk = text[i:i+4]
                if run_manager:
                    run_manager.on_llm_new_token(chunk)
                yield GenerationChunk(text=chunk)
        except Exception as e:
            logger.error(f"流式推理失败: {str(e)}")
            raise e
            
    def get_num_tokens(self, text: str) -> int:
        """获取文本的token数量"""
        # 简单估计，实际上需要根据模型的tokenizer计算
        return len(text) // 2

# 全局LLM实例
_llm_instance = None

def get_llm() -> TextToSQLLLM:
    """获取全局LLM实例，单例模式"""
    global _llm_instance
    if _llm_instance is None:
        _llm_instance = TextToSQLLLM()
    return _llm_instance
