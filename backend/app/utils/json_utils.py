"""
JSON工具函数
处理LLM返回的JSON解析，包括markdown代码块清理
"""

import json
import re
from typing import Any, Optional


def clean_llm_json_response(response: str) -> str:
    """
    清理LLM返回的JSON字符串
    
    部分模型（如MiniMax M2.5, GLM-4.7, GLM-5）不遵守json_object格式，
    会返回markdown包裹的JSON代码块，需要清理。
    
    Args:
        response: LLM返回的原始响应
        
    Returns:
        清理后的JSON字符串
    """
    if not response:
        return response
        
    cleaned = response.strip()
    
    # 移除markdown代码块标记 ```json ... ``` 或 ``` ... ```
    cleaned = re.sub(r'^```(?:json)?\s*\n?', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\n?```\s*$', '', cleaned)
    
    # 移除可能的前后空白
    cleaned = cleaned.strip()
    
    return cleaned


def parse_llm_json(response: str, default: Optional[Any] = None) -> Any:
    """
    解析LLM返回的JSON，自动清理markdown代码块
    
    Args:
        response: LLM返回的原始响应
        default: 解析失败时的默认返回值（None表示抛出异常）
        
    Returns:
        解析后的Python对象
        
    Raises:
        json.JSONDecodeError: 当解析失败且未提供default时
    """
    cleaned = clean_llm_json_response(response)
    
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        if default is not None:
            return default
        raise ValueError(f"LLM返回的JSON格式无效: {cleaned[:200]}...") from e


def safe_parse_llm_json(response: str) -> tuple[bool, Any]:
    """
    安全解析LLM返回的JSON，返回成功标志和结果
    
    Args:
        response: LLM返回的原始响应
        
    Returns:
        (success, result) 元组：
        - success: 是否解析成功
        - result: 解析结果（失败时为错误信息字符串）
    """
    try:
        result = parse_llm_json(response)
        return True, result
    except (json.JSONDecodeError, ValueError) as e:
        return False, str(e)
