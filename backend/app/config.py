"""
配置管理
统一从项目根目录的 .env 文件加载配置
"""

import os
from dotenv import load_dotenv

# 加载项目根目录的 .env 文件
# 路径: MiroFish/.env (相对于 backend/app/config.py)
project_root_env = os.path.join(os.path.dirname(__file__), '../../.env')

if os.path.exists(project_root_env):
    load_dotenv(project_root_env, override=True)
else:
    # 如果根目录没有 .env，尝试加载环境变量（用于生产环境）
    load_dotenv(override=True)


class Config:
    """Flask配置类"""
    
    # Flask配置
    SECRET_KEY = os.environ.get('SECRET_KEY', 'mirofish-secret-key')
    DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    
    # JSON配置 - 禁用ASCII转义，让中文直接显示（而不是 \uXXXX 格式）
    JSON_AS_ASCII = False
    
    # LLM配置（统一使用OpenAI格式）
    LLM_API_KEY = os.environ.get('LLM_API_KEY')
    LLM_BASE_URL = os.environ.get('LLM_BASE_URL', 'https://api.openai.com/v1')
    LLM_MODEL_NAME = os.environ.get('LLM_MODEL_NAME', 'gpt-4o-mini')
    
    # Zep配置
    ZEP_API_KEY = os.environ.get('ZEP_API_KEY')

    # RAGflow配置（知识图谱后端替代方案）
    # GRAPH_BACKEND: 图谱构建后端，可选 "zep"（默认）/ "ragflow" / "zep_local"
    GRAPH_BACKEND = os.environ.get('GRAPH_BACKEND', 'zep')
    RAGFLOW_BASE_URL = os.environ.get('RAGFLOW_BASE_URL', 'http://localhost')
    RAGFLOW_API_KEY = os.environ.get('RAGFLOW_API_KEY')
    # RAGflow可选配置：指定使用的LLM和Embedding模型（留空使用RAGflow系统默认）
    RAGFLOW_LLM_ID = os.environ.get('RAGFLOW_LLM_ID', '')
    RAGFLOW_EMBEDDING_MODEL = os.environ.get('RAGFLOW_EMBEDDING_MODEL', '')

    # Zep本地模式配置（使用 Neo4j + Qdrant 替代 Zep Cloud）
    NEO4J_URI = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    NEO4J_USERNAME = os.environ.get('NEO4J_USERNAME', 'neo4j')
    NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')
    QDRANT_URL = os.environ.get('QDRANT_URL', 'http://localhost:6333')

    # Embedding 模型配置
    EMBEDDING_MODEL = os.environ.get('EMBEDDING_MODEL', 'text-embedding-v3')
    EMBEDDING_USE_LOCAL = os.environ.get('EMBEDDING_USE_LOCAL', 'false').lower() == 'true'
    EMBEDDING_LOCAL_MODEL = os.environ.get(
        'EMBEDDING_LOCAL_MODEL',
        'paraphrase-multilingual-MiniLM-L12-v2'
    )
    EMBEDDING_CACHE_DIR = os.environ.get('EMBEDDING_CACHE_DIR', '')
    
    # 文件上传配置
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB
    UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '../uploads')
    ALLOWED_EXTENSIONS = {'pdf', 'md', 'txt', 'markdown'}
    
    # 文本处理配置
    DEFAULT_CHUNK_SIZE = 500  # 默认切块大小
    DEFAULT_CHUNK_OVERLAP = 50  # 默认重叠大小
    
    # OASIS模拟配置
    OASIS_DEFAULT_MAX_ROUNDS = int(os.environ.get('OASIS_DEFAULT_MAX_ROUNDS', '10'))
    OASIS_SIMULATION_DATA_DIR = os.path.join(os.path.dirname(__file__), '../uploads/simulations')
    
    # OASIS平台可用动作配置
    OASIS_TWITTER_ACTIONS = [
        'CREATE_POST', 'LIKE_POST', 'REPOST', 'FOLLOW', 'DO_NOTHING', 'QUOTE_POST'
    ]
    OASIS_REDDIT_ACTIONS = [
        'LIKE_POST', 'DISLIKE_POST', 'CREATE_POST', 'CREATE_COMMENT',
        'LIKE_COMMENT', 'DISLIKE_COMMENT', 'SEARCH_POSTS', 'SEARCH_USER',
        'TREND', 'REFRESH', 'DO_NOTHING', 'FOLLOW', 'MUTE'
    ]
    
    # Report Agent配置
    REPORT_AGENT_MAX_TOOL_CALLS = int(os.environ.get('REPORT_AGENT_MAX_TOOL_CALLS', '5'))
    REPORT_AGENT_MAX_REFLECTION_ROUNDS = int(os.environ.get('REPORT_AGENT_MAX_REFLECTION_ROUNDS', '2'))
    REPORT_AGENT_TEMPERATURE = float(os.environ.get('REPORT_AGENT_TEMPERATURE', '0.5'))
    
    @classmethod
    def validate(cls):
        """验证必要配置"""
        errors = []
        if cls.GRAPH_BACKEND == 'ragflow':
            if not cls.RAGFLOW_API_KEY:
                errors.append("RAGFLOW_API_KEY 未配置（当前 GRAPH_BACKEND=ragflow）")
        elif cls.GRAPH_BACKEND == 'zep_local':
            if not cls.LLM_API_KEY:
                errors.append("LLM_API_KEY 未配置（当前 GRAPH_BACKEND=zep_local）")
            if not cls.NEO4J_PASSWORD:
                errors.append("NEO4J_PASSWORD 未配置（当前 GRAPH_BACKEND=zep_local）")
        else:
            if not cls.ZEP_API_KEY:
                errors.append("ZEP_API_KEY 未配置")
        return errors

