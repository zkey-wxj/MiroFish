"""
RAGflow图谱构建服务
使用RAGflow API构建知识图谱（Zep的替代方案）

RAGflow支持本地部署，通过知识图谱模式解析文档并提取实体和关系。
图谱数据构建后会缓存到本地，供后续模拟使用。

图谱ID格式: ragflow_{dataset_id}
"""

import os
import json
import uuid
import time
import threading
import tempfile
import shutil
from typing import Dict, Any, List, Optional, Callable

import requests

from ..config import Config
from ..models.task import TaskManager, TaskStatus
from ..utils.logger import get_logger

logger = get_logger('mirofish.ragflow_graph_builder')

# RAGflow图谱数据本地缓存目录
RAGFLOW_GRAPHS_DIR = os.path.join(os.path.dirname(__file__), '../../uploads/ragflow_graphs')


class RagflowGraphBuilderService:
    """
    RAGflow图谱构建服务

    工作流：
    1. 创建RAGflow数据集（知识库），使用knowledge_graph解析模式
    2. 上传文档（原始文件或将文本写成临时文件）
    3. 触发解析，RAGflow会自动提取实体和关系
    4. 轮询等待解析完成
    5. 从RAGflow获取知识图谱数据（实体和关系）
    6. 将结果缓存到本地JSON文件，供模拟读取使用
    """

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = (base_url or Config.RAGFLOW_BASE_URL or "http://localhost").rstrip('/')
        self.api_key = api_key or Config.RAGFLOW_API_KEY
        if not self.api_key:
            raise ValueError("RAGFLOW_API_KEY 未配置")

        self._auth_headers = {"Authorization": f"Bearer {self.api_key}"}
        self._json_headers = {**self._auth_headers, "Content-Type": "application/json"}
        self.task_manager = TaskManager()

        os.makedirs(RAGFLOW_GRAPHS_DIR, exist_ok=True)

    # ── HTTP请求工具 ──────────────────────────────────────────────────────────

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        resp = requests.get(self._api_url(path), headers=self._auth_headers,
                            params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: Optional[Dict] = None) -> Dict[str, Any]:
        resp = requests.post(self._api_url(path), headers=self._json_headers,
                             json=payload or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _post_files(self, path: str, files: Dict, data: Optional[Dict] = None) -> Dict[str, Any]:
        resp = requests.post(self._api_url(path), headers=self._auth_headers,
                             files=files, data=data or {}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str, payload: Optional[Dict] = None) -> Dict[str, Any]:
        resp = requests.delete(self._api_url(path), headers=self._json_headers,
                               json=payload or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _check_response(result: Dict, operation: str) -> Any:
        """检查RAGflow API返回码，非0时抛出异常"""
        if result.get("code", -1) != 0:
            raise ValueError(f"{operation}失败: {result.get('message', '未知错误')}")
        return result.get("data")

    # ── 数据集（知识库）管理 ──────────────────────────────────────────────────

    def create_dataset(self, name: str) -> str:
        """
        创建RAGflow数据集，使用knowledge_graph解析模式

        Returns:
            dataset_id
        """
        payload: Dict[str, Any] = {
            "name": name,
            "chunk_method": "knowledge_graph",
            "description": "MiroFish Knowledge Graph",
        }
        # 如果配置了RAGflow的LLM和Embedding，传入
        if Config.RAGFLOW_LLM_ID:
            payload["llm_id"] = Config.RAGFLOW_LLM_ID
        if Config.RAGFLOW_EMBEDDING_MODEL:
            payload["embedding_model"] = Config.RAGFLOW_EMBEDDING_MODEL

        result = self._post("/datasets", payload)
        data = self._check_response(result, "创建数据集")
        dataset_id = data["id"] if isinstance(data, dict) else data
        logger.info(f"RAGflow数据集已创建: {dataset_id}")
        return dataset_id

    def delete_dataset(self, dataset_id: str):
        """删除RAGflow数据集"""
        result = self._delete("/datasets", {"ids": [dataset_id]})
        self._check_response(result, "删除数据集")

    # ── 文档上传 ──────────────────────────────────────────────────────────────

    def upload_text_as_document(self, dataset_id: str, text: str,
                                filename: str = "document.txt") -> str:
        """将文本写成临时文件并上传到RAGflow数据集"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt',
                                        encoding='utf-8', delete=False) as f:
            f.write(text)
            tmp_path = f.name
        try:
            return self._upload_file(dataset_id, tmp_path, filename, "text/plain")
        finally:
            os.unlink(tmp_path)

    def upload_file_document(self, dataset_id: str, file_path: str) -> str:
        """上传已有文件到RAGflow数据集"""
        filename = os.path.basename(file_path)
        if filename.endswith('.pdf'):
            content_type = 'application/pdf'
        elif filename.endswith('.md') or filename.endswith('.markdown'):
            content_type = 'text/markdown'
        else:
            content_type = 'text/plain'
        return self._upload_file(dataset_id, file_path, filename, content_type)

    def _upload_file(self, dataset_id: str, file_path: str,
                     filename: str, content_type: str) -> str:
        with open(file_path, 'rb') as f:
            files = {"file": (filename, f, content_type)}
            result = self._post_files(f"/datasets/{dataset_id}/documents", files=files)
        data = self._check_response(result, "上传文档")
        docs = data if isinstance(data, list) else [data]
        if not docs:
            raise ValueError("上传成功但未返回文档信息")
        doc_id = docs[0]["id"]
        logger.info(f"文档已上传: {doc_id}")
        return doc_id

    # ── 文档解析 ──────────────────────────────────────────────────────────────

    def start_parsing(self, dataset_id: str, document_ids: List[str]):
        """触发文档解析（启动知识图谱构建）"""
        result = self._post(f"/datasets/{dataset_id}/chunks",
                            {"document_ids": document_ids})
        self._check_response(result, "启动解析")
        logger.info(f"解析已启动: dataset={dataset_id}, docs={document_ids}")

    def get_document_statuses(self, dataset_id: str) -> List[Dict[str, Any]]:
        """获取数据集中所有文档的状态"""
        result = self._get(f"/datasets/{dataset_id}/documents",
                           params={"page": 1, "page_size": 100})
        if result.get("code") == 0:
            return result.get("data", {}).get("docs", [])
        return []

    def wait_for_parsing(
        self,
        dataset_id: str,
        document_ids: List[str],
        progress_callback: Optional[Callable[[str, float], None]] = None,
        timeout: int = 600,
    ):
        """轮询等待文档解析完成（支持进度回调）"""
        start_time = time.time()
        pending = set(document_ids)
        total = len(document_ids)

        # RAGflow run状态: 0=未处理, 1=运行中, 2=完成, 3=失败
        done_states = {"DONE", "done", "2", 2}
        fail_states = {"FAIL", "fail", "3", 3, "CANCEL"}

        while pending:
            if time.time() - start_time > timeout:
                logger.warning(f"解析超时: 仍有 {len(pending)} 个文档未完成")
                break

            for doc in self.get_document_statuses(dataset_id):
                doc_id = doc.get("id")
                if doc_id not in pending:
                    continue
                run = doc.get("run", doc.get("status", ""))
                if run in done_states:
                    pending.discard(doc_id)
                    logger.info(f"文档解析完成: {doc_id}")
                elif run in fail_states:
                    pending.discard(doc_id)
                    logger.error(f"文档解析失败: {doc_id}, 状态: {run}")

            elapsed = int(time.time() - start_time)
            completed = total - len(pending)
            if progress_callback:
                ratio = completed / total if total else 1.0
                progress_callback(
                    f"RAGflow解析中... {completed}/{total} 完成 ({elapsed}秒)", ratio
                )

            if pending:
                time.sleep(5)

        if progress_callback:
            progress_callback("文档解析完成", 1.0)

    # ── 实体与图谱数据获取 ────────────────────────────────────────────────────

    def get_graph_entities(self, dataset_id: str) -> Dict[str, Any]:
        """
        从RAGflow获取知识图谱的实体和关系

        依次尝试：
        1. /datasets/{id}/graphs  —— RAGflow 新版图谱API
        2. 解析chunks并提取实体/关系类型的条目
        """
        # 方式1：专用图谱端点（RAGflow v0.15+）
        try:
            result = self._get(f"/datasets/{dataset_id}/graphs")
            if result.get("code") == 0:
                data = result.get("data", {})
                nodes = data.get("nodes", data.get("entities", []))
                edges = data.get("edges", data.get("relations", data.get("relationships", [])))
                if nodes or edges:
                    logger.info(f"通过图谱API获取到 {len(nodes)} 节点, {len(edges)} 边")
                    return {
                        "nodes": self._normalize_nodes(nodes),
                        "edges": self._normalize_edges(edges),
                    }
        except Exception as e:
            logger.debug(f"图谱专用API不可用，回退到chunks解析: {e}")

        # 方式2：从chunks中提取实体和关系
        nodes, edges = [], []
        try:
            page = 1
            while True:
                result = self._get(
                    f"/datasets/{dataset_id}/chunks",
                    params={"page": page, "page_size": 256},
                )
                if result.get("code") != 0:
                    break
                data = result.get("data", {})
                chunks = data.get("chunks", [])
                if not chunks:
                    break
                n, e = self._parse_kg_chunks(chunks)
                nodes.extend(n)
                edges.extend(e)
                if len(chunks) < 256:
                    break
                page += 1
            logger.info(f"从chunks解析到 {len(nodes)} 节点, {len(edges)} 边")
        except Exception as e:
            logger.warning(f"从chunks获取数据失败: {e}")

        return {"nodes": nodes, "edges": edges}

    def _normalize_nodes(self, nodes: List[Dict]) -> List[Dict]:
        """将RAGflow节点格式标准化为MiroFish格式"""
        result = []
        for n in nodes:
            nid = n.get("id", n.get("entity_id", str(uuid.uuid4())))
            name = n.get("name", n.get("entity_name", n.get("label", "")))
            entity_type = n.get("type", n.get("entity_type", "Entity"))
            result.append({
                "uuid": nid,
                "name": name,
                "labels": [entity_type, "Entity"] if entity_type != "Entity" else ["Entity"],
                "summary": n.get("description", n.get("summary", "")),
                "attributes": n.get("attributes", n.get("properties", {})),
            })
        return result

    def _normalize_edges(self, edges: List[Dict]) -> List[Dict]:
        """将RAGflow边格式标准化为MiroFish格式"""
        result = []
        for e in edges:
            eid = e.get("id", e.get("relation_id", str(uuid.uuid4())))
            result.append({
                "uuid": eid,
                "name": e.get("type", e.get("relation_type", e.get("label", ""))),
                "fact": e.get("description", e.get("fact", "")),
                "source_node_uuid": e.get("source_id", e.get("source", "")),
                "target_node_uuid": e.get("target_id", e.get("target", "")),
                "attributes": e.get("attributes", e.get("properties", {})),
            })
        return result

    def _parse_kg_chunks(self, chunks: List[Dict]) -> tuple:
        """从知识图谱chunks中解析实体和关系（chunks API回退方案）"""
        nodes: List[Dict] = []
        edges: List[Dict] = []

        for chunk in chunks:
            chunk_type = chunk.get("type", chunk.get("chunk_type", "")).lower()
            content = chunk.get("content", chunk.get("content_ltks", ""))
            cid = chunk.get("chunk_id", chunk.get("id", str(uuid.uuid4())))

            if chunk_type in ("entity", "node", "kg_entity"):
                name = chunk.get("entity_name", chunk.get("name", content[:80]))
                etype = chunk.get("entity_type", chunk.get("label", "Entity"))
                nodes.append({
                    "uuid": cid,
                    "name": name,
                    "labels": [etype, "Entity"] if etype != "Entity" else ["Entity"],
                    "summary": content,
                    "attributes": chunk.get("attributes", {}),
                })
            elif chunk_type in ("relation", "edge", "kg_relation", "relationship"):
                edges.append({
                    "uuid": cid,
                    "name": chunk.get("relation_name", chunk.get("type_", "")),
                    "fact": content,
                    "source_node_uuid": chunk.get("source_id", chunk.get("src_id", "")),
                    "target_node_uuid": chunk.get("target_id", chunk.get("tgt_id", "")),
                    "attributes": chunk.get("attributes", {}),
                })

        return nodes, edges

    # ── 本地缓存 ──────────────────────────────────────────────────────────────

    def save_graph_locally(self, graph_id: str,
                           nodes: List[Dict], edges: List[Dict]) -> str:
        """将图谱数据持久化到本地JSON文件"""
        graph_dir = os.path.join(RAGFLOW_GRAPHS_DIR, graph_id)
        os.makedirs(graph_dir, exist_ok=True)

        # 构建节点名称映射（用于边的补充信息）
        node_name_map = {n["uuid"]: n.get("name", "") for n in nodes}
        for edge in edges:
            edge.setdefault("source_node_name", node_name_map.get(edge.get("source_node_uuid", ""), ""))
            edge.setdefault("target_node_name", node_name_map.get(edge.get("target_node_uuid", ""), ""))

        graph_data = {
            "graph_id": graph_id,
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
        }
        graph_file = os.path.join(graph_dir, "graph_data.json")
        with open(graph_file, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, ensure_ascii=False, indent=2)
        logger.info(f"图谱数据已缓存: {graph_file} ({len(nodes)} 节点, {len(edges)} 边)")
        return graph_file

    # ── 公开接口（与GraphBuilderService保持兼容）──────────────────────────────

    def build_graph_async(
        self,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str = "MiroFish Graph",
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        batch_size: int = 3,
        file_paths: Optional[List[str]] = None,
    ) -> str:
        """
        异步构建知识图谱

        Args:
            text: 提取的文本（用于上传为单个文档）
            ontology: 本体定义（RAGflow不使用本体，但保留参数以保持接口兼容）
            graph_name: 图谱名称（用作RAGflow数据集名称）
            file_paths: 可选，直接上传原始文件（优先于text）

        Returns:
            task_id
        """
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={"graph_name": graph_name, "backend": "ragflow",
                      "text_length": len(text)},
        )
        thread = threading.Thread(
            target=self._build_graph_worker,
            args=(task_id, text, graph_name, file_paths),
            daemon=True,
        )
        thread.start()
        return task_id

    def _build_graph_worker(
        self,
        task_id: str,
        text: str,
        graph_name: str,
        file_paths: Optional[List[str]],
    ):
        """后台工作线程：调用RAGflow API完成图谱构建"""
        try:
            self.task_manager.update_task(
                task_id, status=TaskStatus.PROCESSING,
                progress=5, message="正在创建RAGflow数据集..."
            )

            # 1. 创建数据集
            dataset_id = self.create_dataset(graph_name)
            graph_id = f"ragflow_{dataset_id}"
            self.task_manager.update_task(
                task_id, progress=10,
                message=f"数据集已创建，正在上传文档..."
            )

            # 2. 上传文档
            document_ids: List[str] = []
            if file_paths:
                total_files = len(file_paths)
                for i, fp in enumerate(file_paths):
                    doc_id = self.upload_file_document(dataset_id, fp)
                    document_ids.append(doc_id)
                    progress = 10 + int((i + 1) / total_files * 20)
                    self.task_manager.update_task(
                        task_id, progress=progress,
                        message=f"已上传文件 {i + 1}/{total_files}"
                    )
            else:
                doc_id = self.upload_text_as_document(dataset_id, text, "document.txt")
                document_ids.append(doc_id)
                self.task_manager.update_task(task_id, progress=20, message="文档上传完成")

            # 3. 触发解析
            self.task_manager.update_task(
                task_id, progress=25,
                message="正在启动知识图谱解析..."
            )
            self.start_parsing(dataset_id, document_ids)

            # 4. 等待解析完成
            def wait_cb(msg: str, ratio: float):
                progress = 25 + int(ratio * 55)  # 25% → 80%
                self.task_manager.update_task(task_id, progress=progress, message=msg)

            self.wait_for_parsing(dataset_id, document_ids, wait_cb)

            # 5. 获取图谱数据
            self.task_manager.update_task(
                task_id, progress=82, message="正在获取知识图谱数据..."
            )
            graph_data = self.get_graph_entities(dataset_id)
            nodes = graph_data.get("nodes", [])
            edges = graph_data.get("edges", [])

            # 6. 本地缓存
            self.task_manager.update_task(
                task_id, progress=95, message="正在保存图谱数据..."
            )
            self.save_graph_locally(graph_id, nodes, edges)

            # 完成
            self.task_manager.complete_task(task_id, {
                "graph_id": graph_id,
                "dataset_id": dataset_id,
                "node_count": len(nodes),
                "edge_count": len(edges),
            })
            logger.info(f"RAGflow图谱构建完成: {graph_id}, 节点={len(nodes)}, 边={len(edges)}")

        except Exception as exc:
            import traceback
            error_msg = f"{str(exc)}\n{traceback.format_exc()}"
            logger.error(f"RAGflow图谱构建失败: {error_msg}")
            self.task_manager.fail_task(task_id, error_msg)

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """
        获取图谱的完整数据（节点和边）

        优先读取本地缓存，缓存不存在时从RAGflow实时获取。
        """
        graph_file = os.path.join(RAGFLOW_GRAPHS_DIR, graph_id, "graph_data.json")
        if os.path.exists(graph_file):
            with open(graph_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        # 本地缓存不存在，从RAGflow获取
        dataset_id = graph_id.removeprefix("ragflow_")
        graph_data = self.get_graph_entities(dataset_id)
        nodes = graph_data.get("nodes", [])
        edges = graph_data.get("edges", [])
        self.save_graph_locally(graph_id, nodes, edges)

        return {
            "graph_id": graph_id,
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
        }

    def delete_graph(self, graph_id: str):
        """删除RAGflow数据集及本地缓存"""
        dataset_id = graph_id.removeprefix("ragflow_")

        try:
            self.delete_dataset(dataset_id)
            logger.info(f"RAGflow数据集已删除: {dataset_id}")
        except Exception as exc:
            logger.warning(f"删除RAGflow数据集失败（可能已不存在）: {exc}")

        graph_dir = os.path.join(RAGFLOW_GRAPHS_DIR, graph_id)
        if os.path.exists(graph_dir):
            shutil.rmtree(graph_dir)
            logger.info(f"本地图谱缓存已删除: {graph_dir}")
