"""
RAGflow实体读取服务
从本地缓存的RAGflow知识图谱中读取实体，返回与ZepEntityReader相同格式的数据

RAGflow图谱数据在构建时已保存到本地JSON文件，本服务直接读取该缓存。
接口设计与ZepEntityReader保持一致，SimulationManager可无缝切换。
"""

import os
import json
from typing import Dict, Any, List, Optional, Set

from ..utils.logger import get_logger
from .zep_entity_reader import EntityNode, FilteredEntities

logger = get_logger('mirofish.ragflow_entity_reader')

RAGFLOW_GRAPHS_DIR = os.path.join(os.path.dirname(__file__), '../../uploads/ragflow_graphs')


class RagflowEntityReader:
    """
    RAGflow实体读取与过滤服务

    从本地缓存的RAGflow知识图谱JSON文件中读取实体和关系，
    并以与ZepEntityReader相同的接口返回FilteredEntities对象，
    使SimulationManager可以不加修改地使用RAGflow图谱进行模拟。
    """

    def _load_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """从本地缓存加载图谱数据"""
        graph_file = os.path.join(RAGFLOW_GRAPHS_DIR, graph_id, "graph_data.json")
        if not os.path.exists(graph_file):
            logger.error(f"RAGflow图谱缓存不存在: {graph_id}，请确认图谱已成功构建（/graph/build）")
            raise FileNotFoundError(
                f"RAGflow图谱缓存不存在: {graph_id}。"
                "请确认图谱已成功构建（/graph/build）。"
            )
        with open(graph_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取图谱所有节点"""
        return self._load_graph_data(graph_id).get("nodes", [])

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        """获取图谱所有边"""
        return self._load_graph_data(graph_id).get("edges", [])

    def get_node_edges(self, node_uuid: str, graph_id: str) -> List[Dict[str, Any]]:
        """获取指定节点的所有相关边（从本地缓存读取）"""
        try:
            all_edges = self.get_all_edges(graph_id)
            return [
                e for e in all_edges
                if e.get("source_node_uuid") == node_uuid
                or e.get("target_node_uuid") == node_uuid
            ]
        except Exception as e:
            logger.warning(f"获取节点 {node_uuid} 的边失败: {str(e)}")
            return []

    def filter_defined_entities(
        self,
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True,
    ) -> FilteredEntities:
        """
        筛选符合预定义实体类型的节点（与ZepEntityReader.filter_defined_entities接口一致）

        筛选逻辑与ZepEntityReader相同：
        - 节点labels中除"Entity"/"Node"外还有其他标签，才认为是有效实体
        - 如果指定了defined_entity_types，只保留匹配类型的实体

        Args:
            graph_id: RAGflow图谱ID（格式: ragflow_{dataset_id}）
            defined_entity_types: 预定义实体类型列表（来自本体定义）
            enrich_with_edges: 是否为实体附加关联边和节点信息

        Returns:
            FilteredEntities
        """
        logger.info(f"从RAGflow图谱读取实体: {graph_id}")

        all_nodes = self.get_all_nodes(graph_id)
        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []

        total_count = len(all_nodes)
        node_map = {n["uuid"]: n for n in all_nodes}

        filtered_entities: List[EntityNode] = []
        entity_types_found: Set[str] = set()

        for node in all_nodes:
            labels = node.get("labels", [])
            custom_labels = [label for label in labels if label not in ("Entity", "Node")]

            if not custom_labels:
                # 没有自定义标签，跳过
                continue

            if defined_entity_types:
                matching = [l for l in custom_labels if l in defined_entity_types]
                if not matching:
                    continue
                entity_type = matching[0]
            else:
                entity_type = custom_labels[0]

            entity_types_found.add(entity_type)

            entity = EntityNode(
                uuid=node["uuid"],
                name=node.get("name", ""),
                labels=labels,
                summary=node.get("summary", ""),
                attributes=node.get("attributes", {}),
            )

            if enrich_with_edges:
                related_edges: List[Dict] = []
                related_node_uuids: Set[str] = set()

                for edge in all_edges:
                    src = edge.get("source_node_uuid", "")
                    tgt = edge.get("target_node_uuid", "")

                    if src == node["uuid"]:
                        related_edges.append({
                            "direction": "outgoing",
                            "edge_name": edge.get("name", ""),
                            "fact": edge.get("fact", ""),
                            "target_node_uuid": tgt,
                        })
                        related_node_uuids.add(tgt)
                    elif tgt == node["uuid"]:
                        related_edges.append({
                            "direction": "incoming",
                            "edge_name": edge.get("name", ""),
                            "fact": edge.get("fact", ""),
                            "source_node_uuid": src,
                        })
                        related_node_uuids.add(src)

                entity.related_edges = related_edges

                related_nodes = []
                for rel_uuid in related_node_uuids:
                    if rel_uuid in node_map:
                        rn = node_map[rel_uuid]
                        related_nodes.append({
                            "uuid": rn["uuid"],
                            "name": rn.get("name", ""),
                            "labels": rn.get("labels", []),
                            "summary": rn.get("summary", ""),
                        })
                entity.related_nodes = related_nodes

            filtered_entities.append(entity)

        logger.info(
            f"RAGflow实体筛选完成: 总节点={total_count}, "
            f"符合条件={len(filtered_entities)}, 类型={entity_types_found}"
        )

        return FilteredEntities(
            entities=filtered_entities,
            entity_types=entity_types_found,
            total_count=total_count,
            filtered_count=len(filtered_entities),
        )

    def get_entity_with_context(
        self, graph_id: str, entity_uuid: str
    ) -> Optional[EntityNode]:
        """获取单个实体及其完整上下文（从本地缓存读取）"""
        try:
            all_nodes = self.get_all_nodes(graph_id)
            all_edges = self.get_all_edges(graph_id)
            node_map = {n["uuid"]: n for n in all_nodes}
            node = node_map.get(entity_uuid)
            if not node:
                return None
            related_edges, related_node_uuids = [], set()
            for edge in all_edges:
                src, tgt = edge.get("source_node_uuid", ""), edge.get("target_node_uuid", "")
                if src == entity_uuid:
                    related_edges.append({"direction": "outgoing", "edge_name": edge.get("name", ""),
                                           "fact": edge.get("fact", ""), "target_node_uuid": tgt})
                    related_node_uuids.add(tgt)
                elif tgt == entity_uuid:
                    related_edges.append({"direction": "incoming", "edge_name": edge.get("name", ""),
                                           "fact": edge.get("fact", ""), "source_node_uuid": src})
                    related_node_uuids.add(src)
            related_nodes = [
                {"uuid": node_map[u]["uuid"], "name": node_map[u].get("name", ""),
                 "labels": node_map[u].get("labels", []), "summary": node_map[u].get("summary", "")}
                for u in related_node_uuids if u in node_map
            ]
            return EntityNode(
                uuid=node["uuid"], name=node.get("name", ""), labels=node.get("labels", []),
                summary=node.get("summary", ""), attributes=node.get("attributes", {}),
                related_edges=related_edges, related_nodes=related_nodes,
            )
        except Exception as e:
            logger.error(f"获取RAGflow实体 {entity_uuid} 失败: {str(e)}")
            return None

    def get_entities_by_type(
        self, graph_id: str, entity_type: str, enrich_with_edges: bool = True
    ) -> List[EntityNode]:
        """获取指定类型的所有实体"""
        return self.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=[entity_type],
            enrich_with_edges=enrich_with_edges,
        ).entities
