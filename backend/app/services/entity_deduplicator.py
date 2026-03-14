"""
实体去重服务
图谱构建完成后，识别并合并指向同一现实实体的重复节点

典型场景：
  - "特朗普" 与 "美国总统特朗普" 被 Zep 识别为两个不同节点
  - 本服务通过 LLM 判断它们是否指向同一实体，并自动合并
"""

import json
import time
from typing import Dict, Any, List, Optional, Callable, Tuple
from dataclasses import dataclass, field

import httpx
from zep_cloud.client import Zep

from ..config import Config
from ..utils.llm_client import LLMClient
from ..utils.logger import get_logger
from ..utils.zep_paging import fetch_all_nodes, fetch_all_edges

ZEP_API_BASE = "https://api.getzep.com/api/v2"

logger = get_logger('mirofish.entity_deduplicator')


DEDUP_SYSTEM_PROMPT = """你是一个实体消歧专家。你的任务是从一组知识图谱节点中，识别出指向同一现实世界实体的重复节点。

**重要：你必须输出有效的JSON格式数据，不要输出任何其他内容。**

## 判断标准

两个节点应被判定为"同一实体"当且仅当：
- 它们指向现实世界中完全相同的人、组织或事物
- 仅仅是称呼不同（如全名 vs 简称、带头衔 vs 不带头衔）
- 例如："特朗普"和"美国总统特朗普"是同一个人

## 硬性规则（必须严格遵守）

1. **类型必须一致**：人物只能与人物合并，组织只能与组织合并，地点只能与地点合并。绝对不允许跨类型合并。
2. **上下级关系不合并**：国务院≠领事馆，总部≠分公司，部门≠下属机构。它们是不同实体。
3. **关联关系不合并**：某人在某组织任职，不代表这个人和组织是同一实体。
4. **信息来源不是实体**：新闻媒体、数据平台等信息来源与它们报道的实体不是同一事物。
5. **宁可漏判，不可误判**：如果不确定两个节点是否为同一实体，就不要合并。

## 反例（以下情况绝对不应合并）

- "丹凯恩斯将军" 与 "美国参谋长联席会议" → 不合并（人物 vs 组织）
- "美国国务院" 与 "驻土耳其阿达纳总领馆" → 不合并（上下级机构）
- "伊朗驻华大使法兹里" 与 "金十数据" → 不合并（外交官 vs 财经平台）
- "霍尔木兹海峡" 与 "美国" → 不合并（地理位置 vs 国家）
- "新华网" 与 "新华社" → 不合并（网站 vs 通讯社，虽有关联但是不同实体）

## 正例（以下情况应该合并）

- "特朗普" 与 "美国总统特朗普" → 合并（同一个人，简称 vs 带头衔）
- "阿拉格齐" 与 "伊朗外交部长阿拉格齐" → 合并（同一个人，简称 vs 全称+头衔），canonical_name 应为 "阿拉格齐"
- "伊朗革命卫队" 与 "伊朗伊斯兰革命卫队" → 合并（同一组织，简称 vs 全称）

## 输出格式

```json
{
    "duplicate_groups": [
        {
            "canonical_name": "应保留的标准名称（选择最简洁常用的）",
            "members": [
                {"uuid": "节点uuid", "name": "节点名称"}
            ],
            "reason": "合并理由（简短）"
        }
    ]
}
```

规则：
- 每个 duplicate_group 至少包含 2 个 members
- canonical_name 应选择最常用、最简洁、辨识度最高的名称（如"特朗普"优于"美国总统特朗普"）
- 如果没有发现任何重复，返回 `{"duplicate_groups": []}`
"""

DEDUP_BATCH_SIZE = 80
NAME_JACCARD_THRESHOLD = 0.5


@dataclass
class MergeAction:
    """单次合并操作的记录"""
    group_canonical_name: str
    keep_node_uuid: str
    keep_node_name: str
    removed_nodes: List[Dict[str, str]]
    edges_migrated: int
    reason: str


@dataclass
class DeduplicationReport:
    """去重执行报告"""
    graph_id: str
    total_nodes_before: int
    total_nodes_after: int
    groups_found: int
    nodes_removed: int
    edges_migrated: int
    merge_actions: List[MergeAction] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "total_nodes_before": self.total_nodes_before,
            "total_nodes_after": self.total_nodes_after,
            "groups_found": self.groups_found,
            "nodes_removed": self.nodes_removed,
            "edges_migrated": self.edges_migrated,
            "merge_actions": [
                {
                    "canonical_name": a.group_canonical_name,
                    "keep_node": {"uuid": a.keep_node_uuid, "name": a.keep_node_name},
                    "removed_nodes": a.removed_nodes,
                    "edges_migrated": a.edges_migrated,
                    "reason": a.reason,
                }
                for a in self.merge_actions
            ],
            "errors": self.errors,
        }


class EntityDeduplicator:
    """
    实体去重服务

    工作流程：
    1. 从 Zep 获取图谱中所有节点
    2. 将节点名称列表发送给 LLM，识别重复组
    3. 对每个重复组，选择主节点并合并其余节点的边和摘要
    4. 删除多余节点
    """

    def __init__(
        self,
        zep_api_key: Optional[str] = None,
        llm_client: Optional[LLMClient] = None,
    ):
        self.zep_api_key = zep_api_key or Config.ZEP_API_KEY
        if not self.zep_api_key:
            raise ValueError("ZEP_API_KEY 未配置")

        self.zep_client = Zep(api_key=self.zep_api_key)
        self.llm_client = llm_client or LLMClient()

        self._http = httpx.Client(
            base_url=ZEP_API_BASE,
            headers={"Authorization": f"Api-Key {self.zep_api_key}"},
            timeout=60.0,
        )

    def deduplicate(
        self,
        graph_id: str,
        dry_run: bool = False,
        progress_callback: Optional[Callable[[str, float], None]] = None,
    ) -> DeduplicationReport:
        """
        对指定图谱执行实体去重

        Args:
            graph_id: Zep 图谱 ID
            dry_run: 仅检测重复但不实际合并（用于预览）
            progress_callback: 进度回调 (message, progress_0_to_1)

        Returns:
            DeduplicationReport
        """
        def _progress(msg: str, pct: float):
            logger.info(f"[dedup] {msg}")
            if progress_callback:
                progress_callback(msg, pct)

        _progress("读取图谱节点...", 0.0)
        nodes = fetch_all_nodes(self.zep_client, graph_id)
        total_before = len(nodes)

        if total_before < 2:
            _progress("节点数不足，无需去重", 1.0)
            return DeduplicationReport(
                graph_id=graph_id,
                total_nodes_before=total_before,
                total_nodes_after=total_before,
                groups_found=0,
                nodes_removed=0,
                edges_migrated=0,
            )

        node_list = []
        for n in nodes:
            uuid = getattr(n, 'uuid_', None) or getattr(n, 'uuid', '')
            name = n.name or ""
            labels = n.labels or []
            summary = n.summary or ""
            node_list.append({
                "uuid": uuid,
                "name": name,
                "labels": labels,
                "summary": summary,
            })

        _progress(f"共 {total_before} 个节点，开始 LLM 去重识别...", 0.1)
        duplicate_groups = self._find_duplicates(node_list)
        groups_found = len(duplicate_groups)

        if groups_found == 0:
            _progress("未发现重复实体", 1.0)
            return DeduplicationReport(
                graph_id=graph_id,
                total_nodes_before=total_before,
                total_nodes_after=total_before,
                groups_found=0,
                nodes_removed=0,
                edges_migrated=0,
            )

        _progress(f"发现 {groups_found} 组重复实体", 0.3)

        if dry_run:
            actions = []
            total_removable = 0
            for g in duplicate_groups:
                members = g["members"]
                keep = members[0]
                removable = members[1:]
                total_removable += len(removable)
                actions.append(MergeAction(
                    group_canonical_name=g["canonical_name"],
                    keep_node_uuid=keep["uuid"],
                    keep_node_name=keep["name"],
                    removed_nodes=[{"uuid": r["uuid"], "name": r["name"]} for r in removable],
                    edges_migrated=0,
                    reason=g.get("reason", ""),
                ))
            _progress(f"Dry-run 完成：可合并 {total_removable} 个重复节点", 1.0)
            return DeduplicationReport(
                graph_id=graph_id,
                total_nodes_before=total_before,
                total_nodes_after=total_before - total_removable,
                groups_found=groups_found,
                nodes_removed=0,
                edges_migrated=0,
                merge_actions=actions,
            )

        _progress("开始合并重复节点...", 0.4)
        report = DeduplicationReport(
            graph_id=graph_id,
            total_nodes_before=total_before,
            total_nodes_after=total_before,
            groups_found=groups_found,
            nodes_removed=0,
            edges_migrated=0,
        )

        node_map = {n["uuid"]: n for n in node_list}

        for idx, group in enumerate(duplicate_groups):
            group_progress = 0.4 + 0.55 * (idx / groups_found)
            canonical = group["canonical_name"]
            members = group["members"]
            reason = group.get("reason", "")

            if len(members) < 2:
                continue

            valid_members = [m for m in members if m["uuid"] in node_map]
            if len(valid_members) < 2:
                continue

            keep_node = self._pick_primary_node(valid_members, node_map, canonical)
            dup_nodes = [m for m in valid_members if m["uuid"] != keep_node["uuid"]]

            _progress(
                f"合并组 [{canonical}]: 保留 '{keep_node['name']}'，"
                f"删除 {len(dup_nodes)} 个重复节点",
                group_progress,
            )

            edges_migrated = 0
            removed = []

            for dup in dup_nodes:
                try:
                    migrated = self._merge_node_into(
                        graph_id, keep_node["uuid"], dup["uuid"], node_map
                    )
                    edges_migrated += migrated
                    removed.append({"uuid": dup["uuid"], "name": dup["name"]})
                except Exception as e:
                    err_msg = f"合并节点 '{dup['name']}' 失败: {str(e)}"
                    logger.error(err_msg)
                    report.errors.append(err_msg)

            self._update_primary_node(keep_node["uuid"], canonical, valid_members, node_map)

            report.merge_actions.append(MergeAction(
                group_canonical_name=canonical,
                keep_node_uuid=keep_node["uuid"],
                keep_node_name=keep_node["name"],
                removed_nodes=removed,
                edges_migrated=edges_migrated,
                reason=reason,
            ))
            report.nodes_removed += len(removed)
            report.edges_migrated += edges_migrated

            for r in removed:
                node_map.pop(r["uuid"], None)

        report.total_nodes_after = total_before - report.nodes_removed
        _progress(
            f"去重完成：合并 {report.groups_found} 组，"
            f"删除 {report.nodes_removed} 个节点，"
            f"迁移 {report.edges_migrated} 条边",
            1.0,
        )
        return report

    # ------------------------------------------------------------------
    # 名称相似度 & 类型兼容性 预筛选
    # ------------------------------------------------------------------

    @staticmethod
    def _labels_compatible(labels_a: List[str], labels_b: List[str]) -> bool:
        """两个节点的类型标签是否兼容（至少有一个共同标签）"""
        if not labels_a or not labels_b:
            return True
        return bool(set(labels_a) & set(labels_b))

    @staticmethod
    def _name_similar(name_a: str, name_b: str) -> bool:
        """两个名称是否足够相似，可作为候选重复对"""
        a = name_a.strip()
        b = name_b.strip()
        if not a or not b:
            return False
        if a == b:
            return True
        if a in b or b in a:
            return True
        chars_a = set(a)
        chars_b = set(b)
        union = chars_a | chars_b
        if not union:
            return False
        jaccard = len(chars_a & chars_b) / len(union)
        return jaccard >= NAME_JACCARD_THRESHOLD

    def _build_candidate_clusters(
        self, node_list: List[Dict[str, Any]]
    ) -> List[List[Dict[str, Any]]]:
        """
        预筛选：按名称相似度 + 类型兼容性将节点聚类。
        只有名称相似且类型兼容的节点才会被放入同一候选簇。
        使用 union-find 算法构建连通分量。
        """
        n = len(node_list)
        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: int, y: int):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        for i in range(n):
            for j in range(i + 1, n):
                if not self._labels_compatible(
                    node_list[i]["labels"], node_list[j]["labels"]
                ):
                    continue
                if self._name_similar(node_list[i]["name"], node_list[j]["name"]):
                    union(i, j)

        clusters: Dict[int, List[Dict[str, Any]]] = {}
        for i in range(n):
            root = find(i)
            clusters.setdefault(root, []).append(node_list[i])

        return [c for c in clusters.values() if len(c) >= 2]

    # ------------------------------------------------------------------
    # LLM 重复检测
    # ------------------------------------------------------------------

    def _find_duplicates(
        self, node_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        先用名称相似度 + 类型兼容性预筛选，再逐簇调用 LLM 确认。
        这样 LLM 只处理预筛选后的候选节点，避免在无关节点间产生误判。
        """
        clusters = self._build_candidate_clusters(node_list)
        if not clusters:
            logger.info("[dedup] 名称相似度预筛选: 未发现候选重复节点")
            return []

        candidate_count = sum(len(c) for c in clusters)
        logger.info(
            f"[dedup] 名称相似度预筛选: "
            f"{len(clusters)} 组候选 ({candidate_count} 个节点，"
            f"从 {len(node_list)} 个节点中筛出)"
        )

        all_groups: List[Dict[str, Any]] = []
        for cluster in clusters:
            groups = self._find_duplicates_single_batch(cluster)
            all_groups.extend(groups)

        return all_groups

    def _find_duplicates_single_batch(
        self, node_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """对一组候选节点调用 LLM 确认是否为同一实体"""
        nodes_desc = "\n".join(
            f"- uuid: {n['uuid']}  |  name: {n['name']}  "
            f"|  labels: {', '.join(n['labels'])}  "
            f"|  summary: {(n.get('summary') or '')[:100]}"
            for n in node_list
        )

        user_message = (
            f"以下是知识图谱中一组名称相似的实体节点，请判断其中是否有"
            f"指向同一现实实体的重复节点：\n\n"
            f"{nodes_desc}\n\n"
            f"请严格按要求的JSON格式返回结果。"
            f"注意：名称相似不等于是同一实体，请仔细分析 labels 和 summary。"
            f"如果这些节点都不是重复的，返回 {{\"duplicate_groups\": []}}"
        )

        messages = [
            {"role": "system", "content": DEDUP_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]

        try:
            result = self.llm_client.chat_json(
                messages=messages, temperature=0.1, max_tokens=4096
            )
            groups = result.get("duplicate_groups", [])
            return self._validate_groups(groups, node_list)
        except Exception as e:
            logger.error(f"LLM 去重识别失败: {e}")
            return []

    def _validate_groups(
        self,
        groups: List[Dict[str, Any]],
        node_list: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """校验 LLM 返回的分组：过滤无效数据 + 类型一致性二次检查"""
        valid_uuids = {n["uuid"] for n in node_list}
        uuid_to_labels = {n["uuid"]: n.get("labels", []) for n in node_list}
        validated = []

        for g in groups:
            if not isinstance(g, dict):
                continue
            members = g.get("members", [])
            if not isinstance(members, list) or len(members) < 2:
                continue

            valid_members = []
            for m in members:
                if isinstance(m, dict) and m.get("uuid") in valid_uuids:
                    valid_members.append(m)

            if len(valid_members) < 2:
                continue

            # 类型一致性检查：以第一个成员的类型为基准，过滤类型不兼容的成员
            base_labels = set(uuid_to_labels.get(valid_members[0]["uuid"], []))
            if base_labels:
                type_checked = [valid_members[0]]
                for m in valid_members[1:]:
                    m_labels = set(uuid_to_labels.get(m["uuid"], []))
                    if not m_labels or (m_labels & base_labels):
                        type_checked.append(m)
                    else:
                        logger.warning(
                            f"[dedup] 类型不一致，拒绝合并: "
                            f"'{m.get('name')}' ({list(m_labels)}) vs "
                            f"'{valid_members[0].get('name')}' ({list(base_labels)})"
                        )
                valid_members = type_checked

            if len(valid_members) < 2:
                continue

            seen_uuids: set = set()
            unique_members = []
            for m in valid_members:
                if m["uuid"] not in seen_uuids:
                    seen_uuids.add(m["uuid"])
                    unique_members.append(m)

            if len(unique_members) >= 2:
                validated.append({
                    "canonical_name": g.get("canonical_name", unique_members[0]["name"]),
                    "members": unique_members,
                    "reason": g.get("reason", ""),
                })

        return validated

    # ------------------------------------------------------------------
    # 节点合并操作
    # ------------------------------------------------------------------

    def _pick_primary_node(
        self,
        members: List[Dict[str, str]],
        node_map: Dict[str, Dict[str, Any]],
        canonical_name: str,
    ) -> Dict[str, str]:
        """
        选择主节点（保留节点）

        优先级：
        1. 名称与 canonical_name 完全匹配的节点
        2. summary 最长的节点（信息最丰富）
        3. 列表中的第一个
        """
        for m in members:
            if m["name"] == canonical_name:
                return m

        best = members[0]
        best_len = len(node_map.get(best["uuid"], {}).get("summary", ""))
        for m in members[1:]:
            s_len = len(node_map.get(m["uuid"], {}).get("summary", ""))
            if s_len > best_len:
                best = m
                best_len = s_len
        return best

    def _merge_node_into(
        self,
        graph_id: str,
        keep_uuid: str,
        remove_uuid: str,
        node_map: Dict[str, Dict[str, Any]],
    ) -> int:
        """
        将 remove_uuid 节点的边迁移到 keep_uuid，然后删除 remove_uuid

        Returns:
            迁移的边数量
        """
        try:
            edges = self.zep_client.graph.node.get_edges(node_uuid=remove_uuid)
        except Exception as e:
            logger.warning(f"获取节点 {remove_uuid} 的边失败: {e}")
            edges = []

        migrated = 0
        keep_name = node_map.get(keep_uuid, {}).get("name", "")
        old_edge_uuids: List[str] = []

        for edge in edges:
            source_uuid = edge.source_node_uuid
            target_uuid = edge.target_node_uuid
            fact = edge.fact or ""
            edge_name = edge.name or ""
            old_edge_uuids.append(edge.uuid_)

            if source_uuid == remove_uuid:
                other_uuid = target_uuid
            else:
                other_uuid = source_uuid

            if other_uuid == keep_uuid:
                continue

            other_name = node_map.get(other_uuid, {}).get("name", "")
            if not other_name:
                continue

            if source_uuid == remove_uuid:
                src_name, tgt_name = keep_name, other_name
            else:
                src_name, tgt_name = other_name, keep_name

            try:
                self.zep_client.graph.add_fact_triple(
                    graph_id=graph_id,
                    fact=fact if fact else f"{src_name} {edge_name} {tgt_name}",
                    fact_name=edge_name,
                    source_node_name=src_name,
                    target_node_name=tgt_name,
                )
                migrated += 1
                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"迁移边 '{edge_name}' 失败: {e}")

        self._remove_node(remove_uuid, old_edge_uuids)
        return migrated

    def _remove_node(self, node_uuid: str, edge_uuids: List[str]):
        """
        删除节点：先尝试直接 HTTP DELETE，失败则降级为删除所有关联边（节点变为孤立节点）
        """
        try:
            resp = self._http.delete(f"graph/node/{node_uuid}")
            resp.raise_for_status()
            logger.info(f"已通过 HTTP API 删除节点 {node_uuid}")
            return
        except Exception as e:
            logger.warning(f"HTTP 删除节点 {node_uuid} 失败 ({e})，降级为删除关联边")

        deleted_edges = 0
        for eu in edge_uuids:
            try:
                self.zep_client.graph.edge.delete(uuid_=eu)
                deleted_edges += 1
                time.sleep(0.2)
            except Exception as e:
                logger.warning(f"删除边 {eu} 失败: {e}")

        logger.info(f"已删除节点 {node_uuid} 的 {deleted_edges}/{len(edge_uuids)} 条边（节点变为孤立节点）")

    def _update_primary_node(
        self,
        keep_uuid: str,
        canonical_name: str,
        all_members: List[Dict[str, str]],
        node_map: Dict[str, Dict[str, Any]],
    ):
        """更新主节点：合并所有成员的 summary，统一名称"""
        summaries = []
        for m in all_members:
            s = node_map.get(m["uuid"], {}).get("summary", "")
            if s:
                summaries.append(s)

        merged_summary = "\n\n".join(dict.fromkeys(summaries))

        update_body: Dict[str, Any] = {}
        current_name = node_map.get(keep_uuid, {}).get("name", "")
        if current_name != canonical_name:
            update_body["name"] = canonical_name
        if merged_summary:
            update_body["summary"] = merged_summary

        if not update_body:
            return

        try:
            resp = self._http.patch(
                f"graph/node/{keep_uuid}",
                json=update_body,
            )
            resp.raise_for_status()
            logger.info(f"已更新主节点 {keep_uuid} 名称/摘要")
        except Exception as e:
            logger.warning(f"更新主节点 {keep_uuid} 失败 (HTTP PATCH): {e}，跳过名称/摘要更新")
