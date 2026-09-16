"""
Entity Reader and Filter Service
Reads nodes from the graph, filters by entity types.
Built on local KuzuDB graph storage.
"""

import time
from typing import Dict, Any, List, Optional, Set, Callable, TypeVar
from dataclasses import dataclass, field

from ..config import Config
from ..utils.logger import get_logger
from .graph_db import GraphDatabase

logger = get_logger('mirofish.entity_reader')

T = TypeVar('T')


@dataclass
class EntityNode:
    """Entity node data structure"""
    uuid: str
    name: str
    labels: List[str]
    summary: str
    attributes: Dict[str, Any]
    related_edges: List[Dict[str, Any]] = field(default_factory=list)
    related_nodes: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "labels": self.labels,
            "summary": self.summary,
            "attributes": self.attributes,
            "related_edges": self.related_edges,
            "related_nodes": self.related_nodes,
        }

    def get_entity_type(self) -> Optional[str]:
        """Get entity type (excluding default Entity label)"""
        for label in self.labels:
            if label not in ["Entity", "Node"]:
                return label
        return None


@dataclass
class FilteredEntities:
    """Filtered entity collection"""
    entities: List[EntityNode]
    entity_types: Set[str]
    total_count: int
    filtered_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entities": [e.to_dict() for e in self.entities],
            "entity_types": list(self.entity_types),
            "total_count": self.total_count,
            "filtered_count": self.filtered_count,
        }


class KuzuEntityReader:
    """
    Entity Reader and Filter Service

    Main functions:
    1. Read all nodes from the graph
    2. Filter nodes matching predefined entity types
    3. Get related edges and associated nodes for each entity
    """

    def __init__(self, api_key: Optional[str] = None):
        # api_key parameter kept for backward compatibility
        self.db = GraphDatabase()

    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        """
        Get all nodes from the graph.

        Args:
            graph_id: Graph ID

        Returns:
            List of node dicts
        """
        logger.info(f"Fetching all nodes from graph {graph_id}...")
        nodes = self.db.get_all_nodes(graph_id)

        nodes_data = []
        for node in nodes:
            nodes_data.append({
                "uuid": node.uuid_,
                "name": node.name,
                "labels": node.labels,
                "summary": node.summary,
                "attributes": node.attributes,
            })

        logger.info(f"Fetched {len(nodes_data)} nodes")
        return nodes_data

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        """
        Get all edges from the graph.

        Args:
            graph_id: Graph ID

        Returns:
            List of edge dicts
        """
        logger.info(f"Fetching all edges from graph {graph_id}...")
        edges = self.db.get_all_edges(graph_id)

        edges_data = []
        for edge in edges:
            edges_data.append({
                "uuid": edge.uuid_,
                "name": edge.name,
                "fact": edge.fact,
                "source_node_uuid": edge.source_node_uuid,
                "target_node_uuid": edge.target_node_uuid,
                "attributes": edge.attributes,
            })

        logger.info(f"Fetched {len(edges_data)} edges")
        return edges_data

    def get_node_edges(self, node_uuid: str, graph_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all edges connected to a node.

        Args:
            node_uuid: Node UUID
            graph_id: Graph ID (required for KuzuDB, attempts to find if not provided)

        Returns:
            List of edge dicts
        """
        if not graph_id:
            logger.warning(f"graph_id not provided for get_node_edges, searching available Kuzu graphs")
            return []

        try:
            edges = self.db.get_node_edges(graph_id, node_uuid)
            edges_data = []
            for edge in edges:
                edges_data.append({
                    "uuid": edge.uuid_,
                    "name": edge.name,
                    "fact": edge.fact,
                    "source_node_uuid": edge.source_node_uuid,
                    "target_node_uuid": edge.target_node_uuid,
                    "attributes": edge.attributes,
                })
            return edges_data
        except Exception as e:
            logger.warning(f"Failed to get edges for node {node_uuid}: {str(e)}")
            return []

    def filter_defined_entities(
        self,
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True,
        max_entities: Optional[int] = None,
    ) -> FilteredEntities:
        """
        Filter nodes matching predefined entity types.

        Filter logic:
        - Skip nodes with only "Entity" label (no specific type)
        - Keep nodes with labels beyond "Entity" and "Node"

        Args:
            graph_id: Graph ID
            defined_entity_types: Predefined entity types (optional, filters to these if provided)
            enrich_with_edges: Whether to include related edge information
            max_entities: Hard cap on returned entities (None = no cap). Also the
                effective agent count, so it is the memory dial — pass the
                caller's requested world size.

        Returns:
            FilteredEntities: Filtered entity collection
        """
        logger.info(f"Filtering entities in graph {graph_id}...")

        all_nodes = self.get_all_nodes(graph_id)
        total_count = len(all_nodes)

        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []
        node_map = {n["uuid"]: n for n in all_nodes}

        filtered_entities = []
        entity_types_found = set()

        for node in all_nodes:
            labels = node.get("labels", [])
            custom_labels = [l for l in labels if l not in ["Entity", "Node"]]

            if not custom_labels:
                continue

            if defined_entity_types:
                matching_labels = [l for l in custom_labels if l in defined_entity_types]
                if not matching_labels:
                    continue
                entity_type = matching_labels[0]
            else:
                entity_type = custom_labels[0]

            entity_types_found.add(entity_type)

            entity = EntityNode(
                uuid=node["uuid"],
                name=node["name"],
                labels=labels,
                summary=node["summary"],
                attributes=node["attributes"],
            )

            if enrich_with_edges:
                related_edges = []
                related_node_uuids = set()

                for edge in all_edges:
                    if edge["source_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "outgoing",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "target_node_uuid": edge["target_node_uuid"],
                        })
                        related_node_uuids.add(edge["target_node_uuid"])
                    elif edge["target_node_uuid"] == node["uuid"]:
                        related_edges.append({
                            "direction": "incoming",
                            "edge_name": edge["name"],
                            "fact": edge["fact"],
                            "source_node_uuid": edge["source_node_uuid"],
                        })
                        related_node_uuids.add(edge["source_node_uuid"])

                entity.related_edges = related_edges

                related_nodes = []
                for related_uuid in related_node_uuids:
                    if related_uuid in node_map:
                        related_node = node_map[related_uuid]
                        related_nodes.append({
                            "uuid": related_node["uuid"],
                            "name": related_node["name"],
                            "labels": related_node["labels"],
                            "summary": related_node.get("summary", ""),
                        })
                entity.related_nodes = related_nodes

            filtered_entities.append(entity)

        matched_count = len(filtered_entities)

        # A HARD cap on how many entities become agents. `target_entities` used to
        # be only a line in the expansion prompt ("name roughly N distinct
        # entities"), so the model routinely overshot — a request for 40 produced
        # 62 profiles / 64 nodes in production, and the run OOM-killed with
        # `Process exit code: -9`. One profile is generated per entity and each
        # platform holds an agent per entity, so the count is the memory dial;
        # asking nicely is not a control. Deterministic (graph order) so a retry
        # of the same run selects the same population.
        if max_entities is not None and max_entities > 0 and matched_count > max_entities:
            logger.info(
                f"Capping entities at {max_entities} (graph matched {matched_count}); "
                f"the rest are excluded from this simulation."
            )
            filtered_entities = filtered_entities[:max_entities]

        logger.info(f"Filtering complete: {total_count} total nodes, "
                   f"{matched_count} matched, {len(filtered_entities)} selected, "
                   f"types: {entity_types_found}")

        return FilteredEntities(
            entities=filtered_entities,
            entity_types=entity_types_found,
            total_count=total_count,
            filtered_count=len(filtered_entities),
        )

    def get_entity_with_context(
        self,
        graph_id: str,
        entity_uuid: str
    ) -> Optional[EntityNode]:
        """
        Get a single entity with full context (edges and related nodes).

        Args:
            graph_id: Graph ID
            entity_uuid: Entity UUID

        Returns:
            EntityNode or None
        """
        try:
            node = self.db.get_node(graph_id, entity_uuid)
            if not node:
                return None

            edges = self.db.get_node_edges(graph_id, entity_uuid)
            all_nodes = self.get_all_nodes(graph_id)
            node_map = {n["uuid"]: n for n in all_nodes}

            related_edges = []
            related_node_uuids = set()

            for edge in edges:
                if edge.source_node_uuid == entity_uuid:
                    related_edges.append({
                        "direction": "outgoing",
                        "edge_name": edge.name,
                        "fact": edge.fact,
                        "target_node_uuid": edge.target_node_uuid,
                    })
                    related_node_uuids.add(edge.target_node_uuid)
                else:
                    related_edges.append({
                        "direction": "incoming",
                        "edge_name": edge.name,
                        "fact": edge.fact,
                        "source_node_uuid": edge.source_node_uuid,
                    })
                    related_node_uuids.add(edge.source_node_uuid)

            related_nodes = []
            for related_uuid in related_node_uuids:
                if related_uuid in node_map:
                    rn = node_map[related_uuid]
                    related_nodes.append({
                        "uuid": rn["uuid"],
                        "name": rn["name"],
                        "labels": rn["labels"],
                        "summary": rn.get("summary", ""),
                    })

            return EntityNode(
                uuid=node.uuid_,
                name=node.name,
                labels=node.labels,
                summary=node.summary,
                attributes=node.attributes,
                related_edges=related_edges,
                related_nodes=related_nodes,
            )

        except Exception as e:
            logger.error(f"Failed to get entity {entity_uuid}: {str(e)}")
            return None

    def get_entities_by_type(
        self,
        graph_id: str,
        entity_type: str,
        enrich_with_edges: bool = True
    ) -> List[EntityNode]:
        """
        Get all entities of a specific type.

        Args:
            graph_id: Graph ID
            entity_type: Entity type (e.g., "Student", "PublicFigure")
            enrich_with_edges: Whether to include related edge info

        Returns:
            List of entities
        """
        result = self.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=[entity_type],
            enrich_with_edges=enrich_with_edges
        )
        return result.entities
