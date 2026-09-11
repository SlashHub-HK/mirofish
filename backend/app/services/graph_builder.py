"""
Graph Builder Service
Builds knowledge graphs using KuzuDB (embedded) + LLM-based entity extraction.
"""

import os
import uuid
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass

from ..config import Config
from ..models.task import TaskManager, TaskStatus
from .text_processor import TextProcessor
from .graph_db import GraphDatabase, GraphNode
from .entity_extractor import EntityExtractor
from ..utils.logger import get_logger

logger = get_logger('mirofish.graph_builder')

# Global registry of active builds - used to signal stop
_active_builds = {}  # task_id -> {"stop": False}


def request_stop(task_id: str):
    """Request a running build to stop."""
    if task_id in _active_builds:
        _active_builds[task_id]["stop"] = True
        return True
    return False


def is_build_active(task_id: str) -> bool:
    return task_id in _active_builds


@dataclass
class GraphInfo:
    """Graph information"""
    graph_id: str
    node_count: int
    edge_count: int
    entity_types: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "entity_types": self.entity_types,
        }


class GraphBuilderService:
    """
    Graph Builder Service
    Uses KuzuDB for storage and LLM for entity extraction.
    """

    def __init__(self, api_key: Optional[str] = None):
        # api_key parameter kept for backward compatibility from earlier graph backends
        self.db = GraphDatabase()
        self.extractor = EntityExtractor()
        self.task_manager = TaskManager()

    def build_graph_async(
        self,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str = "MiroFish Graph",
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        batch_size: int = 3
    ) -> str:
        """
        Build graph asynchronously.

        Args:
            text: Input text
            ontology: Ontology definition (from ontology generator)
            graph_name: Graph name
            chunk_size: Text chunk size
            chunk_overlap: Chunk overlap size
            batch_size: Chunks per batch for extraction

        Returns:
            Task ID
        """
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={
                "graph_name": graph_name,
                "chunk_size": chunk_size,
                "text_length": len(text),
            }
        )

        thread = threading.Thread(
            target=self._build_graph_worker,
            args=(task_id, text, ontology, graph_name, chunk_size, chunk_overlap, batch_size)
        )
        thread.daemon = True
        thread.start()

        return task_id

    def _build_graph_worker(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int,
        resume_from: int = 0,
        existing_graph_id: str = None,
        prior_entities: Dict = None,
        prior_relationships: List = None,
    ):
        """Graph build worker thread"""
        _active_builds[task_id] = {"stop": False}
        try:
            self.task_manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                progress=5,
                message="Starting graph build..."
            )

            # 1. Create or reuse graph
            if existing_graph_id:
                graph_id = existing_graph_id
                self.task_manager.update_task(task_id, progress=10, message=f"Resuming graph: {graph_id}")
            else:
                graph_id = self.create_graph(graph_name)
                self.task_manager.update_task(task_id, progress=10, message=f"Graph created: {graph_id}")

            # 2. Set ontology
            if not existing_graph_id:
                self.set_ontology(graph_id, ontology)
            self.task_manager.update_task(task_id, progress=15, message="Ontology set")

            # 3. Split text into chunks
            chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
            total_chunks = len(chunks)
            self.task_manager.update_task(
                task_id, progress=20,
                message=f"Text split into {total_chunks} chunks (starting from {resume_from})"
            )

            # 4. Extract entities and relationships from chunks
            def extraction_progress(msg, prog):
                progress = 20 + int(prog * 60)  # 20-80%
                self.task_manager.update_task(task_id, progress=progress, message=msg)

            def stop_check():
                return _active_builds.get(task_id, {}).get("stop", False)

            # Store episodes for tracking (only for new chunks if resuming)
            if not existing_graph_id:
                episodes = self.db.add_episodes_batch(graph_id, chunks)
                episode_uuids = [ep.uuid_ for ep in episodes]
            else:
                episode_uuids = []

            # Extract entities and relationships using LLM
            extraction_result = self.extractor.extract_batch(
                chunks, ontology,
                progress_callback=extraction_progress,
                stop_check=stop_check,
                start_from=resume_from,
                prior_entities={e.get("name", "").lower(): e for e in (prior_entities or [])},
                prior_relationships=prior_relationships,
            )

            # Check if stopped
            if extraction_result.get("stopped"):
                last_chunk = extraction_result["last_chunk_index"]
                # Save progress in task metadata for resume
                self.task_manager.update_task(
                    task_id,
                    status=TaskStatus.FAILED,
                    message=f"Build parado pelo usuário no chunk {last_chunk}/{total_chunks}",
                    error="stopped_by_user",
                )
                # Save partial results for resume
                import json
                progress_file = os.path.join(
                    Config.TASKS_DIR,
                    f"{task_id}_progress.json"
                )
                with open(progress_file, 'w') as f:
                    json.dump({
                        "last_chunk_index": last_chunk,
                        "total_chunks": total_chunks,
                        "graph_id": graph_id,
                        "entities": extraction_result["entities"],
                        "relationships": extraction_result["relationships"],
                    }, f)
                logger.info(f"[{task_id}] Build stopped. Progress saved at chunk {last_chunk}/{total_chunks}")
                return

            # 5. Populate graph with extracted data
            self.task_manager.update_task(
                task_id, progress=80,
                message="Populating graph with extracted entities..."
            )

            self._populate_graph(graph_id, extraction_result, episode_uuids)

            # Mark all episodes as processed
            for ep_uuid in episode_uuids:
                self.db.mark_episode_processed(graph_id, ep_uuid)

            # 6. Get graph info
            self.task_manager.update_task(
                task_id, progress=95,
                message="Retrieving graph info..."
            )

            graph_info = self._get_graph_info(graph_id)

            # Complete
            self.task_manager.complete_task(task_id, {
                "graph_id": graph_id,
                "graph_info": graph_info.to_dict(),
                "chunks_processed": total_chunks,
            })

        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            self.task_manager.fail_task(task_id, error_msg)
        finally:
            _active_builds.pop(task_id, None)

    def create_graph(self, name: str) -> str:
        """Create a graph (public method)"""
        graph_id = f"mirofish_{uuid.uuid4().hex[:16]}"
        self.db.create_graph(graph_id, name, "MiroFish Social Simulation Graph")
        return graph_id

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]):
        """Set graph ontology (public method)"""
        self.db.set_ontology(graph_id, ontology)

    def _populate_graph(
        self,
        graph_id: str,
        extraction_result: Dict[str, Any],
        episode_uuids: List[str]
    ):
        """Populate the graph with extracted entities and relationships"""
        entities = extraction_result.get("entities", [])
        relationships = extraction_result.get("relationships", [])

        # Add entities as nodes
        entity_name_to_uuid = {}
        for entity in entities:
            name = entity.get("name", "").strip()
            if not name:
                continue
            entity_type = entity.get("type", "Entity")
            labels = ["Entity", entity_type] if entity_type != "Entity" else ["Entity"]
            summary = entity.get("summary", "")

            node = self.db.add_node(
                graph_id, name=name, labels=labels, summary=summary
            )
            entity_name_to_uuid[name.lower()] = node.uuid_

        # Add relationships as edges
        for rel in relationships:
            source_name = rel.get("source", "").strip()
            target_name = rel.get("target", "").strip()
            rel_type = rel.get("type", "related_to")
            fact = rel.get("fact", "")

            source_uuid = entity_name_to_uuid.get(source_name.lower())
            target_uuid = entity_name_to_uuid.get(target_name.lower())

            if not source_uuid or not target_uuid:
                # Try to find or create the missing nodes
                if not source_uuid:
                    node = self.db.add_node(graph_id, name=source_name, labels=["Entity"])
                    source_uuid = node.uuid_
                    entity_name_to_uuid[source_name.lower()] = source_uuid
                if not target_uuid:
                    node = self.db.add_node(graph_id, name=target_name, labels=["Entity"])
                    target_uuid = node.uuid_
                    entity_name_to_uuid[target_name.lower()] = target_uuid

            ep_uuid = episode_uuids[0] if episode_uuids else None
            self.db.add_edge(
                graph_id, source_uuid, target_uuid,
                name=rel_type, fact=fact,
                episode_uuid=ep_uuid
            )

        logger.info(f"Graph {graph_id} populated: {len(entity_name_to_uuid)} nodes, "
                    f"{len(relationships)} edges")

    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable] = None
    ) -> List[str]:
        """
        Add text chunks to graph, extract and populate.
        Returns episode UUIDs.
        """
        # Store episodes
        episodes = self.db.add_episodes_batch(graph_id, chunks)
        episode_uuids = [ep.uuid_ for ep in episodes]

        # Get ontology for extraction
        ontology = self.db.get_ontology(graph_id) or {"entity_types": [], "edge_types": []}

        # Extract and populate
        extraction_result = self.extractor.extract_batch(
            chunks, ontology, progress_callback=progress_callback
        )
        self._populate_graph(graph_id, extraction_result, episode_uuids)

        # Mark processed
        for ep_uuid in episode_uuids:
            self.db.mark_episode_processed(graph_id, ep_uuid)

        return episode_uuids

    def _wait_for_episodes(
        self,
        episode_uuids: List[str],
        progress_callback: Optional[Callable] = None,
        timeout: int = 600
    ):
        """
        Wait for episodes to be processed.
        With KuzuDB, processing is synchronous so this is a no-op,
        but kept for API compatibility with callers.
        """
        if progress_callback:
            progress_callback(f"Processing complete: {len(episode_uuids)} chunks", 1.0)

    def _get_graph_info(self, graph_id: str) -> GraphInfo:
        """Get graph information"""
        nodes = self.db.get_all_nodes(graph_id)
        edges = self.db.get_all_edges(graph_id)

        entity_types = set()
        for node in nodes:
            for label in node.labels:
                if label not in ("Entity", "Node"):
                    entity_types.add(label)

        return GraphInfo(
            graph_id=graph_id,
            node_count=len(nodes),
            edge_count=len(edges),
            entity_types=list(entity_types)
        )

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        """Get complete graph data (nodes and edges with details)"""
        return self.db.get_graph_data(graph_id)

    def delete_graph(self, graph_id: str):
        """Delete a graph"""
        self.db.delete_graph(graph_id)
