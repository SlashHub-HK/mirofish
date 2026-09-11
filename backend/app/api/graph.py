"""
Graph-related API routes
Uses project context mechanism with server-side persistent state
"""

import os
from flask import request, jsonify

from . import graph_bp
from ..config import Config
from ..core.workbench_session import WorkbenchSession
from ..services.graph_builder import GraphBuilderService
from ..utils.logger import get_logger
from ..models.task import TaskManager
from ..models.project import ProjectManager, ProjectStatus

# Get logger
logger = get_logger('mirofish.api')


def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed"""
    if not filename or '.' not in filename:
        return False
    ext = os.path.splitext(filename)[1].lower().lstrip('.')
    return ext in Config.ALLOWED_EXTENSIONS


# ============== Project Management API ==============

@graph_bp.route('/project/<project_id>', methods=['GET'])
def get_project(project_id: str):
    """
    Get project details
    """
    project = ProjectManager.get_project(project_id)
    
    if not project:
        return jsonify({
            "success": False,
            "error": f"Project not found: {project_id}"
        }), 404

    return jsonify({
        "success": True,
        "data": project.to_dict()
    })


@graph_bp.route('/project/list', methods=['GET'])
def list_projects():
    """
    List all projects
    """
    limit = request.args.get('limit', 50, type=int)
    projects = ProjectManager.list_projects(limit=limit)
    
    return jsonify({
        "success": True,
        "data": [p.to_dict() for p in projects],
        "count": len(projects)
    })


@graph_bp.route('/project/<project_id>', methods=['DELETE'])
def delete_project(project_id: str):
    """
    Delete project
    """
    success = ProjectManager.delete_project(project_id)
    
    if not success:
        return jsonify({
            "success": False,
            "error": f"Project not found or deletion failed: {project_id}"
        }), 404
    
    return jsonify({
        "success": True,
        "message": f"Project deleted: {project_id}"
    })


@graph_bp.route('/project/<project_id>/reset', methods=['POST'])
def reset_project(project_id: str):
    """
    Reset project state (for rebuilding the graph)
    """
    project = ProjectManager.get_project(project_id)
    
    if not project:
        return jsonify({
            "success": False,
            "error": f"Project not found: {project_id}"
        }), 404

    # Reset to ontology-generated state
    if project.ontology:
        project.status = ProjectStatus.ONTOLOGY_GENERATED
    else:
        project.status = ProjectStatus.CREATED
    
    project.graph_id = None
    project.graph_build_task_id = None
    project.error = None
    ProjectManager.save_project(project)
    
    return jsonify({
        "success": True,
        "message": f"Project reset: {project_id}",
        "data": project.to_dict()
    })


@graph_bp.route('/project/<project_id>', methods=['PUT', 'PATCH'])
def update_project(project_id: str):
    """
    Update project metadata (name, etc.)
    """
    project = ProjectManager.get_project(project_id)

    if not project:
        return jsonify({
            "success": False,
            "error": f"Project not found: {project_id}"
        }), 404

    data = request.get_json() or {}
    if 'name' in data:
        project.name = data['name']
    ProjectManager.save_project(project)

    return jsonify({
        "success": True,
        "data": project.to_dict()
    })


@graph_bp.route('/project/<project_id>/stop', methods=['POST'])
def stop_project(project_id: str):
    """
    Stop a running graph build task for a project.
    Saves progress so it can be resumed later.
    """
    project = ProjectManager.get_project(project_id)

    if not project:
        return jsonify({
            "success": False,
            "error": f"Project not found: {project_id}"
        }), 404

    if project.status != ProjectStatus.GRAPH_BUILDING:
        return jsonify({
            "success": False,
            "error": "Project is not currently building"
        }), 400

    task_id = project.graph_build_task_id
    if task_id:
        from ..services.graph_builder import request_stop
        request_stop(task_id)

    # Set status to paused (we use ONTOLOGY_GENERATED but keep task_id for resume)
    project.status = ProjectStatus.ONTOLOGY_GENERATED
    project.error = "paused"
    ProjectManager.save_project(project)

    return jsonify({
        "success": True,
        "message": f"Build stop requested for: {project_id}",
        "data": project.to_dict()
    })


@graph_bp.route('/project/<project_id>/resume', methods=['POST'])
def resume_project(project_id: str):
    """
    Resume a previously stopped graph build from where it left off.
    """
    import json as json_mod

    project = ProjectManager.get_project(project_id)

    if not project:
        return jsonify({"success": False, "error": f"Project not found: {project_id}"}), 404

    task_id = project.graph_build_task_id
    if not task_id:
        return jsonify({"success": False, "error": "No previous build task found"}), 400

    # Load saved progress
    progress_file = os.path.join(
        Config.TASKS_DIR,
        f"{task_id}_progress.json"
    )
    if not os.path.exists(progress_file):
        return jsonify({"success": False, "error": "No saved progress found. Use rebuild instead."}), 400

    with open(progress_file, 'r') as f:
        progress_data = json_mod.load(f)

    last_chunk = progress_data.get("last_chunk_index", 0)
    total_chunks = progress_data.get("total_chunks", 0)
    graph_id = progress_data.get("graph_id")
    prior_entities = progress_data.get("entities", [])
    prior_relationships = progress_data.get("relationships", [])

    # Create new task for resumed build
    from ..models.task import TaskManager as TM
    from ..services.graph_builder import GraphBuilderService
    from ..services.text_processor import TextProcessor
    from ..resources.documents import DocumentStore

    tm = TM()
    new_task_id = tm.create_task(
        task_type="graph_build",
        metadata={
            "project_id": project_id,
            "graph_name": project.name or "MiroFish Graph",
            "resumed_from_chunk": last_chunk,
            "total_chunks": total_chunks,
        }
    )

    project.status = ProjectStatus.GRAPH_BUILDING
    project.graph_build_task_id = new_task_id
    project.error = None
    ProjectManager.save_project(project)

    # Get text and ontology
    doc_store = DocumentStore()
    text = doc_store.get_extracted_text(project_id)
    ontology = project.ontology
    chunk_size = project.chunk_size or Config.DEFAULT_CHUNK_SIZE
    chunk_overlap = project.chunk_overlap or Config.DEFAULT_CHUNK_OVERLAP

    import threading
    builder = GraphBuilderService()

    def run_resume():
        builder._build_graph_worker(
            task_id=new_task_id,
            text=text,
            ontology=ontology,
            graph_name=project.name or "MiroFish Graph",
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            batch_size=3,
            resume_from=last_chunk,
            existing_graph_id=graph_id,
            prior_entities=prior_entities,
            prior_relationships=prior_relationships,
        )
        # Cleanup progress file on completion
        task = tm.get_task(new_task_id)
        if task and task.get("status") == "completed":
            try:
                os.remove(progress_file)
            except OSError:
                pass

    thread = threading.Thread(target=run_resume, daemon=True)
    thread.start()

    return jsonify({
        "success": True,
        "data": {
            "project_id": project_id,
            "task_id": new_task_id,
            "resumed_from_chunk": last_chunk,
            "total_chunks": total_chunks,
            "message": f"Build resumed from chunk {last_chunk}/{total_chunks}",
        }
    })


# ============== API 1: Upload Files and Generate Ontology ==============

@graph_bp.route('/ontology/generate', methods=['POST'])
def generate_ontology():
    """API 1: Upload files and analyze to generate ontology definition."""
    try:
        logger.info("=== Starting ontology generation ===")

        simulation_requirement = request.form.get('simulation_requirement', '')
        project_name = request.form.get('project_name', 'Unnamed Project')
        additional_context = request.form.get('additional_context', '')
        uploaded_files = request.files.getlist('files')

        session = WorkbenchSession.open(metadata={"entrypoint": "api.graph.generate_ontology"})
        result = session.generate_ontology(
            simulation_requirement=simulation_requirement,
            uploaded_files=uploaded_files,
            project_name=project_name,
            additional_context=additional_context if additional_context else None,
        )

        return jsonify({
            "success": True,
            "data": result
        })

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Failed to generate ontology: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ============== API 2: Build Graph ==============

@graph_bp.route('/build', methods=['POST'])
def build_graph():
    """API 2: Build graph based on project_id."""
    try:
        logger.info("=== Starting graph build ===")

        data = request.get_json() or {}
        project_id = data.get('project_id')
        if not project_id:
            return jsonify({
                "success": False,
                "error": "Please provide project_id"
            }), 400

        session = WorkbenchSession.open(project_id=project_id, metadata={"entrypoint": "api.graph.build_graph"})
        result = session.start_graph_build(
            project_id=project_id,
            graph_name=data.get('graph_name'),
            chunk_size=data.get('chunk_size'),
            chunk_overlap=data.get('chunk_overlap'),
            force=data.get('force', False),
        )

        return jsonify({
            "success": True,
            "data": result
        })

    except FileNotFoundError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 404

    except ValueError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Failed to start graph build: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ============== Task Query API ==============

@graph_bp.route('/task/<task_id>', methods=['GET'])
def get_task(task_id: str):
    """
    Query task status
    """
    task = TaskManager().get_task(task_id)
    
    if not task:
        return jsonify({
            "success": False,
            "error": f"Task not found: {task_id}"
        }), 404
    
    return jsonify({
        "success": True,
        "data": task.to_dict()
    })


@graph_bp.route('/tasks', methods=['GET'])
def list_tasks():
    """
    List all tasks
    """
    tasks = TaskManager().list_tasks()
    
    return jsonify({
        "success": True,
        "data": [t.to_dict() for t in tasks],
        "count": len(tasks)
    })


# ============== Graph Data API ==============

@graph_bp.route('/data/<graph_id>', methods=['GET'])
def get_graph_data(graph_id: str):
    """
    Get graph data (nodes and edges)
    """
    try:
        builder = GraphBuilderService()
        graph_data = builder.get_graph_data(graph_id)
        
        return jsonify({
            "success": True,
            "data": graph_data
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@graph_bp.route('/delete/<graph_id>', methods=['DELETE'])
def delete_graph(graph_id: str):
    """
    Delete graph
    """
    try:
        builder = GraphBuilderService()
        builder.delete_graph(graph_id)

        return jsonify({
            "success": True,
            "message": f"Graph deleted: {graph_id}"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
