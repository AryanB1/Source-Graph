"""API routes for runs."""
import logging
import time
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..db import get_session
from ..schemas import CreateRunRequest, CreateRunResponse, GraphDTO
from ..services import (
    IngestionError,
    NotFoundError,
    ValidationError,
    create_run_and_ingest,
    get_run_graph,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/runs", tags=["runs"])


@router.post("", response_model=CreateRunResponse, status_code=201)
def create_run(
    request: CreateRunRequest,
    session: Session = Depends(get_session),
) -> CreateRunResponse:
    """
    Create a new ingestion run.
    
    This endpoint:
    1. Validates the request
    2. Executes ingestion (query or seed mode)
    3. Persists posts and edges to the database
    4. Returns a run ID that can be used to retrieve the graph
    
    Request body:
    - mode: "query" or "seed"
    - query: search query (required for query mode)
    - seedUri: post URI to start from (required for seed mode)
    - params: optional parameters for ingestion
    
    Returns:
        CreateRunResponse with runId
    """
    start_time = time.time()
    
    try:
        # Convert request to dict for service layer
        payload = request.model_dump(by_alias=True)
        
        # Execute ingestion and persistence
        run_id = create_run_and_ingest(session, payload)
        
        duration = time.time() - start_time
        logger.info(
            f"Run {run_id} created successfully in {duration:.2f}s "
            f"(mode={request.mode})"
        )
        
        return CreateRunResponse(runId=str(run_id))
    
    except ValidationError as e:
        logger.warning(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except IngestionError as e:
        logger.error(f"Ingestion error: {e}")
        raise HTTPException(status_code=502, detail=f"Ingestion failed: {str(e)}")
    
    except Exception as e:
        logger.error(f"Unexpected error creating run: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{run_id}/graph", response_model=GraphDTO)
def get_graph(
    run_id: uuid.UUID,
    max_nodes: Optional[int] = Query(None, description="Maximum number of nodes to return"),
    session: Session = Depends(get_session),
) -> GraphDTO:
    """
    Retrieve the graph for a run.
    
    This endpoint:
    1. Fetches all posts and edges linked to the run
    2. Computes in/out degrees for each node
    3. Optionally filters to top N nodes by engagement
    4. Returns the graph with statistics
    
    Path parameters:
    - run_id: UUID of the run
    
    Query parameters:
    - maxNodes: optional limit on number of nodes (filters by engagement score)
    
    Returns:
        GraphDTO with nodes, edges, and stats
    """
    start_time = time.time()
    
    try:
        graph = get_run_graph(session, run_id, max_nodes)
        
        duration = time.time() - start_time
        logger.info(
            f"Graph retrieved for run {run_id} in {duration:.2f}s "
            f"({graph.stats.node_count} nodes, {graph.stats.edge_count} edges)"
        )
        
        return graph
    
    except NotFoundError as e:
        logger.warning(f"Run not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    
    except Exception as e:
        logger.error(f"Unexpected error retrieving graph: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
