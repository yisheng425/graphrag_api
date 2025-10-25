# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Celery task definitions for GraphRAG service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from celery import Celery

from graphrag.service.runner import run_index_job, run_query_job
from graphrag.service.settings import get_settings

if TYPE_CHECKING:
    from celery.result import AsyncResult

settings = get_settings()

celery_app = Celery(
    "graphrag_service",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(task_track_started=True)


@celery_app.task(name="graphrag.build_index")
def build_index_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Execute a GraphRAG indexing job inside a Celery worker."""
    return run_index_job(payload)


def enqueue_build_index(payload: dict[str, Any]) -> AsyncResult:
    """Enqueue a build index task and return the async result."""
    return build_index_task.delay(payload)


@celery_app.task(name="graphrag.run_query")
def query_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Execute a GraphRAG query job inside a Celery worker."""
    return run_query_job(payload)


def enqueue_query(payload: dict[str, Any]) -> AsyncResult:
    """Enqueue a query task and return the async result."""
    return query_task.delay(payload)


def get_task(task_id: str) -> AsyncResult:
    """Fetch a task result by identifier."""
    return celery_app.AsyncResult(task_id)
