"""Service layer exposing GraphRAG pipelines via API endpoints."""

from graphrag.service.app import create_app

__all__ = ["create_app"]
