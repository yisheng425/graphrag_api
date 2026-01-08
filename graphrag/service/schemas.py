# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Pydantic schemas for the GraphRAG service API."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from graphrag.config.enums import IndexingMethod, SearchMethod


class IndexRequest(BaseModel):
    """Request payload for index or update operations."""

    root: Path = Field(description="Project root directory", examples=["./ragtest"])
    config: Path | None = Field(
        default=None, description="Optional configuration file path"
    )
    method: IndexingMethod = Field(
        default=IndexingMethod.Standard,
        description="Indexing method to use",
    )
    verbose: bool = False
    memprofile: bool = False
    cache: bool = True
    skip_validation: bool = False
    output: Path | None = Field(
        default=None, description="Optional override for output directory"
    )

    @field_validator("root", mode="before")
    @classmethod
    def _expand_root(cls, value: Any) -> Any:
        if isinstance(value, (str, Path)):
            return Path(value).expanduser()
        return value

    @field_validator("config", "output", mode="before")
    @classmethod
    def _expand_optional_paths(cls, value: Any) -> Any:
        if value in (None, ""):
            return None
        if isinstance(value, (str, Path)):
            return Path(value).expanduser()
        return value

    def to_payload(self, *, is_update: bool) -> dict[str, Any]:
        """Convert the request into a Celery-friendly payload."""
        return {
            "root": str(self.root),
            "config": str(self.config) if self.config else None,
            "method": self.method.value,
            "verbose": self.verbose,
            "memprofile": self.memprofile,
            "cache": self.cache,
            "skip_validation": self.skip_validation,
            "output": str(self.output) if self.output else None,
            "is_update": is_update,
        }


class QueryRequest(BaseModel):
    """Request payload for query operations."""

    root: Path = Field(description="Project root directory", examples=["./ragtest"])
    config: Path | None = Field(
        default=None,
        description="Optional configuration file path",
        examples=["./configs/settings.yaml"],
    )
    data: Path | None = Field(
        default=None,
        description="Index data directory containing parquet outputs",
        examples=["./ragtest/update_index_output/latest/delta"],
    )
    method: SearchMethod = Field(
        default=SearchMethod.LOCAL,
        description="Query algorithm to execute",
    )
    query: str = Field(description="User query text", min_length=1)
    community_level: int | None = Field(
        default=2,
        description="Leiden hierarchy level for community-based searches",
    )
    dynamic_community_selection: bool = Field(
        default=False,
        description="Enable dynamic community selection for global search",
    )
    response_type: str = Field(
        default="Multiple Paragraphs",
        description="Desired response format instruction",
    )
    streaming: bool = Field(
        default=False,
        description="Stream response chunks during execution",
    )
    verbose: bool = Field(default=False, description="Enable verbose logging")

    @field_validator("root", mode="before")
    @classmethod
    def _expand_root(cls, value: Any) -> Any:
        if isinstance(value, (str, Path)):
            return Path(value).expanduser()
        return value

    @field_validator("config", "data", mode="before")
    @classmethod
    def _expand_optional_paths(cls, value: Any) -> Any:
        if value in (None, ""):
            return None
        if isinstance(value, (str, Path)):
            return Path(value).expanduser()
        return value

    @field_validator("community_level")
    @classmethod
    def _validate_community_level(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("community_level 不能为负数")
        return value

    def to_payload(self) -> dict[str, Any]:
        """Convert the request into a task payload."""

        return {
            "root": str(self.root),
            "config": str(self.config) if self.config else None,
            "data": str(self.data) if self.data else None,
            "method": self.method.value,
            "query": self.query,
            "community_level": self.community_level,
            "dynamic_community_selection": self.dynamic_community_selection,
            "response_type": self.response_type,
            "streaming": self.streaming,
            "verbose": self.verbose,
        }


class TaskSubmissionResponse(BaseModel):
    """Response after enqueuing a task."""

    task_id: str


class WorkflowResult(BaseModel):
    """Serialized workflow execution result."""

    workflow: str
    errors: list[str]
    result: str | None = None


class TaskResult(BaseModel):
    """Structured task result payload."""

    encountered_errors: bool
    workflows: list[WorkflowResult]
    output_dir: str | None = None
    reporting_dir: str | None = None
    update_output_dir: str | None = None


class QueryResult(BaseModel):
    """Structured query result payload."""

    type: Literal["query"] = "query"
    method: SearchMethod
    query: str
    response: Any
    context: Any | None = None
    response_type: str | None = None
    community_level: int | None = None
    dynamic_community_selection: bool | None = None
    streaming: bool | None = None


class TaskStatusResponse(BaseModel):
    """Status for a Celery task."""

    task_id: str
    status: str
    state: str
    result: TaskResult | QueryResult | None = None
    error: str | None = None
