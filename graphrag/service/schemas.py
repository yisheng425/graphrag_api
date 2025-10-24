# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Pydantic schemas for the GraphRAG service API."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator

from graphrag.config.enums import IndexingMethod


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


class TaskStatusResponse(BaseModel):
    """Status for a Celery task."""

    task_id: str
    status: str
    state: str
    result: TaskResult | None = None
    error: str | None = None
