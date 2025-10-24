"""Configuration helpers for the GraphRAG service layer."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from pydantic import BaseModel, Field, ValidationError

from graphrag.config.enums import IndexingMethod


class ServiceSettings(BaseModel):
    """Runtime settings for the API service and task workers."""

    celery_broker_url: str = Field(
        default="redis://localhost:6379/0",
        description="Celery broker URL.",
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/0",
        description="Celery result backend URL.",
    )
    allowed_roots: list[Path] = Field(
        default_factory=list,
        description="Allowed project root directories for indexing operations.",
    )
    default_index_method: IndexingMethod = Field(
        default=IndexingMethod.Standard,
        description="Default indexing method used when request omits it.",
    )

    def is_root_allowed(self, root: Path) -> bool:
        """Check whether the resolved project root is permitted."""

        if not self.allowed_roots:
            return True
        resolved = root.resolve()
        return any(resolved.is_relative_to(allowed) for allowed in self.allowed_roots)


def _parse_allowed_roots(raw: str) -> list[Path]:
    parts = [segment.strip() for segment in raw.split(os.pathsep) if segment.strip()]
    return [Path(part).expanduser().resolve() for part in parts]


def _parse_index_method(raw: str) -> IndexingMethod:
    try:
        return IndexingMethod(raw)
    except ValueError:
        return IndexingMethod.Standard


def _build_settings_from_env() -> ServiceSettings:
    broker = os.environ.get("GRAPHRAG_CELERY_BROKER_URL", "redis://localhost:6379/0")
    backend = os.environ.get("GRAPHRAG_CELERY_BACKEND_URL", broker)
    roots_raw = os.environ.get("GRAPHRAG_ALLOWED_ROOTS", "")
    default_method_raw = os.environ.get(
        "GRAPHRAG_DEFAULT_INDEX_METHOD", IndexingMethod.Standard.value
    )

    settings = ServiceSettings(
        celery_broker_url=broker,
        celery_result_backend=backend,
        allowed_roots=_parse_allowed_roots(roots_raw),
        default_index_method=_parse_index_method(default_method_raw),
    )
    return settings


@lru_cache(maxsize=1)
def get_settings() -> ServiceSettings:
    """Load service settings with caching."""

    try:
        return _build_settings_from_env()
    except ValidationError as exc:
        raise RuntimeError("Invalid GraphRAG service configuration") from exc
