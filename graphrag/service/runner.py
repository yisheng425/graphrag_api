# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Execution helpers for invoking GraphRAG pipelines."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import graphrag.api as api
from graphrag.config.enums import CacheType, IndexingMethod, ReportingType
from graphrag.config.load_config import load_config
from graphrag.index.validate_config import validate_config_names

logger = logging.getLogger(__name__)


def _resolve_child_path(base: Path, path: Path | None) -> Path | None:
    if path is None:
        return None
    if path.is_absolute():
        return path
    return (base / path).resolve()


def _normalize_method(raw: str | IndexingMethod) -> IndexingMethod:
    if isinstance(raw, IndexingMethod):
        return raw
    try:
        return IndexingMethod(raw)
    except ValueError as exc:
        message = f"Unsupported indexing method: {raw}"
        raise ValueError(message) from exc


def _stringify_result(result: Any) -> str | None:
    if result is None:
        return None
    try:
        return str(result)
    except Exception:  # noqa: BLE001
        return None


def run_index_job(payload: dict[str, Any]) -> dict[str, Any]:
    """Run a GraphRAG indexing job synchronously."""
    root_dir = Path(payload["root"]).expanduser().resolve()
    if not root_dir.is_dir():
        msg = f"Invalid root directory: {root_dir}"
        raise FileNotFoundError(msg)

    config_path = (
        _resolve_child_path(root_dir, Path(payload["config"]))
        if payload.get("config")
        else None
    )
    output_dir = (
        _resolve_child_path(root_dir, Path(payload["output"]))
        if payload.get("output")
        else None
    )

    method = _normalize_method(payload.get("method", IndexingMethod.Standard.value))

    cli_overrides: dict[str, Any] = {}
    if output_dir:
        cli_overrides["output.base_dir"] = str(output_dir)
        cli_overrides["reporting.base_dir"] = str(output_dir)
        cli_overrides["update_index_output.base_dir"] = str(output_dir)

    config = load_config(
        root_dir=root_dir,
        config_filepath=config_path,
        cli_overrides=cli_overrides,
    )

    if not payload.get("cache", True):
        config.cache.type = CacheType.none

    skip_validation = payload.get("skip_validation", False)
    if not skip_validation:
        validate_config_names(config)

    if payload.get("verbose"):
        logger.setLevel(logging.INFO)

    outputs = asyncio.run(
        api.build_index(
            config=config,
            method=method,
            is_update_run=payload.get("is_update", False),
            memory_profile=payload.get("memprofile", False),
        )
    )

    workflows: list[dict[str, Any]] = []
    encountered_errors = False
    for output in outputs:
        errors = [str(err) for err in output.errors or []]
        if errors:
            encountered_errors = True
        workflows.append(
            {
                "workflow": output.workflow,
                "errors": errors,
                "result": _stringify_result(output.result),
            }
        )

    reporting_dir: str | None = None
    if config.reporting.type == ReportingType.file:
        reporting_dir = config.reporting.base_dir

    return {
        "encountered_errors": encountered_errors,
        "workflows": workflows,
        "output_dir": getattr(config.output, "base_dir", None),
        "reporting_dir": reporting_dir,
        "update_output_dir": getattr(config.update_index_output, "base_dir", None),
    }
