"""FastAPI application exposing GraphRAG indexing as a service."""

from __future__ import annotations

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, status

from graphrag.service import schemas
from graphrag.service.settings import ServiceSettings, get_settings
from graphrag.service.tasks import enqueue_build_index, get_task


def _ensure_root_allowed(root: Path, settings: ServiceSettings) -> Path:
    resolved = root.expanduser().resolve()
    if not resolved.is_dir():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"项目根目录不存在: {resolved}",
        )
    if not settings.is_root_allowed(resolved):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="指定root不在允许列表中",
        )
    return resolved


def _normalize_child_path(root: Path, path: Path | None) -> Path | None:
    if path is None:
        return None
    candidate = path.expanduser()
    return candidate if candidate.is_absolute() else (root / candidate).resolve()


def get_service_settings() -> ServiceSettings:
    return get_settings()


def create_app() -> FastAPI:
    """Instantiate the FastAPI application."""

    app = FastAPI(title="GraphRAG Service", version="1.0.0")

    @app.post("/index", response_model=schemas.TaskSubmissionResponse)
    def submit_index(
        request: schemas.IndexRequest,
        settings: ServiceSettings = Depends(get_service_settings),
    ) -> schemas.TaskSubmissionResponse:
        root = _ensure_root_allowed(request.root, settings)

        config_path = _normalize_child_path(root, request.config)
        output_path = _normalize_child_path(root, request.output)

        if config_path and not config_path.is_file():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"配置文件不存在: {config_path}",
            )

        if output_path and not output_path.parent.exists():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"输出目录父路径不存在: {output_path.parent}",
            )

        payload = request.to_payload(is_update=False)
        payload["root"] = str(root)
        payload["config"] = str(config_path) if config_path else None
        payload["output"] = str(output_path) if output_path else None

        async_result = enqueue_build_index(payload)
        return schemas.TaskSubmissionResponse(task_id=async_result.id)

    @app.post("/update", response_model=schemas.TaskSubmissionResponse)
    def submit_update(
        request: schemas.IndexRequest,
        settings: ServiceSettings = Depends(get_service_settings),
    ) -> schemas.TaskSubmissionResponse:
        root = _ensure_root_allowed(request.root, settings)
        config_path = _normalize_child_path(root, request.config)
        output_path = _normalize_child_path(root, request.output)

        if config_path and not config_path.is_file():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"配置文件不存在: {config_path}",
            )

        if output_path and not output_path.parent.exists():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"输出目录父路径不存在: {output_path.parent}",
            )

        payload = request.to_payload(is_update=True)
        payload["root"] = str(root)
        payload["config"] = str(config_path) if config_path else None
        payload["output"] = str(output_path) if output_path else None

        async_result = enqueue_build_index(payload)
        return schemas.TaskSubmissionResponse(task_id=async_result.id)

    @app.get("/tasks/{task_id}", response_model=schemas.TaskStatusResponse)
    def get_task_status(task_id: str) -> schemas.TaskStatusResponse:
        async_result = get_task(task_id)

        response = schemas.TaskStatusResponse(
            task_id=task_id,
            status=async_result.status,
            state=async_result.state,
        )

        if async_result.failed():
            response.error = str(async_result.result)
        elif async_result.successful():
            result_payload = async_result.result
            if isinstance(result_payload, dict):
                response.result = schemas.TaskResult.model_validate(result_payload)
        return response

    return app


app = create_app()
