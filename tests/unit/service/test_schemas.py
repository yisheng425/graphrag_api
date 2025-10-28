"""Unit tests for service-layer schemas."""

from __future__ import annotations

from pathlib import Path

from graphrag.config.enums import SearchMethod
from graphrag.service.schemas import QueryRequest, QueryResult


def test_query_request_to_payload_expands_paths(tmp_path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    data_dir = root / "output"
    data_dir.mkdir()

    request = QueryRequest(
        root=root,
        query="hello",
        method=SearchMethod.LOCAL,
        data=data_dir,
        response_type="Multiple Paragraphs",
    )

    payload = request.to_payload()

    assert payload["root"] == str(root)
    assert payload["data"] == str(data_dir)
    assert payload["method"] == SearchMethod.LOCAL.value
    assert payload["response_type"] == "Multiple Paragraphs"
    assert payload["streaming"] is False


def test_query_result_validation_parses_enum() -> None:
    result = QueryResult.model_validate(
        {
            "type": "query",
            "method": "local",
            "query": "hello",
            "response": "world",
            "context": {"entities": []},
            "response_type": "Multiple Paragraphs",
        }
    )

    assert result.method is SearchMethod.LOCAL
    assert result.query == "hello"
    assert isinstance(result.context, dict)