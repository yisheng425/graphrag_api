# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""异步工具函数，提供持久事件循环以支持 Celery solo 模式。"""

from __future__ import annotations

import asyncio
from typing import Any, Coroutine, TypeVar

_T = TypeVar("_T")

# 持久事件循环，用于在 Celery solo 模式下避免 asyncio.run() 关闭循环导致的问题
_persistent_loop: asyncio.AbstractEventLoop | None = None


def run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    """使用持久事件循环运行协程。

    在 Celery solo 模式下，asyncio.run() 每次都创建新循环并在结束时关闭，
    但 httpx/aiolimiter 等库的客户端会在 GC 时尝试在已关闭的循环上执行清理，
    导致 'Event loop is closed' 错误。

    使用持久循环可以避免此问题，所有任务共用同一个循环。

    Args:
        coro: 要执行的协程

    Returns:
        协程的返回值
    """
    global _persistent_loop

    if _persistent_loop is None or _persistent_loop.is_closed():
        _persistent_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_persistent_loop)

    return _persistent_loop.run_until_complete(coro)
