"""Optional single-loop scheduler; the host still owns resources and supervision."""

import asyncio
from collections.abc import Awaitable, Callable


class PeriodicRelay:
    def __init__(
        self,
        attempt: Callable[[], Awaitable[int]],
        *,
        poll_seconds: float = 1,
        shutdown_seconds: float = 5,
    ) -> None:
        if not 0 < poll_seconds < float("inf") or not 0 < shutdown_seconds < float("inf"):
            raise ValueError("positive finite lifecycle bounds required")
        self._attempt = attempt
        self._poll_seconds = poll_seconds
        self._shutdown_seconds = shutdown_seconds
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("relay must be stopped before restart")
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="reliable-messaging-relay")

    def notify(self) -> None:
        """Lossy post-commit hint; the durable scan remains authoritative."""
        self._wake.set()

    async def wait(self) -> None:
        if self._task is None:
            raise RuntimeError("relay is not started")
        await asyncio.shield(self._task)

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        self._wake.set()
        task = self._task
        try:
            async with asyncio.timeout(self._shutdown_seconds):
                await asyncio.shield(task)
        except (TimeoutError, asyncio.CancelledError):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise
        finally:
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()
            await self._attempt()
            if not self._stop.is_set():
                try:
                    async with asyncio.timeout(self._poll_seconds):
                        await self._wake.wait()
                except TimeoutError:
                    pass
