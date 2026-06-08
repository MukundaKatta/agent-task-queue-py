"""agent-task-queue-py — priority queue for agent sub-tasks."""

from __future__ import annotations

import heapq
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """A queued sub-task."""

    task_id: str
    fn: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    priority: int = 0  # lower = higher priority (min-heap)
    created_at: float = field(default_factory=time.time)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Exception | None = None
    metadata: dict = field(default_factory=dict)
    seq: int = 0  # monotonic insertion counter for stable FIFO tie-breaking

    def __lt__(self, other: "Task") -> bool:
        # Heap ordering: lower priority number first, then FIFO by insertion order.
        # ``seq`` is a strictly increasing counter, so it guarantees stable FIFO
        # ordering even when two tasks share the same ``created_at`` timestamp
        # (``time.time()`` has limited resolution and can collide in tight loops).
        return (self.priority, self.seq) < (other.priority, other.seq)


class TaskQueue:
    """
    Priority queue for agent sub-tasks.

    Tasks are executed in priority order (lowest priority number first).
    Supports synchronous sequential execution or inspection without running.

    Example::

        queue = TaskQueue()
        queue.enqueue("fetch_data", fetch_fn, args=("url",), priority=0)
        queue.enqueue("summarize", summarize_fn, priority=10)
        queue.enqueue("urgent_fix", fix_fn, priority=-1)   # runs first

        results = queue.run_all()
        for r in results:
            print(r.task_id, r.status, r.result)

        # Or run one at a time:
        task = queue.pop()
        queue.execute(task)
    """

    def __init__(self) -> None:
        self._heap: list[Task] = []
        self._all: dict[str, Task] = {}
        self._lock = threading.Lock()
        self._counter = 0

    def enqueue(
        self,
        task_id: str,
        fn: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        priority: int = 0,
        metadata: dict | None = None,
    ) -> "TaskQueue":
        """Add a task to the queue.

        Args:
            task_id: Unique identifier for the task. Used by :meth:`get` and
                :meth:`cancel`. Must not already exist in the queue.
            fn: The callable to run when the task executes.
            args: Positional arguments passed to ``fn``.
            kwargs: Keyword arguments passed to ``fn``.
            priority: Execution priority. Lower numbers run first (min-heap);
                negative values are allowed and run before ``0``.
            metadata: Optional free-form dict stored on the task.

        Returns:
            ``self``, so calls can be chained.

        Raises:
            ValueError: If ``task_id`` is already present in the queue. Allowing
                a duplicate would silently desynchronize the internal heap from
                the id index, causing the same id to run more than once while
                :meth:`size` and :meth:`pending` report only one entry.
        """
        with self._lock:
            if task_id in self._all:
                raise ValueError(f"task_id {task_id!r} already exists in the queue")
            task = Task(
                task_id=task_id,
                fn=fn,
                args=args,
                kwargs=kwargs or {},
                priority=priority,
                metadata=metadata or {},
                seq=self._counter,
            )
            self._counter += 1
            heapq.heappush(self._heap, task)
            self._all[task_id] = task
        return self

    def pop(self) -> Task | None:
        """Remove and return the highest-priority pending task, or None if empty."""
        with self._lock:
            while self._heap:
                task = heapq.heappop(self._heap)
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.RUNNING
                    return task
            return None

    def peek(self) -> Task | None:
        """Return the highest-priority pending task without removing it."""
        with self._lock:
            for task in sorted(self._heap):
                if task.status == TaskStatus.PENDING:
                    return task
            return None

    def execute(self, task: Task) -> Task:
        """Run a task and update its status, result, and error in place.

        The task's ``fn`` is invoked with its stored ``args`` and ``kwargs``.
        On success the status becomes :attr:`TaskStatus.DONE` and ``result`` is
        set; on any exception the status becomes :attr:`TaskStatus.FAILED` and
        the exception is stored on ``error`` rather than propagated. The same
        ``task`` object is returned for convenience.
        """
        task.status = TaskStatus.RUNNING
        try:
            task.result = task.fn(*task.args, **task.kwargs)
            task.status = TaskStatus.DONE
        except Exception as exc:  # noqa: BLE001
            task.error = exc
            task.status = TaskStatus.FAILED
        return task

    def run_all(self) -> list[Task]:
        """Run all pending tasks in priority order and return them."""
        results: list[Task] = []
        while True:
            task = self.pop()
            if task is None:
                break
            self.execute(task)
            results.append(task)
        return results

    def cancel(self, task_id: str) -> bool:
        """Mark a task as cancelled. Returns True if the task existed."""
        with self._lock:
            task = self._all.get(task_id)
            if task and task.status == TaskStatus.PENDING:
                task.status = TaskStatus.CANCELLED
                return True
            return False

    def get(self, task_id: str) -> Task | None:
        """Return a task by ID, or None if not found."""
        return self._all.get(task_id)

    def pending(self) -> list[Task]:
        """Return all pending tasks in priority order."""
        with self._lock:
            return sorted(
                t for t in self._all.values() if t.status == TaskStatus.PENDING
            )

    def done(self) -> list[Task]:
        """Return all completed tasks."""
        return [t for t in self._all.values() if t.status == TaskStatus.DONE]

    def failed(self) -> list[Task]:
        """Return all failed tasks."""
        return [t for t in self._all.values() if t.status == TaskStatus.FAILED]

    def size(self) -> int:
        """Return the number of pending tasks."""
        return sum(1 for t in self._all.values() if t.status == TaskStatus.PENDING)

    def clear(self) -> None:
        """Remove all tasks."""
        with self._lock:
            self._heap.clear()
            self._all.clear()


__all__ = ["TaskQueue", "Task", "TaskStatus"]
