"""Tests for agent-task-queue-py."""
import pytest
from agent_task_queue import TaskQueue, Task, TaskStatus


def test_enqueue_and_pop():
    q = TaskQueue()
    q.enqueue("t1", lambda: 42)
    task = q.pop()
    assert task is not None
    assert task.task_id == "t1"


def test_pop_returns_none_when_empty():
    q = TaskQueue()
    assert q.pop() is None


def test_execute_success():
    q = TaskQueue()
    q.enqueue("t1", lambda: 99)
    task = q.pop()
    result = q.execute(task)
    assert result.status == TaskStatus.DONE
    assert result.result == 99


def test_execute_failure():
    def bad():
        raise RuntimeError("oops")

    q = TaskQueue()
    q.enqueue("bad", bad)
    task = q.pop()
    result = q.execute(task)
    assert result.status == TaskStatus.FAILED
    assert isinstance(result.error, RuntimeError)


def test_priority_ordering():
    q = TaskQueue()
    q.enqueue("low", lambda: "low", priority=10)
    q.enqueue("high", lambda: "high", priority=1)
    q.enqueue("mid", lambda: "mid", priority=5)
    tasks = q.run_all()
    assert [t.task_id for t in tasks] == ["high", "mid", "low"]


def test_run_all_returns_all():
    q = TaskQueue()
    q.enqueue("a", lambda: 1)
    q.enqueue("b", lambda: 2)
    q.enqueue("c", lambda: 3)
    results = q.run_all()
    assert len(results) == 3
    assert all(t.status == TaskStatus.DONE for t in results)


def test_run_all_results():
    q = TaskQueue()
    q.enqueue("double", lambda: 4 * 2)
    results = q.run_all()
    assert results[0].result == 8


def test_cancel():
    q = TaskQueue()
    q.enqueue("t1", lambda: 1)
    assert q.cancel("t1") is True
    assert q.get("t1").status == TaskStatus.CANCELLED
    assert q.pop() is None  # cancelled tasks are skipped


def test_cancel_nonexistent():
    q = TaskQueue()
    assert q.cancel("nonexistent") is False


def test_get():
    q = TaskQueue()
    q.enqueue("t1", lambda: 1)
    task = q.get("t1")
    assert task is not None
    assert task.task_id == "t1"


def test_get_missing():
    q = TaskQueue()
    assert q.get("missing") is None


def test_size():
    q = TaskQueue()
    assert q.size() == 0
    q.enqueue("a", lambda: 1)
    q.enqueue("b", lambda: 2)
    assert q.size() == 2
    q.pop()
    assert q.size() == 1


def test_pending():
    q = TaskQueue()
    q.enqueue("a", lambda: 1, priority=5)
    q.enqueue("b", lambda: 2, priority=1)
    pending = q.pending()
    assert pending[0].task_id == "b"  # lower priority number first


def test_done():
    q = TaskQueue()
    q.enqueue("a", lambda: 1)
    q.run_all()
    assert len(q.done()) == 1


def test_failed():
    q = TaskQueue()
    q.enqueue("bad", lambda: 1 / 0)
    q.run_all()
    assert len(q.failed()) == 1


def test_clear():
    q = TaskQueue()
    q.enqueue("a", lambda: 1)
    q.clear()
    assert q.size() == 0
    assert q.pop() is None


def test_enqueue_with_args():
    q = TaskQueue()
    q.enqueue("add", lambda a, b: a + b, args=(3, 4))
    results = q.run_all()
    assert results[0].result == 7


def test_enqueue_with_kwargs():
    q = TaskQueue()
    q.enqueue("greet", lambda name="world": f"hello {name}", kwargs={"name": "Alice"})
    results = q.run_all()
    assert results[0].result == "hello Alice"


def test_enqueue_chaining():
    q = TaskQueue()
    result = q.enqueue("a", lambda: 1).enqueue("b", lambda: 2)
    assert result is q
    assert q.size() == 2


def test_task_status_enum():
    assert TaskStatus.PENDING.value == "pending"
    assert TaskStatus.DONE.value == "done"
    assert TaskStatus.FAILED.value == "failed"


def test_peek_does_not_remove():
    q = TaskQueue()
    q.enqueue("t1", lambda: 1)
    task = q.peek()
    assert task is not None
    assert q.size() == 1  # still there
