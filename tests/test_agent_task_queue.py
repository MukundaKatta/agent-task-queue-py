"""Tests for agent-task-queue-py.

Written against the Python standard-library :mod:`unittest` framework so they
run with no third-party dependencies::

    python3 -m unittest discover -s tests
"""

import unittest
from unittest import mock

from agent_task_queue import Task, TaskQueue, TaskStatus


class EnqueueAndPopTests(unittest.TestCase):
    def test_enqueue_and_pop(self):
        q = TaskQueue()
        q.enqueue("t1", lambda: 42)
        task = q.pop()
        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, "t1")
        # pop transitions the task to RUNNING
        self.assertEqual(task.status, TaskStatus.RUNNING)

    def test_pop_returns_none_when_empty(self):
        q = TaskQueue()
        self.assertIsNone(q.pop())

    def test_enqueue_with_args(self):
        q = TaskQueue()
        q.enqueue("add", lambda a, b: a + b, args=(3, 4))
        results = q.run_all()
        self.assertEqual(results[0].result, 7)

    def test_enqueue_with_kwargs(self):
        q = TaskQueue()
        q.enqueue(
            "greet",
            lambda name="world": f"hello {name}",
            kwargs={"name": "Alice"},
        )
        results = q.run_all()
        self.assertEqual(results[0].result, "hello Alice")

    def test_enqueue_chaining(self):
        q = TaskQueue()
        result = q.enqueue("a", lambda: 1).enqueue("b", lambda: 2)
        self.assertIs(result, q)
        self.assertEqual(q.size(), 2)

    def test_enqueue_metadata_is_stored(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1, metadata={"trace": "abc"})
        self.assertEqual(q.get("a").metadata, {"trace": "abc"})

    def test_duplicate_task_id_raises(self):
        # Regression: a duplicate id used to desync the heap from the id index,
        # so the same id ran twice while size()/pending() reported one entry.
        q = TaskQueue()
        q.enqueue("dup", lambda: 1)
        with self.assertRaises(ValueError):
            q.enqueue("dup", lambda: 2)
        # State stays consistent: still exactly one task, run once.
        self.assertEqual(q.size(), 1)
        results = q.run_all()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].result, 1)

    def test_duplicate_id_allowed_after_clear(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1)
        q.clear()
        # After clearing, the id is free to reuse.
        q.enqueue("a", lambda: 2)
        self.assertEqual(q.run_all()[0].result, 2)


class ExecuteTests(unittest.TestCase):
    def test_execute_success(self):
        q = TaskQueue()
        q.enqueue("t1", lambda: 99)
        task = q.pop()
        result = q.execute(task)
        self.assertEqual(result.status, TaskStatus.DONE)
        self.assertEqual(result.result, 99)
        self.assertIsNone(result.error)

    def test_execute_failure(self):
        def bad():
            raise RuntimeError("oops")

        q = TaskQueue()
        q.enqueue("bad", bad)
        task = q.pop()
        result = q.execute(task)
        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertIsInstance(result.error, RuntimeError)

    def test_execute_returns_same_task(self):
        q = TaskQueue()
        q.enqueue("t1", lambda: 1)
        task = q.pop()
        self.assertIs(q.execute(task), task)

    def test_execute_passes_args_and_kwargs(self):
        q = TaskQueue()
        q.enqueue("f", lambda a, b, c=0: a + b + c, args=(1, 2), kwargs={"c": 3})
        task = q.pop()
        self.assertEqual(q.execute(task).result, 6)


class PriorityOrderingTests(unittest.TestCase):
    def test_priority_ordering(self):
        q = TaskQueue()
        q.enqueue("low", lambda: "low", priority=10)
        q.enqueue("high", lambda: "high", priority=1)
        q.enqueue("mid", lambda: "mid", priority=5)
        tasks = q.run_all()
        self.assertEqual([t.task_id for t in tasks], ["high", "mid", "low"])

    def test_negative_priority_runs_first(self):
        q = TaskQueue()
        q.enqueue("normal", lambda: 1, priority=0)
        q.enqueue("urgent", lambda: 2, priority=-5)
        self.assertEqual([t.task_id for t in q.run_all()], ["urgent", "normal"])

    def test_fifo_within_same_priority(self):
        # Force identical timestamps so ordering must fall back to insertion order.
        with mock.patch("agent_task_queue.time.time", lambda: 1000.0):
            q = TaskQueue()
            ids = [f"t{i:02d}" for i in range(30)]
            for tid in ids:
                q.enqueue(tid, lambda: None, priority=0)
            self.assertEqual([t.task_id for t in q.run_all()], ids)

    def test_priority_then_fifo_ordering(self):
        with mock.patch("agent_task_queue.time.time", lambda: 1000.0):
            q = TaskQueue()
            q.enqueue("a", lambda: None, priority=5)
            q.enqueue("b", lambda: None, priority=1)
            q.enqueue("c", lambda: None, priority=1)
            q.enqueue("d", lambda: None, priority=5)
            # Lower priority number first; ties broken by insertion order.
            self.assertEqual(
                [t.task_id for t in q.run_all()], ["b", "c", "a", "d"]
            )

    def test_task_ordering_dunder(self):
        # Lower (priority, seq) compares as "less than".
        a = Task("a", lambda: None, priority=1, seq=0)
        b = Task("b", lambda: None, priority=2, seq=0)
        c = Task("c", lambda: None, priority=1, seq=1)
        self.assertLess(a, b)
        self.assertLess(a, c)  # same priority, earlier seq


class RunAllTests(unittest.TestCase):
    def test_run_all_returns_all(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1)
        q.enqueue("b", lambda: 2)
        q.enqueue("c", lambda: 3)
        results = q.run_all()
        self.assertEqual(len(results), 3)
        self.assertTrue(all(t.status == TaskStatus.DONE for t in results))

    def test_run_all_results(self):
        q = TaskQueue()
        q.enqueue("double", lambda: 4 * 2)
        results = q.run_all()
        self.assertEqual(results[0].result, 8)

    def test_run_all_empty_queue(self):
        self.assertEqual(TaskQueue().run_all(), [])

    def test_run_all_skips_cancelled(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1)
        q.enqueue("b", lambda: 2)
        q.cancel("a")
        results = q.run_all()
        self.assertEqual([t.task_id for t in results], ["b"])


class CancelTests(unittest.TestCase):
    def test_cancel(self):
        q = TaskQueue()
        q.enqueue("t1", lambda: 1)
        self.assertTrue(q.cancel("t1"))
        self.assertEqual(q.get("t1").status, TaskStatus.CANCELLED)
        self.assertIsNone(q.pop())  # cancelled tasks are skipped

    def test_cancel_nonexistent(self):
        q = TaskQueue()
        self.assertFalse(q.cancel("nonexistent"))

    def test_cancel_already_running_returns_false(self):
        q = TaskQueue()
        q.enqueue("t1", lambda: 1)
        q.pop()  # now RUNNING
        # Only PENDING tasks can be cancelled.
        self.assertFalse(q.cancel("t1"))


class InspectionTests(unittest.TestCase):
    def test_get(self):
        q = TaskQueue()
        q.enqueue("t1", lambda: 1)
        task = q.get("t1")
        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, "t1")

    def test_get_missing(self):
        q = TaskQueue()
        self.assertIsNone(q.get("missing"))

    def test_size(self):
        q = TaskQueue()
        self.assertEqual(q.size(), 0)
        q.enqueue("a", lambda: 1)
        q.enqueue("b", lambda: 2)
        self.assertEqual(q.size(), 2)
        q.pop()
        self.assertEqual(q.size(), 1)

    def test_pending(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1, priority=5)
        q.enqueue("b", lambda: 2, priority=1)
        pending = q.pending()
        self.assertEqual(pending[0].task_id, "b")  # lower priority number first

    def test_done(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1)
        q.run_all()
        self.assertEqual(len(q.done()), 1)

    def test_failed(self):
        q = TaskQueue()
        q.enqueue("bad", lambda: 1 / 0)
        q.run_all()
        self.assertEqual(len(q.failed()), 1)

    def test_clear(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1)
        q.clear()
        self.assertEqual(q.size(), 0)
        self.assertIsNone(q.pop())


class PeekTests(unittest.TestCase):
    def test_peek_does_not_remove(self):
        q = TaskQueue()
        q.enqueue("t1", lambda: 1)
        task = q.peek()
        self.assertIsNotNone(task)
        self.assertEqual(q.size(), 1)  # still there
        self.assertEqual(task.status, TaskStatus.PENDING)  # not transitioned

    def test_peek_returns_highest_priority(self):
        q = TaskQueue()
        q.enqueue("low", lambda: None, priority=10)
        q.enqueue("high", lambda: None, priority=1)
        self.assertEqual(q.peek().task_id, "high")

    def test_peek_empty_returns_none(self):
        self.assertIsNone(TaskQueue().peek())

    def test_peek_skips_cancelled(self):
        q = TaskQueue()
        q.enqueue("a", lambda: 1, priority=1)
        q.enqueue("b", lambda: 2, priority=5)
        q.cancel("a")
        self.assertEqual(q.peek().task_id, "b")


class TaskStatusTests(unittest.TestCase):
    def test_task_status_enum(self):
        self.assertEqual(TaskStatus.PENDING.value, "pending")
        self.assertEqual(TaskStatus.DONE.value, "done")
        self.assertEqual(TaskStatus.FAILED.value, "failed")
        self.assertEqual(TaskStatus.RUNNING.value, "running")
        self.assertEqual(TaskStatus.CANCELLED.value, "cancelled")


if __name__ == "__main__":
    unittest.main()
