# agent-task-queue-py

A tiny, dependency-free **priority queue for agent sub-tasks**. Enqueue work
items with a priority, execute them in order, and inspect their results and
failures. Useful when an agent fans out into many sub-tasks (fetch, summarize,
fix, etc.) and you want deterministic, priority-ordered, synchronous execution
without pulling in Celery, RQ, or an event loop.

- Pure standard library — no runtime dependencies.
- Deterministic ordering: lower priority number runs first, ties broken FIFO.
- Failures are captured, not raised, so one bad task does not abort the batch.
- Fully type-hinted and ships a `py.typed` marker.

## Install

```bash
pip install agent-task-queue-py
```

## Usage

```python
from agent_task_queue import TaskQueue, TaskStatus

def fetch_data(url):
    return f"data from {url}"

def summarize(text="..."):
    return text.upper()

def fix_critical():
    return "fixed"

queue = TaskQueue()
queue.enqueue("urgent", fix_critical, priority=-1)            # runs first
queue.enqueue("fetch", fetch_data, args=("http://x",), priority=0)
queue.enqueue("summarize", summarize, priority=10)           # runs last

# Run everything in priority order. Failures are captured, not raised.
for task in queue.run_all():
    if task.status == TaskStatus.DONE:
        print(task.task_id, "->", task.result)
    elif task.status == TaskStatus.FAILED:
        print(task.task_id, "failed:", task.error)

# Or drive execution one task at a time:
queue.enqueue("later", summarize, args=("hello",))
task = queue.pop()          # highest-priority pending task, now RUNNING
queue.execute(task)         # populates task.result / task.error
print(task.result)          # "HELLO"
```

Output of the `run_all()` loop above:

```
urgent -> fixed
fetch -> data from http://x
summarize -> ...
```

### Priorities

`priority` is an integer where **lower numbers run first** (a min-heap).
Negative values are allowed, so `priority=-1` runs before `priority=0`. Tasks
that share a priority run in the order they were enqueued (stable FIFO).

### Cancelling and inspecting

```python
queue.cancel("summarize")   # only PENDING tasks can be cancelled -> bool

queue.size()       # number of pending tasks
queue.peek()       # highest-priority pending task, without removing it
queue.pending()    # pending tasks in priority order
queue.done()       # completed tasks
queue.failed()     # failed tasks
queue.get("fetch") # look up any task by id
queue.clear()      # drop every task
```

## API

### `TaskQueue`

| Method | Description |
| --- | --- |
| `enqueue(task_id, fn, args=(), kwargs=None, priority=0, metadata=None)` | Add a task. Returns `self` for chaining. Raises `ValueError` if `task_id` already exists. |
| `pop() -> Task \| None` | Remove and return the highest-priority pending task (marking it `RUNNING`), or `None` if the queue has no pending tasks. |
| `peek() -> Task \| None` | Return the highest-priority pending task **without** removing it or changing its status. |
| `execute(task) -> Task` | Run `task.fn(*args, **kwargs)`, set `status`/`result`/`error` in place, and return the task. Exceptions are captured on `task.error`, never raised. |
| `run_all() -> list[Task]` | Pop and execute every pending task in priority order; return the executed tasks. Cancelled tasks are skipped. |
| `cancel(task_id) -> bool` | Mark a pending task `CANCELLED`. Returns `True` only if a pending task with that id existed. |
| `get(task_id) -> Task \| None` | Look up a task by id regardless of status. |
| `pending() -> list[Task]` | Pending tasks in priority order. |
| `done() -> list[Task]` | Tasks that finished successfully. |
| `failed() -> list[Task]` | Tasks that raised during execution. |
| `size() -> int` | Number of pending tasks. |
| `clear() -> None` | Remove all tasks. |

### `Task`

A dataclass describing a queued unit of work. Notable fields:

| Field | Type | Description |
| --- | --- | --- |
| `task_id` | `str` | Unique identifier; used by `get` and `cancel`. |
| `fn` | `Callable` | The callable to run. |
| `args` / `kwargs` | `tuple` / `dict` | Arguments passed to `fn`. |
| `priority` | `int` | Lower runs first; defaults to `0`. |
| `status` | `TaskStatus` | Current lifecycle state. |
| `result` | `Any` | Return value once `DONE`. |
| `error` | `Exception \| None` | Captured exception once `FAILED`. |
| `metadata` | `dict` | Free-form data you attach at enqueue time. |
| `created_at` | `float` | `time.time()` at creation. |

### `TaskStatus`

An `enum.Enum` with the values `PENDING`, `RUNNING`, `DONE`, `FAILED`, and
`CANCELLED`.

## Notes

- **Unique ids.** `task_id` is treated as a unique key. Enqueuing a duplicate id
  raises `ValueError` so the internal heap and id index can never drift out of
  sync (which previously caused a task to run twice).
- **Failures are captured.** `execute` and `run_all` never raise from your task
  code; inspect `task.status`, `task.result`, and `task.error` instead.
- **Thread safety.** Mutating operations are guarded by an internal lock, but
  `run_all` executes tasks sequentially in the calling thread.

## Development

Run the test suite with the standard library only — no third-party deps:

```bash
python -m unittest discover -s tests
```

## License

MIT
