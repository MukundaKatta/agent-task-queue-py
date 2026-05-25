# agent-task-queue-py

Priority queue for agent sub-tasks. Enqueue work items with priority, execute them in order, and track results.

## Install

```bash
pip install agent-task-queue-py
```

## Usage

```python
from agent_task_queue import TaskQueue, TaskStatus

queue = TaskQueue()
queue.enqueue("urgent", fix_critical, priority=-1)
queue.enqueue("fetch", fetch_data, args=("url",), priority=0)
queue.enqueue("summarize", summarize, priority=10)

# Run all in priority order
results = queue.run_all()
for task in results:
    if task.status == TaskStatus.DONE:
        print(task.task_id, task.result)
    elif task.status == TaskStatus.FAILED:
        print(task.task_id, task.error)

# Or run one at a time
task = queue.pop()
queue.execute(task)

# Cancel a pending task
queue.cancel("summarize")

# Inspect
queue.size()       # pending count
queue.pending()    # pending tasks in priority order
queue.done()       # completed tasks
queue.failed()     # failed tasks
```

## License

MIT
