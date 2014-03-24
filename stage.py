from task import Task

class Stage:
  def __init__(self):
    self.start_time = -1
    self.tasks = []

  def __str__(self):
    avg_task_runtime = sum([t.runtime() for t in self.tasks]) * 1.0 / len(self.tasks)
    max_task_runtime = max([t.runtime() for t in self.tasks])
    return ("%s tasks (avg runtime: %s, max runtime: %s) Start: %s, runtime: %s" %
      (len(self.tasks), avg_task_runtime, max_task_runtime, self.start_time,
       self.finish_time() - self.start_time))

  def verbose_str(self):
    # Get info about the longest task.
    max_index = -1
    max_runtime = -1
    for i, task in enumerate(self.tasks):
      if task.runtime() > max_runtime:
        max_runtime = task.runtime()
        max_index = i
    return "%s\n    Longest Task: %s" % (self, self.tasks[i])
    

  def finish_time(self):
    return max([t.finish_time for t in self.tasks])

  def total_runtime(self):
    return sum([t.finish_time - t.start_time for t in self.tasks])

  def total_fetch_wait(self):
    return sum([t.fetch_wait for t in self.tasks if t.has_fetch])

  def total_runtime_no_fetch(self):
    return sum([t.runtime_faster_fetch(0) for t in self.tasks])

  def total_time_fetching(self):
    return sum([t.total_time_fetching for t in self.tasks if t.has_fetch])

  def total_disk_read_time(self):
    return sum([t.total_disk_read_time for t in self.tasks if t.has_fetch])

  def add_event(self, line):
    if line.find("TASK_TYPE") == -1:
      return

    task = Task(line)
    if self.start_time == -1:
      self.start_time = task.start_time
    else:
      self.start_time = min(self.start_time, task.start_time)

    self.tasks.append(task)


