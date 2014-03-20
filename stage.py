from task import Task

class Stage:
  def __init__(self):
    self.start_time = -1
    self.tasks = []

  def __str__(self):
    return "%s tasks Start: %s, finish: %s" % (len(self.tasks), self.start_time, self.finish_time())

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


