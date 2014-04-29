import numpy
from task import Task

class Stage:
  def __init__(self):
    self.start_time = -1
    self.tasks = []

  def average_task_runtime(self):
    return sum([t.runtime() for t in self.tasks]) * 1.0 / len(self.tasks)

  def __str__(self):
    max_task_runtime = max([t.runtime() for t in self.tasks])
    if self.tasks[0].has_fetch:
      input_method = "shuffle"
    else:
      input_method = self.tasks[0].input_read_method
    return (("%s tasks (avg runtime: %s, max runtime: %s) Start: %s, runtime: %s, "
      "Input MB: %s (from %s), Output MB: %s") %
      (len(self.tasks), self.average_task_runtime(), max_task_runtime, self.start_time,
       self.finish_time() - self.start_time, self.input_mb(), input_method, self.output_mb()))

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
    return sum([t.remote_disk_read_time for t in self.tasks if t.has_fetch])

  def task_runtimes_with_median_progress_rate(self):
    """ Returns task runtimes using the Dolly method to eliminate stragglers.

    Replaces each task's runtime with the runtime based on the median progress rate
    for the stage. """
    # Ignore tasks with 0 input bytes when computing progress rates. These seem to occur for the
    # big shuffle to partition the data (even though the blocks read are non-zero for those tasks).
    # TODO: Figure out what's going on with the zero-input-mb tasks.
    progress_rates = [t.runtime() * 1.0 / t.input_size_mb() for t in self.tasks
      if t.input_size_mb() > 0]
    median_progress_rate = numpy.median(progress_rates)
    runtimes = [t.input_size_mb() * median_progress_rate for t in self.tasks]
    return runtimes

  def input_mb(self):
    """ Returns the total input size for this stage.
    
    This is only valid if the stage read data from a shuffle.
    """
    total_input_bytes = sum([t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch])
    total_input_bytes += sum([t.input_mb for t in self.tasks])
    return total_input_bytes

  def output_mb(self):
    """ Returns the total output size for this stage.

    This is only valid if the output data was written for a shuffle.
    TODO: Add HDFS / in-memory RDD output size.
    """
    total_output_size = sum([t.shuffle_mb_written for t in self.tasks])
    return total_output_size

  def add_event(self, line):
    if line.find("TASK_TYPE") == -1:
      return

    task = Task(line)
    if self.start_time == -1:
      self.start_time = task.start_time
    else:
      self.start_time = min(self.start_time, task.start_time)

    self.tasks.append(task)




