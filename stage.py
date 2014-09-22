import collections
import numpy
from task import Task
import concurrency

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
      "Max concurrency: %s, "
      "Input MB: %s (from %s), Output MB: %s, Straggers: %s, Progress rate straggers: %s, "
      "Progress rate stragglers explained by scheduler delay (%s), HDFS read (%s), "
      "HDFS and read (%s), GC (%s), Network (%s), JIT (%s), output rate stragglers: %s") %
      (len(self.tasks), self.average_task_runtime(), max_task_runtime, self.start_time,
       self.finish_time() - self.start_time, concurrency.get_max_concurrency(self.tasks),
       self.input_mb(), input_method, self.output_mb(),
       self.traditional_stragglers(), self.progress_rate_stragglers()[0],
       self.scheduler_delay_stragglers()[0], self.hdfs_read_stragglers()[0],
       self.hdfs_read_and_scheduler_delay_stragglers()[0], self.gc_stragglers()[0],
       self.network_stragglers()[0], self.jit_stragglers()[0],
       self.output_progress_rate_stragglers()[0]))

  def verbose_str(self):
    # Get info about the longest task.
    max_index = -1
    max_runtime = -1
    for i, task in enumerate(self.tasks):
      if task.runtime() > max_runtime:
        max_runtime = task.runtime()
        max_index = i
    return "%s\n    Longest Task: %s" % (self, self.tasks[i])    

  def conservative_finish_time(self):
    # Subtract scheduler delay to account for asynchrony in the scheduler where sometimes tasks
    # aren't marked as finished until a few ms later.
    return max([(t.finish_time - t.scheduler_delay) for t in self.tasks])

  def finish_time(self):
    return max([t.finish_time for t in self.tasks])

  def total_runtime(self):
    return sum([t.finish_time - t.start_time for t in self.tasks])

  def total_fetch_wait(self):
    return sum([t.fetch_wait for t in self.tasks if t.has_fetch])

  def total_runtime_no_remote_shuffle_read(self):
    return sum([t.runtime_no_remote_shuffle_read() for t in self.tasks])

  def total_time_fetching(self):
    return sum([t.total_time_fetching for t in self.tasks if t.has_fetch])

  def total_disk_read_time(self):
    return sum([t.remote_disk_read_time for t in self.tasks if t.has_fetch])

  def traditional_stragglers(self):
    """ Returns the total number of straggler tasks for this stage using the traditional metric.
    
    This method considers a task a straggler if its runtime is at least 1.5 times the median
    runtime for other tasks in the stage. """
    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    stragglers = [t for t in self.tasks if t.runtime() >= 1.5*median_task_duration]
    return len(stragglers)

  def total_traditional_straggler_runtime(self):
    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    return sum([t.runtime() for t in self.tasks if t.runtime() >= 1.5*median_task_duration])

  def traditional_stragglers_explained_by_progress_rate(self):
    """ Returns the number of stragglers that can be explained by the amount of data they process.
    
    We describe a straggler as "explainable" by the amount of data it processes if it is not
    a straggler based on its progress rate.
    """
    progress_rates = [t.runtime() * 1.0 / t.input_size_mb()
      for t in self.tasks if t.input_size_mb() > 0]
    median_progress_rate = numpy.median(progress_rates)
    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    progress_rate_stragglers = 0
    progress_rate_stragglers_total_time = 0
    for task in self.tasks:
      if task.runtime() >= 1.5*median_task_duration and task.input_size_mb() > 0:
        progress_rate = task.runtime() * 1.0 / task.input_size_mb()
        if progress_rate < 1.5*median_progress_rate:
          task.straggler_behavior_explained = True
          progress_rate_stragglers += 1
          progress_rate_stragglers_total_time += task.runtime()
    return progress_rate_stragglers, progress_rate_stragglers_total_time

  def progress_rate_stragglers(self):
    stragglers = self.get_progress_rate_stragglers()
    return len(stragglers), sum([t.runtime() for t in stragglers])

  def get_tasks_with_non_zero_input(self):
    return [t for t in self.tasks if t.input_size_mb() > 0]

  def get_progress_rate_stragglers(self):
    progress_rates = [t.runtime() * 1.0 / t.input_size_mb()
      for t in self.get_tasks_with_non_zero_input()]
    median_progress_rate = numpy.median(progress_rates)
    progress_rate_stragglers = [t for t in self.get_tasks_with_non_zero_input()
      if t.runtime() * 1.0 / t.input_size_mb() >= 1.5*median_progress_rate]
    return progress_rate_stragglers

  def get_attributable_stragglers(self, progress_rate_fn):
    """ Returns the progress rate stragglers that are no longer stragglers
    when the provided function is used to compute the progress rate."""
    new_progress_rates = [progress_rate_fn(t) for t in self.get_tasks_with_non_zero_input()]
    median_new_progress_rate = numpy.median(new_progress_rates)
    attributable_stragglers = [t for t in self.get_progress_rate_stragglers()
      if progress_rate_fn(t) < 1.5 * median_new_progress_rate]
    for t in attributable_stragglers:
      t.straggler_behavior_explained = True
    return attributable_stragglers

  def get_attributable_stragglers_stats(self, progress_rate_fn):
    """ Returns the number of and total time taken by attributable stragglers. """
    attributable_stragglers = self.get_attributable_stragglers(progress_rate_fn)
    return len(attributable_stragglers), sum([t.runtime() for t in attributable_stragglers])

  def output_progress_rate_stragglers(self):
    "Returns stats about stragglers that can be attributed to output data size. """
    return 0, 0
    def progress_rate_based_on_output(task):
      return (task.runtime() / (task.shuffle_mb_written + task.output_mb))
    attributable_stragglers = self.get_attributable_stragglers(progress_rate_based_on_output)
    straggler_time = sum([t.runtime() for t in attributable_stragglers])
    return len(attributable_stragglers), straggler_time

  def hdfs_read_stragglers(self):
    """ Returns the number of and total time taken by stragglers caused by slow HDFS reads,
    as well as the number of those stragglers that had non-local reads.
    
    Considers a task a straggler if its processing rate is more than 1.5x the median. """
    def progress_rate_wo_hdfs_read(task):
      return (task.runtime() - task.input_read_time) * 1.0 / task.input_size_mb()
    attributable_stragglers = self.get_attributable_stragglers(progress_rate_wo_hdfs_read)
    non_local = len([t for t in attributable_stragglers if not t.data_local])
    straggler_time = sum([t.runtime() for t in attributable_stragglers])
    return len(attributable_stragglers), straggler_time, non_local

  def hdfs_write_stragglers(self):
    """ Returns the number of and total time taken by stragglers caused by slow HDFS writes,
    as well as the number of those stragglers that had non-local reads.

    Considers a task a straggler if its processing rate is more than 1.5x the median. """
    def progress_rate_wo_hdfs_write(task):
      return (task.runtime() - task.output_write_time) * 1.0 / task.input_size_mb()
    attributable_stragglers = self.get_attributable_stragglers(progress_rate_wo_hdfs_write)
    straggler_time = sum([t.runtime() for t in attributable_stragglers])
    return len(attributable_stragglers), straggler_time

  def network_stragglers(self):
    """ Returns the number of and total time taken by stragglers caused by poor network performance.

    Does not account for tasks that have poor network performance because they read input data from
    a remote data node; only accounts for tasks with slow networks during the shuffle phase.
    """
    def progress_rate_wo_network(task):
      return (task.runtime() - task.fetch_wait) * 1.0 / task.input_size_mb()
    if not self.tasks[0].has_fetch:
      # If the first task doesn't have a fetch none of them should, so there can't be any
      # network stragglers.
      return 0, 0
    return self.get_attributable_stragglers_stats(progress_rate_wo_network)

  def scheduler_delay_stragglers(self):
    def progress_rate_wo_scheduler_delay(task):
      return (task.runtime() - task.scheduler_delay) * 1.0 / task.input_size_mb()
    return self.get_attributable_stragglers_stats(progress_rate_wo_scheduler_delay)

  def hdfs_read_and_scheduler_delay_stragglers(self):
    def progress_rate_wo_read_and_sched(task):
      return ((task.runtime() - task.scheduler_delay - task.input_read_time) * 1.0 /
        task.input_size_mb())
    return self.get_attributable_stragglers_stats(progress_rate_wo_read_and_sched)

  def gc_stragglers(self):
    def progress_rate_wo_gc(task):
      return (task.runtime() - task.gc_time) * 1.0 / task.input_size_mb()
    return self.get_attributable_stragglers_stats(progress_rate_wo_gc)

  def shuffle_write_stragglers(self):
    def progress_rate_wo_shuffle_write(task):
      return (task.runtime() - task.shuffle_write_time) * 1.0 / task.input_size_mb()
    return self.get_attributable_stragglers_stats(progress_rate_wo_shuffle_write)

  def jit_stragglers(self):
    executor_to_task_finish_times = collections.defaultdict(list)
    for task in self.tasks:
      executor_to_task_finish_times[task.executor].append(task.finish_time)

    # Tasks where no other task in the same stage completed on the executor
    # before this task started.
    virgin_tasks = [] 
    for task in self.get_tasks_with_non_zero_input():
      first_task_finish_on_executor = min(executor_to_task_finish_times[task.executor])
      if task.start_time < first_task_finish_on_executor:
        virgin_tasks.append(task)

    def progress_rate(task):
      return task.runtime() * 1.0 / task.input_size_mb()

    median_task_progress_rate = numpy.median([progress_rate(t)
      for t in self.get_tasks_with_non_zero_input()])
    median_virgin_task_progress_rate = numpy.median([progress_rate(t) for t in virgin_tasks])
    jit_stragglers = 0
    total_time = 0
    for task in virgin_tasks:
      task_progress_rate = progress_rate(task)
      if task_progress_rate >= 1.5*median_task_progress_rate:
        if task_progress_rate < 1.5*median_virgin_task_progress_rate:
          jit_stragglers += 1
          total_time += task.runtime()
          task.straggler_behavior_explained = True
    return jit_stragglers, total_time

  def task_runtimes_with_median_progress_rate(self):
    """ Returns task runtimes using the Dolly method to eliminate stragglers.

    Replaces each task's runtime with the runtime based on the median progress rate
    for the stage. """
    # Ignore tasks with 0 input bytes when computing progress rates. These seem to occur for the
    # big shuffle to partition the data (even though the blocks read are non-zero for those tasks).
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




