import collections
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
      "Input MB: %s (from %s), Output MB: %s, Straggers: %s, HDFS straggers: %s") %
      (len(self.tasks), self.average_task_runtime(), max_task_runtime, self.start_time,
       self.finish_time() - self.start_time, self.input_mb(), input_method, self.output_mb(),
       self.total_stragglers(), self.hdfs_read_stragglers()[0]))

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

  def total_stragglers(self):
    """ Returns the total number of straggler tasks for this stage. """
    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    stragglers = [t for t in self.tasks if t.runtime() >= 1.5*median_task_duration]
    return len(stragglers)

  def total_straggler_runtime(self):
    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    return sum([t.runtime() for t in self.tasks if t.runtime() >= 1.5*median_task_duration])

  def progress_rate_stragglers(self):
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

  def hdfs_read_stragglers(self):
    """ Returns the number of and total time taken by stragglers caused by slow HDFS reads. """
    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    stragglers = [t for t in self.tasks if t.runtime() >= 1.5*median_task_duration]
    times_wo_input_read = [t.runtime() - t.input_read_time for t in self.tasks]
    median_time_wo_input_read = numpy.median(times_wo_input_read)
    read_stragglers = [t for t in stragglers if t.runtime() - t.input_read_time < 1.5 * median_time_wo_input_read]
    read_stragglers_time = sum([t.runtime() for t in read_stragglers])
    for t in read_stragglers:
      t.straggler_behavior_explained = True
    return len(read_stragglers), read_stragglers_time

  def get_stragglers(self):
    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    stragglers = [t for t in self.tasks if t.runtime() >= 1.5*median_task_duration]
    return stragglers

  def network_stragglers(self):
    """ Returns the number of and total time taken by stragglers caused by poor network performance.

    Does not account for tasks that have poor network performance because they read input data from
    a remote data node; only accounts for tasks with slow networks during the shuffle phase.
    """
    times_wo_network = [t.runtime() - t.fetch_wait for t in self.tasks if t.has_fetch]
    if len(times_wo_network) == 0:
      # This stage is not a reduce stage, so there's no shuffle and therefore no network stragglers.
      return 0, 0
    median_time_wo_network = numpy.median(times_wo_network)
    network_stragglers = [t for t in self.get_stragglers()
      if t.has_fetch and (t.runtime() - t.fetch_wait) < 1.5 * median_time_wo_network]
    for t in network_stragglers:
      t.straggler_behavior_explained = True
    network_stragglers_time = sum([t.runtime() for t in network_stragglers])
    return len(network_stragglers), network_stragglers_time

  def scheduler_delay_stragglers(self):
    times_wo_scheduler_delay = [t.runtime() - t.scheduler_delay for t in self.tasks]
    median_time_wo_scheduler_delay = numpy.median(times_wo_scheduler_delay)
    print len(self.tasks), "Median time wo scheduler dleay: ", median_time_wo_scheduler_delay
    scheduler_delay_stragglers = [t for t in self.get_stragglers()
      if (t.runtime() - t.scheduler_delay) < 1.5 * median_time_wo_scheduler_delay]
    for t in scheduler_delay_stragglers:
      t.straggler_behavior_explained = True
    scheduler_delay_stragglers_time = sum([t.runtime() for t in scheduler_delay_stragglers])
    return len(scheduler_delay_stragglers), scheduler_delay_stragglers_time

  def gc_stragglers(self):
    times_wo_gc = [t.runtime() - t.gc_time for t in self.tasks]
    median_time_wo_gc = numpy.median(times_wo_gc)
    gc_stragglers = [t for t in self.get_stragglers()
      if (t.runtime() - t.gc_time) < 1.5 * median_time_wo_gc]
    for t in gc_stragglers:
      t.straggler_behavior_explained = True
    gc_stragglers_time = sum([t.runtime() for t in gc_stragglers])
    return len(gc_stragglers), gc_stragglers_time

  def jit_stragglers(self):
    executor_to_task_finish_times = collections.defaultdict(list)
    for task in self.tasks:
      executor_to_task_finish_times[task.executor].append(task.finish_time)

    # Tasks where no other task in the same stage completed on the executor
    # before this task started.
    virgin_tasks = [] 
    for task in self.tasks:
      first_task_finish_on_executor = min(executor_to_task_finish_times[task.executor])
      if task.start_time < first_task_finish_on_executor:
        virgin_tasks.append(task)

    median_task_duration = numpy.median([t.runtime() for t in self.tasks])
    median_virgin_task_duration = numpy.median([t.runtime() for t in virgin_tasks])
    jit_stragglers = 0
    for task in virgin_tasks:
      if task.runtime() >= 1.5*median_task_duration:
        if task.runtime() < 1.5*median_virgin_task_duration:
          jit_stragglers += 1
          task.straggler_behavior_explained = True
    return jit_stragglers

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




