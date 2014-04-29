import collections
import logging
import math
import sys

import simulate
import stage
import task

""" Returns the "percent" percentile in the list N.

Assumes N is sorted.
"""
def get_percentile(N, percent, key=lambda x:x):
  if not N:
    return 0
  k = (len(N) - 1) * percent
  f = math.floor(k)
  c = math.ceil(k)
  if f == c:
    return key(N[int(k)])
  d0 = key(N[int(f)]) * (c-k)
  d1 = key(N[int(c)]) * (k-f)
  return d0 + d1

def write_cdf(values, filename):
  values.sort()
  f = open(filename, "w")
  for percent in range(100):
    fraction = percent / 100.
    f.write("%s\t%s\n" % (fraction, get_percentile(values, fraction)))
  f.close()

class Analyzer:
  def __init__(self, filename):
    self.logger = logging.getLogger("Analyzer")
    f = open(filename, "r")
    # Map of stage IDs to Stages.
    self.stages = collections.defaultdict(stage.Stage)
    for line in f:
      STAGE_ID_MARKER = "STAGE_ID="
      stage_id_loc = line.find(STAGE_ID_MARKER)
      if stage_id_loc != -1:
        stage_id_and_suffix = line[stage_id_loc + len(STAGE_ID_MARKER):]
        stage_id = stage_id_and_suffix[:stage_id_and_suffix.find(" ")]
        self.stages[stage_id].add_event(line)

    # Compute the amount of overlapped time between stages
    # (there should just be two stages, at the beginning, that overlap and run concurrently).
    # This computation assumes that not more than two stages overlap.
    print ["%s: %s tasks" % (id, len(s.tasks)) for id, s in self.stages.iteritems()]
    start_and_finish_times = [(id, s.start_time, s.finish_time())
        for id, s in self.stages.iteritems()]
    start_and_finish_times.sort(key = lambda x: x[1])
    self.overlap = 0
    old_end = 0
    previous_id = ""
    self.stages_to_combine = set()
    print "Start and finish times: ", start_and_finish_times
    for id, start, finish in start_and_finish_times:
      if start < old_end:
        self.overlap += old_end - start
        print "Overlap:", self.overlap, "between ", id, "and", previous_id
        self.stages_to_combine.add(id)
        self.stages_to_combine.add(previous_id)
        old_end = max(old_end, finish)
      if finish > old_end:
        old_end = finish
        previous_id = id

    print "Stages to combine: ", self.stages_to_combine

  def all_tasks(self):
    """ Returns a list of all tasks. """
    return [task for stage in self.stages.values() for task in stage.tasks]

  def print_stage_info(self):
    for id, stage in self.stages.iteritems():
      print "STAGE %s: %s" % (id, stage.verbose_str())

  def print_heading(self, text):
    print "\n******** %s ********" % text

  def get_simulated_runtime(self, waterfall_prefix=""):
    """ Returns the simulated runtime for the job.

    This should be approximately the same as the original runtime of the job, except
    that it doesn't include scheduler delay.

    If a non-empty waterfall_prefix is passed in, makes a waterfall plot based on the simulated
    runtimes.
    """
    total_runtime = 0
    tasks_for_combined_stages = []
    all_start_finish_times = []
    for id, stage in self.stages.iteritems():
      if id in self.stages_to_combine:
        tasks_for_combined_stages.extend(stage.tasks)
      else:
        tasks = sorted(stage.tasks, key = lambda task: task.start_time)
        simulated_runtime, start_finish_times = simulate.simulate([task.runtime() for task in tasks])
        start_finish_times_adjusted = [
          (start + total_runtime, finish + total_runtime) for start, finish in start_finish_times]
        all_start_finish_times.append(start_finish_times_adjusted)
        total_runtime += simulated_runtime
    if len(tasks_for_combined_stages) > 0:
      tasks = sorted(tasks_for_combined_stages, key = lambda task: task.start_time)
      simulated_runtime, start_finish_times = simulate.simulate([task.runtime() for task in tasks])
      start_finish_times_adjusted = [
        (start - simulated_runtime, finish - simulated_runtime) for start, finish in start_finish_times]
      all_start_finish_times.append(start_finish_times_adjusted)
      total_runtime += simulated_runtime

    if waterfall_prefix:
      self.write_simulated_waterfall(all_start_finish_times, "%s_simulated" % waterfall_prefix)
    return total_runtime 

  def simulated_runtime_over_actual(self, prefix):
    simulated_runtime = self.get_simulated_runtime(waterfall_prefix=prefix)
    # TODO: Incorporate Shark setup time here!
    actual_start_time = min([s.start_time for s in self.stages.values()])
    actual_finish_time = max([s.finish_time() for s in self.stages.values()])
    print "Simulated runtime: ", simulated_runtime, "actual time: ", actual_finish_time - actual_start_time
    return simulated_runtime * 1.0 / (actual_finish_time - actual_start_time)

  def no_stragglers_perfect_parallelism_speedup(self, num_slots=32):
    """ Returns how fast the job would have run if time were perfectly spread across 32 slots. """
    total_runtime = sum([s.total_runtime() for s in self.stages.values()])
    ideal_runtime = total_runtime * 1.0 / num_slots
    # Simulate the runtime rather than using the actual one to sidestep issues with # of slots.
    simulated_actual_runtime = self.get_simulated_runtime()
    return ideal_runtime / simulated_actual_runtime

  def no_stragglers_using_average_runtime_speedup(self, prefix):
    """ Returns how much faster the job would have run if there were no stragglers.

    Eliminates stragglers by replacing each task's runtime with the average runtime
    for tasks in the job.
    """
    self.print_heading("Computing speedup by averaging out stragglers")
    total_no_stragglers_runtime = 0
    averaged_runtimes_for_combined_stages = []
    all_start_finish_times = []
    for id, stage in self.stages.iteritems():
      averaged_runtimes = [stage.average_task_runtime()] * len(stage.tasks)
      if id in self.stages_to_combine:
        averaged_runtimes_for_combined_stages.extend(averaged_runtimes) 
      else:
        no_stragglers_runtime, start_finish_times = simulate.simulate(averaged_runtimes)
        # Adjust the start and finish times based on when the stage staged.
        start_finish_times_adjusted = [
          (start + total_no_stragglers_runtime, finish + total_no_stragglers_runtime) \
          for start, finish in start_finish_times]
        total_no_stragglers_runtime += no_stragglers_runtime
        all_start_finish_times.append(start_finish_times_adjusted)
    if len(averaged_runtimes_for_combined_stages) > 0:
      no_stragglers_runtime, start_finish_times = simulate.simulate(
        averaged_runtimes_for_combined_stages)
      # Adjust the start and finish times based on when the stage staged.
      # The subtraction is a hack to put the combined stages at the beginning, which
      # is when they usually occur.
      start_finish_times_adjusted = [
        (start - no_stragglers_runtime, finish - no_stragglers_runtime) for start, finish in start_finish_times]
      total_no_stragglers_runtime += no_stragglers_runtime
      all_start_finish_times.append(start_finish_times_adjusted)

    self.write_simulated_waterfall(all_start_finish_times, "%s_sim_no_stragglers" % prefix)
    return total_no_stragglers_runtime * 1.0 / self.get_simulated_runtime()

  def replace_95_stragglers_with_median_speedup(self):
    """ Returns how much faster the job would have run if there were no stragglers.

    Removes stragglers by replacing the longest 5% of tasks with the median runtime
    for tasks in the stage.
    """
    total_no_stragglers_runtime = 0
    runtimes_for_combined_stages = []
    for id, stage in self.stages.iteritems():
      runtimes = [task.runtime() for task in stage.tasks]
      runtimes.sort()
      median_runtime = get_percentile(runtimes, 0.5)
      threshold_runtime = get_percentile(runtimes, 0.95)
      no_straggler_runtimes = []
      for runtime in runtimes:
        if runtime >= threshold_runtime:
          no_straggler_runtimes.append(median_runtime)
        else:
          no_straggler_runtimes.append(runtime)
      if id in self.stages_to_combine:
        runtimes_for_combined_stages.extend(no_straggler_runtimes)
      else:
        no_stragglers_runtime = simulate.simulate(no_straggler_runtimes)[0]
        total_no_stragglers_runtime += no_stragglers_runtime
        original_runtime = simulate.simulate([task.runtime() for task in stage.tasks])[0]
        print "%s: Orig: %s, no stragg: %s" % (id, original_runtime, no_stragglers_runtime)
    if len(runtimes_for_combined_stages) > 0:
      total_no_stragglers_runtime += simulate.simulate(runtimes_for_combined_stages)[0]
    return total_no_stragglers_runtime * 1.0 / self.get_simulated_runtime()


  def replace_stragglers_with_median_speedup(self):
    """ Returns how much faster the job would have run if there were no stragglers.

    Removes stragglers by replacing tasks that took more than 50% longer than the median
    with the median runtime for tasks in the stage.
    """
    total_no_stragglers_runtime = 0
    runtimes_for_combined_stages = []
    for id, stage in self.stages.iteritems():
      runtimes = [task.runtime() for task in stage.tasks]
      runtimes.sort()
      median_runtime = get_percentile(runtimes, 0.5)
      no_straggler_runtimes = []
      for runtime in runtimes:
        if runtime > 1.5 * median_runtime:
          no_straggler_runtimes.append(median_runtime)
        else:
          no_straggler_runtimes.append(runtime)
      if id in self.stages_to_combine:
        runtimes_for_combined_stages.extend(no_straggler_runtimes)
      else:
        total_no_stragglers_runtime += simulate.simulate(no_straggler_runtimes)[0]
    if len(runtimes_for_combined_stages) > 0:
      total_no_stragglers_runtime += simulate.simulate(runtimes_for_combined_stages)[0]
    return total_no_stragglers_runtime * 1.0 / self.get_simulated_runtime()

  def calculate_speedup(self, description, compute_base_runtime, compute_faster_runtime):
    """ Returns how much faster the job would have run if each task had a faster runtime.

    Paramters:
      description: A description for the speedup, which will be printed to the command line.
      compute_base_runtime: Function that accepts a task and computes the runtime for that task.
        The resulting runtime will be used as the "base" time for the job, which the faster time
        will be compared to.
      compute_faster_runtime: Function that accepts a task and computes the new runtime for that
        task. The resulting job runtime will be compared to the job runtime using
        compute_base_runtime.
    """
    self.print_heading(description)
    # Making these single-element lists is a hack to ensure that they can be accessed from
    # inside the nested add_tasks_to_totals() function.
    total_time = [0]
    total_faster_time = [0]
    # Combine all of the tasks for stages that can be combined -- since they can use the cluster
    # concurrently.
    tasks_for_combined_stages = []

    def add_tasks_to_totals(unsorted_tasks):
      # Sort the tasks by the start time, not the finish time -- otherwise the longest tasks
      # end up getting run last, which can artificially inflate job completion time.
      tasks = sorted(unsorted_tasks, key = lambda task: task.start_time)

      # Get the runtime for the stage
      task_runtimes = [compute_base_runtime(task) for task in tasks]
      base_runtime = simulate.simulate(task_runtimes)[0]
      total_time[0] += base_runtime

      faster_runtimes = [compute_faster_runtime(task) for task in tasks]
      faster_runtime = simulate.simulate(faster_runtimes)[0]
      total_faster_time[0] += faster_runtime
      print "Base: %s, faster: %s" % (base_runtime, faster_runtime)

    for id, stage in self.stages.iteritems():
      print "STAGE", id, stage
      if id in self.stages_to_combine:
        tasks_for_combined_stages.extend(stage.tasks)
      else:
        add_tasks_to_totals(stage.tasks)

    if len(tasks_for_combined_stages) > 0:
      print "Combined stages", self.stages_to_combine
      add_tasks_to_totals(tasks_for_combined_stages)

    print "Faster time: %s, base time: %s" % (total_faster_time[0], total_time[0])
    return total_faster_time[0] * 1.0 / total_time[0]

  def fraction_time_scheduler_delay(self):
    """ Of the total time spent across all machines in the cluster, what fraction of time was
    spent waiting on the scheduler?"""
    total_scheduler_delay = 0
    total_runtime = 0
    for id, stage in self.stages.iteritems():
      total_scheduler_delay += sum([t.scheduler_delay for t in stage.tasks])
      total_runtime += stage.total_runtime()
    return total_scheduler_delay * 1.0 / total_runtime

  def network_speedup(self, relative_fetch_time):
    return self.calculate_speedup(
      "Computing speedup with %s relative fetch time" % relative_fetch_time,
      lambda t: t.runtime(),
      lambda t: t.runtime_faster_fetch(relative_fetch_time))

  def fraction_time_waiting_on_network(self):
    """ Of the total time spent across all machines in the cluster, what fraction of time was
    spent waiting on the network? """
    total_fetch_wait = 0
    # This is just used as a sanity check: total_runtime_no_fetch + total_fetch_wait
    # should equal total_runtime.
    total_runtime_no_fetch = 0
    total_runtime = 0
    for id, stage in self.stages.iteritems():
      total_fetch_wait += stage.total_fetch_wait()
      total_runtime_no_fetch += stage.total_runtime_no_fetch()
      total_runtime += stage.total_runtime()
    assert(total_runtime == total_fetch_wait + total_runtime_no_fetch)
    return total_fetch_wait * 1.0 / total_runtime

  def fraction_time_using_network(self):
    total_network_time = 0
    total_runtime = 0
    for stage in self.stages.values():
      total_network_time += sum([t.network_time() for t in stage.tasks])
      total_runtime += sum([t.runtime() for t in stage.tasks])
    return total_network_time * 1.0 / total_runtime

  def no_disk_shuffle_speedup(self):
    """ Returns the speedup if all disk I/O time for the shuffle had been completely eliminated. """
    return self.calculate_speedup(
      "Computing speedup without disk",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_disk_for_shuffle())

  def no_input_disk_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without disk input",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_input())

  def fraction_time_waiting_on_disk(self):
    total_disk_wait_time = 0
    total_runtime = 0
    for stage in self.stages.values():
      for task in stage.tasks:
        total_disk_wait_time += (task.runtime() - task.runtime_no_disk_for_shuffle())
        total_runtime += task.runtime()
    return total_disk_wait_time * 1.0 / total_runtime

  def fraction_fetch_time_reading_from_disk(self):
    total_time_fetching = sum([s.total_time_fetching() for s in self.stages.values()])
    total_disk_read_time = sum([s.total_disk_read_time() for s in self.stages.values()])
    if total_time_fetching == 0:
      return 0
    return total_disk_read_time * 1.0 / total_time_fetching

  def no_compute_speedup(self):
    """ Returns the time the job would have taken if all compute time had been eliminated. """
    return self.calculate_speedup(
      "Computing speedup with no compute", lambda t: t.runtime(), lambda t: t.runtime_no_compute())

  def fraction_time_waiting_on_compute(self):
    total_compute_wait_time = 0
    total_runtime = 0
    for stage in self.stages.values():
      for task in stage.tasks:
        total_compute_wait_time += (task.runtime() - task.runtime_no_compute())
        total_runtime += task.runtime()
    return total_compute_wait_time * 1.0 / total_runtime

  def fraction_time_computing(self):
    total_compute_time = 0
    total_runtime = 0
    for stage in self.stages.values():
      for task in stage.tasks:
        total_compute_time += task.compute_time()
        total_runtime += task.runtime()
    return total_compute_time * 1.0 / total_runtime

  def fraction_time_gc(self):
    total_gc_time = 0
    total_runtime = 0
    for stage in self.stages.values():
      total_gc_time += sum([t.gc_time for t in stage.tasks])
      total_runtime += sum([t.runtime() for t in stage.tasks])
    return total_gc_time * 1.0 / total_runtime

  def fraction_time_using_disk(self):
    """ Fraction of task time spent writing shuffle outputs to disk and reading them back.
    
    Does not include time to spill data to disk (which is fine for now because that feature is
    turned off by default nor the time to persist result data to disk (if that happens).
    """ 
    total_disk_write_time = 0
    total_runtime = 0
    for id, stage in self.stages.iteritems():
      stage_disk_write_time = 0
      stage_total_runtime = 0
      for task in stage.tasks:
        stage_disk_write_time += task.disk_time()
        stage_total_runtime += task.runtime()
      self.logger.debug("Stage %s: Disk write time: %s, total runtime: %s" %
        (id, stage_disk_write_time, stage_total_runtime))
      total_disk_write_time += stage_disk_write_time
      total_runtime += stage_total_runtime
    return total_disk_write_time * 1.0 / total_runtime

  def write_network_and_disk_times_scatter(self, prefix):
    """ Writes data and gnuplot file for a disk/network throughput scatter plot.
    
    Writes each individual transfer, so there are multiple data points for each task. """
    # Data file.
    network_filename = "%s_network_times.scatter" % prefix
    network_file = open(network_filename, "w")
    network_file.write("KB\tTime\n")
    disk_filename = "%s_disk_times.scatter" % prefix
    disk_file = open(disk_filename, "w")
    disk_file.write("KB\tTime\n")
    for stage in self.stages.values():
      for task in stage.tasks:
        if not task.has_fetch:
          continue
        for b, time in task.network_times:
          network_file.write("%s\t%s\n" % (b / 1024., time))
        for b, time in task.disk_times:
          disk_file.write("%s\t%s\n" % (b / 1024., time))
    network_file.close()
    disk_file.close()

    # Write plot file.
    scatter_base_file = open("scatter_base.gp", "r")
    plot_file = open("%s_net_disk_scatter.gp" % prefix, "w")
    for line in scatter_base_file:
      plot_file.write(line)
    scatter_base_file.close()
    plot_file.write("set output \"%s_scatter.pdf\"\n" % prefix)
    plot_file.write("plot \"%s\" using 1:2 with dots title \"Network\",\\\n" %
      network_filename)
    plot_file.write("\"%s\" using 1:2 with p title \"Disk\"\n" % disk_filename)
    plot_file.close()

  def write_task_write_times_scatter(self, prefix):
    filename = "%s_task_write_times.scatter" % prefix
    scatter_file = open(filename, "w")
    scatter_file.write("MB\tTime\n")
    for task in self.all_tasks():
      if task.shuffle_mb_written > 0:
        scatter_file.write("%s\t%s\n" % (task.shuffle_mb_written, task.shuffle_write_time))
    scatter_file.close()

    # Write plot file.
    scatter_base_file = open("scatter_base.gp", "r")
    plot_file = open("%s_task_write_scatter.gp" % prefix, "w")
    for line in scatter_base_file:
      plot_file.write(line)
    scatter_base_file.close()
    plot_file.write("set xlabel \"Data (MB)\"\n")
    plot_file.write("set output \"%s_task_write_scatter.pdf\"\n" % prefix)
    plot_file.write("plot \"%s\" using 1:2 with dots title \"Disk Write\"\n" % filename)
    plot_file.close()

  def write_simulated_waterfall(self, start_finish_times, prefix):
    """ Outputs a gnuplot file that visually shows all task runtimes.
    
    start_finish_times is expected to be a list of lists, one for each stage,
    where the list for a particular stage contains the start and finish times
    for each task.
    """
    cumulative_tasks = 0
    stage_cumulative_tasks = []
    all_times = []
    # Sort stages by the start time of the first task.
    for stage_times in sorted(start_finish_times):
      all_times.extend(stage_times)
      cumulative_tasks = cumulative_tasks + len(stage_times)
      stage_cumulative_tasks.append(str(cumulative_tasks))

    base_file = open("waterfall_base.gp", "r")
    plot_file = open("%s_waterfall.gp" % prefix, "w")
    for line in base_file:
      plot_file.write(line)
    base_file.close()

    LINE_TEMPLATE = "set arrow from %s,%s to %s,%s ls %s nohead\n"

    # Write all time relative to the first start time so the graph is easier to read.
    first_start = all_times[0][0]
    for i, start_finish in enumerate(all_times):
      start = start_finish[0] - first_start
      finish = start_finish[1] - first_start
      # Write data to plot file.
      plot_file.write(LINE_TEMPLATE % (start, i, finish, i, 3))

    last_end = all_times[-1][1]
    ytics_str = ",".join(stage_cumulative_tasks)
    plot_file.write("set ytics (%s)\n" % ytics_str)
    plot_file.write("set xrange [0:%s]\n" % (last_end - first_start))
    plot_file.write("set yrange [0:%s]\n" % len(all_times))
    plot_file.write("set output \"%s_waterfall.pdf\"\n" % prefix)
    plot_file.write("plot -1\n")

    plot_file.close()

  def write_waterfall(self, prefix):
    """ Outputs a gnuplot file that visually shows all task runtimes. """
    all_tasks = []
    cumulative_tasks = 0
    stage_cumulative_tasks = []
    for stage in sorted(self.stages.values(), key = lambda x: x.start_time):
      all_tasks.extend(sorted(stage.tasks, key = lambda x: x.start_time))
      cumulative_tasks = cumulative_tasks + len(stage.tasks)
      stage_cumulative_tasks.append(str(cumulative_tasks))

    base_file = open("waterfall_base.gp", "r")
    plot_file = open("%s_waterfall.gp" % prefix, "w")
    for line in base_file:
      plot_file.write(line)
    base_file.close()

    LINE_TEMPLATE = "set arrow from %s,%s to %s,%s ls %s nohead\n"

    # Write all time relative to the first start time so the graph is easier to read.
    first_start = all_tasks[0].start_time
    for i, task in enumerate(all_tasks):
      start = task.start_time - first_start
      # Show the scheduler delay at the beginning -- but it could be at the beginning or end or split.
      scheduler_delay_end = start + task.scheduler_delay
      deserialize_end = scheduler_delay_end + task.executor_deserialize_time
      hdfs_read_end = deserialize_end
      local_read_end = deserialize_end
      fetch_wait_end = deserialize_end
      if task.has_fetch:
        local_read_end = deserialize_end + task.local_read_time
        fetch_wait_end = local_read_end + task.fetch_wait
      elif task.input_read_method == "HDFS":
        hdfs_read_end = deserialize_end + task.input_read_time
        local_read_end = hdfs_read_end
        fetch_wait_end = hdfs_read_end
      # Here, assume GC happens as part of compute (although we know that sometimes
      # GC happens during fetch wait.
      compute_end = fetch_wait_end + task.compute_time_without_gc()
      gc_end = compute_end + task.gc_time
      task_end = gc_end + task.shuffle_write_time
      if math.fabs((first_start + task_end) - task.finish_time) >= 0.1:
        print "!!!!!!!!!!!!!!!!Mismatch at index %s" % i
        print task
        assert False

      # Write data to plot file.
      plot_file.write(LINE_TEMPLATE % (start, i, scheduler_delay_end, i, 6))
      plot_file.write(LINE_TEMPLATE % (scheduler_delay_end, i, deserialize_end, i, 8))
      if task.has_fetch:
        plot_file.write(LINE_TEMPLATE % (deserialize_end, i, local_read_end, i, 1))
        plot_file.write(LINE_TEMPLATE % (local_read_end, i, fetch_wait_end, i, 2))
        plot_file.write(LINE_TEMPLATE % (fetch_wait_end, i, compute_end, i, 3))
      else:
        plot_file.write(LINE_TEMPLATE % (deserialize_end, i, hdfs_read_end, i, 7))
        plot_file.write(LINE_TEMPLATE % (hdfs_read_end, i, compute_end, i, 3))
      plot_file.write(LINE_TEMPLATE % (compute_end, i, gc_end, i, 4))
      plot_file.write(LINE_TEMPLATE % (gc_end, i, task_end, i, 5))

    last_end = all_tasks[-1].finish_time
    ytics_str = ",".join(stage_cumulative_tasks)
    plot_file.write("set ytics (%s)\n" % ytics_str)
    plot_file.write("set xrange [0:%s]\n" % (last_end - first_start))
    plot_file.write("set yrange [0:%s]\n" % len(all_tasks))
    plot_file.write("set output \"%s_waterfall.pdf\"\n" % prefix)

    # Hacky way to force a key to be printed.
    plot_file.write("plot -1 ls 6 title 'Scheduler delay',\\\n")
    plot_file.write(" -1 ls 8 title 'Task deserialization', -1 ls 7 title 'HDFS read',\\\n")
    plot_file.write("-1 ls 1 title 'Local read wait',\\\n")
    plot_file.write("-1 ls 2 title 'Network wait', -1 ls 3 title 'Compute', \\\n")
    plot_file.write("-1 ls 4 title 'GC', -1 ls 5 title 'Disk write wait'\\\n")
    plot_file.close()

  def make_cdfs_for_performance_model(self, prefix):
    """ Writes plot files to create CDFS of the compute / network / disk rate. """
    all_tasks = self.all_tasks()
    compute_rates = [task.compute_time() * 1.0 / task.input_data for task in all_tasks]
    write_cdf(compute_rates, "%s_compute_rate_cdf" % prefix)
    
    network_rates = [task.compute_time() * 1.0 / task.input_data for task in all_tasks]
    write_cdf(network_rates, "%s_network_rate_cdf" % prefix)

    write_rates = [task.shuffle_write_time * 1.0 / task.shuffle_mb_written for task in all_tasks]
    write_cdf(write_rates, "%s_write_rate_cdf" % prefix)

def parse(filename, agg_results_filename=None):
  analyzer = Analyzer(filename)

  analyzer.print_stage_info()

  analyzer.write_task_write_times_scatter(filename)

  #analyzer.make_cdfs_for_performance_model(filename)

  analyzer.write_network_and_disk_times_scatter(filename)
  
  analyzer.write_waterfall(filename)

  # Compute the speedup for a fetch time of 1.0 as a sanity check!
  # relative_fetch_time is a multipler that describes how long the fetch took relative to how
  # long it took in the original trace.  For example, a relative_fetch_time of 0 is for
  # a network that shuffled data instantaneously, and a relative_fetch_time of 0.25
  # is for a 4x faster network.
  results_file = open("%s_improvements" % filename, "w")
  no_network_speedup = -1
  for relative_fetch_time in [0]:
    faster_fetch_speedup = analyzer.network_speedup(relative_fetch_time)
    print "Speedup from relative fetch of %s: %s" % (relative_fetch_time, faster_fetch_speedup)
    if relative_fetch_time == 0:
      no_network_speedup = faster_fetch_speedup
    results_file.write("%s %s\n" % (relative_fetch_time, faster_fetch_speedup))

  fraction_time_scheduler_delay = analyzer.fraction_time_scheduler_delay()
  print ("\nFraction time scheduler delay: %s" % fraction_time_scheduler_delay)
  fraction_time_waiting_on_network = analyzer.fraction_time_waiting_on_network()
  print "\nFraction time waiting on network: %s" % fraction_time_waiting_on_network
  fraction_time_using_network = analyzer.fraction_time_using_network()
  print "\nFraction time using network: %s" % fraction_time_using_network
  print ("\nFraction of fetch time spent reading from disk: %s" %
    analyzer.fraction_fetch_time_reading_from_disk())
  no_disk_speedup = analyzer.no_disk_shuffle_speedup()
  print "Speedup from eliminating disk for shuffle: %s" % no_disk_speedup
  no_input_disk_speedup = analyzer.no_input_disk_speedup()
  print "Speedup from eliminating disk for input: %s" % no_input_disk_speedup
  fraction_time_waiting_on_disk = analyzer.fraction_time_waiting_on_disk()
  print "Fraction time waiting on disk: %s" % fraction_time_waiting_on_disk
  fraction_time_using_disk = analyzer.fraction_time_using_disk()
  print("\nFraction of time spent writing/reading shuffle data to/from disk: %s" %
    fraction_time_using_disk)
  print("\nFraction of time spent garbage collecting: %s" %
    analyzer.fraction_time_gc())
  no_compute_speedup = analyzer.no_compute_speedup()
  print "\nSpeedup from eliminating compute: %s" % no_compute_speedup
  fraction_time_waiting_on_compute = analyzer.fraction_time_waiting_on_compute()
  print "\nFraction of time waiting on compute: %s" % fraction_time_waiting_on_compute
  fraction_time_computing = analyzer.fraction_time_computing()
  print "\nFraction of time computing: %s" % fraction_time_computing
  
  no_stragglers_average_runtime_speedup = analyzer.no_stragglers_using_average_runtime_speedup(
    filename)
  no_stragglers_replace_with_median_speedup = analyzer.replace_stragglers_with_median_speedup()
  no_stragglers_replace_95_with_median_speedup = \
    analyzer.replace_95_stragglers_with_median_speedup()
  no_stragglers_perfect_parallelism = \
    analyzer.no_stragglers_perfect_parallelism_speedup()
  print (("\nSpeedup from eliminating stragglers: %s (perfect parallelism) %s (use average) "
    "%s (1.5=>med) %s (95%%ile=>med)") %
    (no_stragglers_perfect_parallelism, no_stragglers_average_runtime_speedup,
     no_stragglers_replace_with_median_speedup, no_stragglers_replace_95_with_median_speedup))

  simulated_versus_actual = analyzer.simulated_runtime_over_actual(filename)
  print "\n Simulated versus actual runtime: ", simulated_versus_actual

  if agg_results_filename != None:
    print "Adding results to %s" % agg_results_filename
    f = open(agg_results_filename, "a")
    f.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
      filename.split("/")[1].split("_")[0],
      no_network_speedup, fraction_time_waiting_on_network, fraction_time_using_network,
      no_disk_speedup, fraction_time_waiting_on_disk, fraction_time_using_disk,
      no_compute_speedup, fraction_time_waiting_on_compute, fraction_time_computing,
      no_stragglers_average_runtime_speedup, no_stragglers_replace_with_median_speedup,
      no_stragglers_replace_95_with_median_speedup, no_stragglers_perfect_parallelism,
      no_input_disk_speedup, simulated_versus_actual))
    f.close()


def main(argv):
  if len(argv) < 2:
    print "Usage: python parse_logs.py <log filename> <debug level> <(OPT) agg. results filename>"
    sys.exit()

  log_level = argv[1]
  if log_level == "debug":
    logging.basicConfig(level=logging.DEBUG)
  logging.basicConfig(level=logging.INFO)
  filename = argv[0]
  if len(argv) > 2:
    parse(filename, argv[2])
  else:
    parse(filename)

if __name__ == "__main__":
  main(sys.argv[1:])
