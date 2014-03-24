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
      if finish > old_end:
        old_end = finish
        previous_id = id

  def print_stage_info(self):
    for id, stage in self.stages.iteritems():
      print "STAGE %s: %s" % (id, stage.verbose_str())

  def print_heading(self, text):
    print "\n******** %s ********" % text

  def get_simulated_runtime(self):
    """ Returns the simulated runtime for the job.

    This should be approximately the same as the original runtime of the job, except
    that it doesn't include scheduler delay.
    """
    total_runtime = 0
    tasks_for_combined_stages = []
    for id, stage in self.stages.iteritems():
      if id in self.stages_to_combine:
        tasks_for_combined_stages.extend(stage.tasks)
      else:
        tasks = sorted(stage.tasks, key = lambda task: task.start_time)
        total_runtime += simulate.simulate([task.runtime() for task in tasks])
    if len(tasks_for_combined_stages) > 0:
      tasks = sorted(tasks_for_combined_stages, key = lambda task: task.start_time)
      total_runtime += simulate.simulate([task.runtime() for task in tasks])
    return total_runtime 

  def no_stragglers_using_average_runtime_speedup(self):
    """ Returns how much faster the job would have run if there were no stragglers.

    Eliminates stragglers by replacing each task's runtime with the average runtime
    for tasks in the job.
    """
    self.print_heading("Computing speedup by averaging out stragglers")
    total_no_stragglers_runtime = 0
    averaged_runtimes_for_combined_stages = []
    for id, stage in self.stages.iteritems():
      averaged_runtimes = [stage.average_task_runtime()] * len(stage.tasks)
      if id in self.stages_to_combine:
        averaged_runtimes_for_combined_stages.extend(averaged_runtimes) 
      else:
        total_no_stragglers_runtime += simulate.simulate(averaged_runtimes)
    if len(averaged_runtimes_for_combined_stages) > 0:
      total_no_stragglers_runtime += simulate.simulate(averaged_runtimes_for_combined_stages)
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
        runtimes_for_combined_stages.extend(runtimes)
      else:
        total_no_stragglers_runtime += simulate.simulate(no_straggler_runtimes)
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
        runtimes_for_combined_stages.extend(runtimes)
      else:
        total_no_stragglers_runtime += simulate.simulate(no_straggler_runtimes)
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
      base_runtime = simulate.simulate(task_runtimes)
      total_time[0] += base_runtime

      faster_runtimes = [compute_faster_runtime(task) for task in tasks]
      faster_runtime = simulate.simulate(faster_runtimes)
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

  def network_speedup(self, relative_fetch_time):
    return self.calculate_speedup(
      "Computing speedup with %s relative fetch time" % relative_fetch_time,
      lambda t: t.runtime(),
      lambda t: t.runtime_faster_fetch(relative_fetch_time))

  def fraction_time_waiting_on_network(self):
    """ Of the total time spent across all machines in the network, what fraction of time was
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

  def disk_speedup(self):
    """ Returns the speedup if all disk I/O time had been completely eliminated. """
    return self.calculate_speedup(
      "Computing speedup without disk",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_disk_for_shuffle())

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
    """ Writes data and gnuplot file for a disk/network throughput scatter plot."""
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

def main(argv):
  if len(argv) < 2:
    print "Usage: python parse_logs.py <log filename> <debug level> <(OPT) agg. results filename>"
    sys.exit()

  log_level = argv[1]
  if log_level == "debug":
    logging.basicConfig(level=logging.DEBUG)
  logging.basicConfig(level=logging.INFO)
  filename = argv[0]
  analyzer = Analyzer(filename)

  analyzer.print_stage_info()

  analyzer.write_network_and_disk_times_scatter(filename)

  # Compute the speedup for a fetch time of 1.0 as a sanity check!
  # relative_fetch_time is a multipler that describes how long the fetch took relative to how
  # long it took in the original trace.  For example, a relative_fetch_time of 0 is for
  # a network that shuffled data instantaneously, and a relative_fetch_time of 0.25
  # is for a 4x faster network.
  results_file = open("%s_improvements" % filename, "w")
  no_network_speedup = -1
  for relative_fetch_time in [0, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0]:
    faster_fetch_speedup = analyzer.network_speedup(relative_fetch_time)
    print "Speedup from relative fetch of %s: %s" % (relative_fetch_time, faster_fetch_speedup)
    if relative_fetch_time == 0:
      no_network_speedup = faster_fetch_speedup
    results_file.write("%s %s\n" % (relative_fetch_time, faster_fetch_speedup))

  fraction_time_waiting_on_network = analyzer.fraction_time_waiting_on_network()
  print "\nFraction time waiting on network: %s" % fraction_time_waiting_on_network
  fraction_time_using_network = analyzer.fraction_time_using_network()
  print "\nFraction time using network: %s" % fraction_time_using_network
  print ("\nFraction of fetch time spent reading from disk: %s" %
    analyzer.fraction_fetch_time_reading_from_disk())
  no_disk_speedup = analyzer.disk_speedup()
  print "Speedup from eliminating disk: %s" % no_disk_speedup
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
  
  no_stragglers_average_runtime_speedup = analyzer.no_stragglers_using_average_runtime_speedup()
  no_stragglers_replace_with_median_speedup = analyzer.replace_stragglers_with_median_speedup()
  no_stragglers_replace_95_with_median_speedup = \
    analyzer.replace_95_stragglers_with_median_speedup()
  print ("\nSpeedup from eliminating stragglers: %s (use average) %s (1.5=>med) %s (95%%ile=>med)" %
    (no_stragglers_average_runtime_speedup, no_stragglers_replace_with_median_speedup,
     no_stragglers_replace_95_with_median_speedup))

  if len(argv) > 2:
    agg_results_filename = argv[2]
    print "Adding results to %s" % agg_results_filename
    f = open(agg_results_filename, "a")
    f.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
      filename.split("/")[1].split("_")[0],
      no_network_speedup, fraction_time_waiting_on_network, fraction_time_using_network,
      no_disk_speedup, fraction_time_waiting_on_disk, fraction_time_using_disk,
      no_compute_speedup, fraction_time_waiting_on_compute, fraction_time_computing,
      no_stragglers_average_runtime_speedup, no_stragglers_replace_with_median_speedup,
      no_stragglers_replace_95_with_median_speedup))
    f.close()

if __name__ == "__main__":
  main(sys.argv[1:])
