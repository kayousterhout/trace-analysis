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
    f = open(filename, "r")
    # Map of stage IDs to Stages.
    self.stages = collections.defaultdict(stage.Stage)
    for line in f:
      STAGE_ID_MARKER = "STAGE_ID="
      stage_id_loc = line.find(STAGE_ID_MARKER)
      if stage_id_loc != -1:
        stage_id_and_suffix = line[stage_id_loc + len(STAGE_ID_MARKER):]
        stage_id = stage_id_and_suffix[:stage_id_and_suffix.find(" ")]
        # TODO: Remove this if not running query 3b in the benchmark! This is a hack to combine two
        # stages that run concurrently.
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

  def print_heading(self, text):
    print "\n******** %s ********" % text

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
      # TODO: compare this to the original stage run time as a sanity check.
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

  def disk_speedup(self):
    """ Returns the speedup if all disk I/O time had been completely eliminated. """
    return self.calculate_speedup(
      "Computing speedup without disk",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_disk_for_shuffle())

  def fraction_fetch_time_reading_from_disk(self):
    total_time_fetching = sum([s.total_time_fetching() for s in self.stages.values()])
    total_disk_read_time = sum([s.total_disk_read_time() for s in self.stages.values()])
    return total_disk_read_time * 1.0 / total_time_fetching

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
  log_level = argv[1]
  if log_level == "debug":
    logging.basicConfig(level=logging.DEBUG)
  logging.basicConfig(level=logging.INFO)
  filename = argv[0]
  analyzer = Analyzer(filename)

  analyzer.write_network_and_disk_times_scatter(filename)

  # Compute the speedup for a fetch time of 1.0 as a sanity check!
  # relative_fetch_time is a multipler that describes how long the fetch took relative to how
  # long it took in the original trace.  For example, a relative_fetch_time of 0 is for
  # a network that shuffled data instantaneously, and a relative_fetch_time of 0.25
  # is for a 4x faster network.
  results_file = open("%s_improvements" % filename, "w")
  for relative_fetch_time in [0, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0]:
    faster_fetch_speedup = analyzer.network_speedup(relative_fetch_time)
    print "Speedup from relative fetch of %s: %s" % (relative_fetch_time, faster_fetch_speedup)
    results_file.write("%s %s\n" % (relative_fetch_time, faster_fetch_speedup))

  print "\nFraction time waiting on network: %s" % analyzer.fraction_time_waiting_on_network()
  print ("\nFraction of fetch time spent reading from disk: %s" %
    analyzer.fraction_fetch_time_reading_from_disk())
  print "Speedup from eliminating disk: %s" % analyzer.disk_speedup()

if __name__ == "__main__":
  main(sys.argv[1:])
