import collections
import json
import logging
import math
import numpy
import sys

import concurrency
import simulate
import stage
import task

def write_cdf(values, filename):
  values.sort()
  f = open(filename, "w")
  for percent in range(100):
    fraction = percent / 100.
    f.write("%s\t%s\n" % (fraction, numpy.percentile(values, percent)))
  f.close()

class Job:
  def __init__(self):
    self.logger = logging.getLogger("Job")
    # Map of stage IDs to Stages.
    self.stages = collections.defaultdict(stage.Stage)
  
  def add_event(self, data, is_json):
    if is_json:
      event_type = data["Event"]
      if event_type == "SparkListenerTaskEnd":
        stage_id = data["Stage ID"]
        self.stages[stage_id].add_event(data, True)
    else:
      STAGE_ID_MARKER = "STAGE_ID="
      stage_id_loc = data.find(STAGE_ID_MARKER)
      if stage_id_loc != -1:
        stage_id_and_suffix = data[stage_id_loc + len(STAGE_ID_MARKER):]
        stage_id = stage_id_and_suffix[:stage_id_and_suffix.find(" ")]
        self.stages[stage_id].add_event(data, False)

  def initialize_job(self):
    """ Should be called after adding all events to the job. """
    # Drop empty stages.
    stages_to_drop = []
    for id, s in self.stages.iteritems():
      if len(s.tasks) == 0:
        stages_to_drop.append(id)
    for id in stages_to_drop:
      print "Dropping stage %s" % id
      del self.stages[id]

    # Compute the amount of overlapped time between stages
    # (there should just be two stages, at the beginning, that overlap and run concurrently).
    # This computation assumes that not more than two stages overlap.
    print ["%s: %s tasks" % (id, len(s.tasks)) for id, s in self.stages.iteritems()]
    start_and_finish_times = [(id, s.start_time, s.conservative_finish_time())
        for id, s in self.stages.iteritems()]
    start_and_finish_times.sort(key = lambda x: x[1])
    self.overlap = 0
    old_end = 0
    previous_id = ""
    self.stages_to_combine = set()
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

    self.combined_stages_concurrency = -1
    if len(self.stages_to_combine) > 0:
      tasks_for_combined_stages = []
      for stage_id in self.stages_to_combine:
        tasks_for_combined_stages.extend(self.stages[stage_id].tasks)
      self.combined_stages_concurrency = concurrency.get_max_concurrency(tasks_for_combined_stages)

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
        simulated_runtime, start_finish_times = simulate.simulate(
          [t.runtime() for t in tasks], concurrency.get_max_concurrency(tasks))
        start_finish_times_adjusted = [
          (start + total_runtime, finish + total_runtime) for start, finish in start_finish_times]
        all_start_finish_times.append(start_finish_times_adjusted)
        total_runtime += simulated_runtime
    if len(tasks_for_combined_stages) > 0:
      tasks = sorted(tasks_for_combined_stages, key = lambda task: task.start_time)
      simulated_runtime, start_finish_times = simulate.simulate(
        [task.runtime() for task in tasks], self.combined_stages_concurrency)
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
    print "Simulated runtime: ", simulated_runtime, "actual time: ", self.original_runtime()
    return simulated_runtime * 1.0 / self.original_runtime()

  def original_runtime(self):
    actual_start_time = min([s.start_time for s in self.stages.values()])
    actual_finish_time = max([s.finish_time() for s in self.stages.values()])
    return actual_finish_time - actual_start_time

  def write_data_to_file(self, data, file_handle):
    stringified_data = [str(x) for x in data]
    stringified_data += "\n"
    file_handle.write("\t".join(stringified_data))

  def write_hdfs_stage_normalized_runtimes(self, agg_filename_prefix):
    """ Writes the normalized runtimes for all stages that read from hdfs. """
    all_hdfs_runtimes_file = open("%s_normalized_runtimes_hdfs" % agg_filename_prefix, "a")
    non_local_runtimes_file = open("%s_normalized_runtimes_hdfs_non_local" % agg_filename_prefix, "a")
    for stage in self.stages.values():
      if stage.tasks[0].input_mb > 0:
        median_runtime = numpy.median([x.runtime() for x in stage.tasks])
        normalized_runtimes = [str(x.runtime() * 1.0 / median_runtime) for x in stage.tasks]
        all_hdfs_runtimes_file.write("\n".join(normalized_runtimes))
        all_hdfs_runtimes_file.write("\n")

        normalized_non_local_runtimes = [str(x.runtime() * 1.0 / median_runtime)
          for x in stage.tasks if not x.data_local]
        if len(normalized_non_local_runtimes) > 0:
          non_local_runtimes_file.write("\n".join(normalized_non_local_runtimes))
          non_local_runtimes_file.write("\n")
    all_hdfs_runtimes_file.close()
    non_local_runtimes_file.close()

  def write_straggler_info(self, job_name, prefix):
    """ Writes information about straggler causes to a file.""" 
    filename = "%s_stragglers" % prefix

    total_tasks = sum([len(s.tasks) for s in self.stages.values()])
    total_runtime = sum([s.total_runtime() for s in self.stages.values()])
    total_traditional_stragglers = sum([s.traditional_stragglers() for s in self.stages.values()])
    total_traditional_straggler_time = sum(
      [s.total_traditional_straggler_runtime() for s in self.stages.values()])

    traditional_stragglers_explained_by_progress_rate = sum(
      [s.traditional_stragglers_explained_by_progress_rate()[0] for s in self.stages.values()])

    progress_rate_straggler_info = [s.progress_rate_stragglers() for s in self.stages.values()]
    progress_rate_straggler_count = sum([x[0] for x in progress_rate_straggler_info])
    progress_rate_straggler_time = sum([x[1] for x in progress_rate_straggler_info])

    output_progress_rate_straggler_info = [s.output_progress_rate_stragglers() \
      for s in self.stages.values()]
    output_progress_rate_straggler_count = sum([x[0] for x in output_progress_rate_straggler_info])
    output_progress_rate_straggler_time = sum([x[1] for x in output_progress_rate_straggler_info])

    hdfs_read_stragglers_info = [s.hdfs_read_stragglers() for s in self.stages.values()]
    hdfs_read_stragglers_count = sum([x[0] for x in hdfs_read_stragglers_info])
    hdfs_read_stragglers_time = sum([x[1] for x in hdfs_read_stragglers_info])
    hdfs_read_stragglers_non_local = sum([x[2] for x in hdfs_read_stragglers_info])

    hdfs_write_stragglers_info = [s.hdfs_write_stragglers() for s in self.stages.values()]
    hdfs_write_stragglers_count = sum([x[0] for x in hdfs_write_stragglers_info])
    hdfs_write_stragglers_time = sum([x[1] for x in hdfs_write_stragglers_info])

    gc_straggler_info = [s.gc_stragglers() for s in self.stages.values()]
    gc_straggler_count = sum([x[0] for x in gc_straggler_info])
    gc_straggler_time = sum([x[1] for x in gc_straggler_info])

    network_straggler_info = [s.network_stragglers() for s in self.stages.values()]
    network_straggler_count = sum([x[0] for x in network_straggler_info])
    network_straggler_time = sum([x[1] for x in network_straggler_info])

    scheduler_delay_straggler_info = [s.scheduler_delay_stragglers() for s in self.stages.values()]
    scheduler_delay_straggler_count = sum([x[0] for x in scheduler_delay_straggler_info])
    scheduler_delay_straggler_time = sum([x[1] for x in scheduler_delay_straggler_info])

    shuffle_write_straggler_info = [s.shuffle_write_stragglers() for s in self.stages.values()]
    shuffle_write_straggler_count = sum([x[0] for x in shuffle_write_straggler_info])
    shuffle_write_straggler_time = sum([x[1] for x in shuffle_write_straggler_info])

    scheduler_and_read_stragger_info = [s.hdfs_read_and_scheduler_delay_stragglers()
      for s in self.stages.values()]
    scheduler_and_read_straggler_count = sum([x[0] for x in scheduler_and_read_stragger_info])
    scheduler_and_read_straggler_time = sum([x[1] for x in scheduler_and_read_stragger_info])

    # Important that JIT stragglers be classified last!
    jit_straggler_info = [s.jit_stragglers() for s in self.stages.values()]
    jit_straggler_count = sum([x[0] for x in jit_straggler_info])
    jit_straggler_time = sum([x[1] for x in jit_straggler_info])

    all_stragglers = []
    for s in self.stages.values():
      all_stragglers.extend(s.get_progress_rate_stragglers())
    explained_stragglers = [t for t in all_stragglers if t.straggler_behavior_explained]
    explained_stragglers_total_time = sum([t.runtime() for t in explained_stragglers])

    f = open(filename, "a")
    data_to_write = [job_name, total_tasks, total_runtime,
      # 3 4 5
      len(explained_stragglers), explained_stragglers_total_time, len(all_stragglers),
      # 6 7
      total_traditional_stragglers, total_traditional_straggler_time,
      # 8
      traditional_stragglers_explained_by_progress_rate,
      # 9 10
      progress_rate_straggler_count, progress_rate_straggler_time,
      # 11 12 13
      hdfs_read_stragglers_count, hdfs_read_stragglers_time, hdfs_read_stragglers_non_local,
      # 14 15
      gc_straggler_count, gc_straggler_time,
      # 16 17
      network_straggler_count, network_straggler_time,
      # 18 19
      scheduler_delay_straggler_count, scheduler_delay_straggler_time,
      # 20 21
      scheduler_and_read_straggler_count, scheduler_and_read_straggler_time,
      # 22
      jit_straggler_count,
      # 23 24
      shuffle_write_straggler_count, shuffle_write_straggler_time,
      # 25 26
      hdfs_write_stragglers_count, hdfs_write_stragglers_time,
      # 27 28
      output_progress_rate_straggler_count, output_progress_rate_straggler_time,
      # 29
      jit_straggler_time]
    self.write_data_to_file(data_to_write, f)
    f.close()

  def median_progress_rate_speedup(self, prefix):
    """ Returns how fast the job would have run if all tasks had the median progress rate. """
    total_median_progress_rate_runtime = 0
    runtimes_for_combined_stages = []
    all_start_finish_times = []
    for id, stage in self.stages.iteritems():
      median_rate_runtimes = stage.task_runtimes_with_median_progress_rate()
      if id in self.stages_to_combine:
        runtimes_for_combined_stages.extend(median_rate_runtimes)
      else:
        no_stragglers_runtime, start_finish_times = simulate.simulate(
          median_rate_runtimes, concurrency.get_max_concurrency(stage.tasks))
        start_finish_times_adjusted = [
          (start + total_median_progress_rate_runtime, finish + total_median_progress_rate_runtime) \
          for start, finish in start_finish_times]
        total_median_progress_rate_runtime += no_stragglers_runtime
        all_start_finish_times.append(start_finish_times_adjusted)
        print "No stragglers runtime: ", no_stragglers_runtime
        print "MAx concurrency: ", concurrency.get_max_concurrency(stage.tasks)

    if len(runtimes_for_combined_stages) > 0:
      no_stragglers_runtime, start_finish_times = simulate.simulate(
        runtimes_for_combined_stages, self.combined_stages_concurrency)
      start_finish_times_adjusted = [
        (start + total_median_progress_rate_runtime, finish + total_median_progress_rate_runtime) \
        for start, finish in start_finish_times]
      total_median_progress_rate_runtime += no_stragglers_runtime
      all_start_finish_times.append(start_finish_times_adjusted)

    self.write_simulated_waterfall(all_start_finish_times, "%s_sim_median_progress_rate" % prefix)
    return total_median_progress_rate_runtime * 1.0 / self.get_simulated_runtime()

  def no_stragglers_perfect_parallelism_speedup(self):
    """ Returns how fast the job would have run if time were perfectly spread across 32 slots. """
    ideal_runtime = 0
    total_runtime_combined_stages = 0
    for id, stage in self.stages.iteritems():
      if id in self.stages_to_combine:
        total_runtime_combined_stages += stage.total_runtime()
      else:
        new_runtime = float(stage.total_runtime()) / concurrency.get_max_concurrency(stage.tasks)
        print "New runtime: %s, original runtime: %s" % (new_runtime, stage.finish_time() - stage.start_time)
        ideal_runtime += new_runtime


    print "Total runtime combined: %s (concurrency %d" % (total_runtime_combined_stages, self.combined_stages_concurrency)
    ideal_runtime += float(total_runtime_combined_stages) / self.combined_stages_concurrency
    print "Getting simulated runtime"
    simulated_actual_runtime = self.get_simulated_runtime()
    print "Ideal runtime for all: %s, simulated: %s" % (ideal_runtime, simulated_actual_runtime)
    return ideal_runtime / simulated_actual_runtime

  def replace_all_tasks_with_average_speedup(self, prefix):
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
        no_stragglers_runtime, start_finish_times = simulate.simulate(
          averaged_runtimes, concurrency.get_max_concurrency(stage.tasks))
        # Adjust the start and finish times based on when the stage staged.
        start_finish_times_adjusted = [
          (start + total_no_stragglers_runtime, finish + total_no_stragglers_runtime) \
          for start, finish in start_finish_times]
        total_no_stragglers_runtime += no_stragglers_runtime
        all_start_finish_times.append(start_finish_times_adjusted)
    if len(averaged_runtimes_for_combined_stages) > 0:
      no_stragglers_runtime, start_finish_times = simulate.simulate(
        averaged_runtimes_for_combined_stages, self.combined_stages_concurrency)
      # Adjust the start and finish times based on when the stage staged.
      # The subtraction is a hack to put the combined stages at the beginning, which
      # is when they usually occur.
      start_finish_times_adjusted = [
        (start - no_stragglers_runtime, finish - no_stragglers_runtime) for start, finish in start_finish_times]
      total_no_stragglers_runtime += no_stragglers_runtime
      all_start_finish_times.append(start_finish_times_adjusted)

    self.write_simulated_waterfall(all_start_finish_times, "%s_sim_no_stragglers" % prefix)
    return total_no_stragglers_runtime * 1.0 / self.get_simulated_runtime()

  def replace_stragglers_with_median_speedup(self, threshold_fn):
    """ Returns how much faster the job would have run if there were no stragglers.

    For each stage, passes the list of task runtimes into threshold_fn, which should
    return a threshold runtime. Then, replaces all task runtimes greater than the given
    threshold with the median runtime.

    For example, to replace the tasks with the longest 5% of runtimes with the median:
      self.replace_stragglers_with_median_speedup(lambda runtimes: numpy.percentile(runtimes, 95)
    """
    self.print_heading("Computing speedup from replacing straggler tasks with median")
    total_no_stragglers_runtime = 0
    start_and_runtimes_for_combined_stages = []
    original_start_and_runtimes_for_combined_stages = []
    num_stragglers_combined_stages = 0
    for id, stage in self.stages.iteritems():
      runtimes = [task.runtime() for task in stage.tasks]
      median_runtime = numpy.percentile(runtimes, 50)
      threshold_runtime = threshold_fn(runtimes)
      no_straggler_start_and_runtimes = []
      num_stragglers = 0
      sorted_stage_tasks = sorted(stage.tasks, key = lambda t: t.runtime())
      for task in sorted_stage_tasks:
        if task.runtime() >= threshold_runtime:
          assert(median_runtime <= task.runtime())
          no_straggler_start_and_runtimes.append((task.start_time, median_runtime))
          num_stragglers += 1 
        else:
          no_straggler_start_and_runtimes.append((task.start_time, task.runtime()))
      if id in self.stages_to_combine:
        start_and_runtimes_for_combined_stages.extend(no_straggler_start_and_runtimes)
        original_start_and_runtimes_for_combined_stages.extend(
          [(t.start_time, t.runtime()) for t in stage.tasks])
        num_stragglers_combined_stages += num_stragglers
      else:
        max_concurrency = concurrency.get_max_concurrency(stage.tasks)
        no_stragglers_runtime = simulate.simulate(
          [x[1] for x in no_straggler_start_and_runtimes], max_concurrency)[0]
        total_no_stragglers_runtime += no_stragglers_runtime
        original_runtime = simulate.simulate(
          [task.runtime() for task in sorted_stage_tasks], max_concurrency)[0]
        print ("%s: Original: %s, Orig (sim): %s, no stragg: %s (%s stragglers)" %
          (id, stage.finish_time() - stage.start_time, original_runtime, no_stragglers_runtime,
           num_stragglers))
    if len(start_and_runtimes_for_combined_stages) > 0:
      original_start_time = min([x[0] for x in start_and_runtimes_for_combined_stages])
      original_finish_time = max([x[0] + x[1] for x in start_and_runtimes_for_combined_stages])
      start_and_runtimes_for_combined_stages.sort()
      runtimes_for_combined_stages = [x[1] for x in start_and_runtimes_for_combined_stages]
      new_runtime = simulate.simulate(
        runtimes_for_combined_stages, self.combined_stages_concurrency)[0]
      original_runtime = simulate.simulate(
        [x[1] for x in sorted(original_start_and_runtimes_for_combined_stages)],
        self.combined_stages_concurrency)[0]
      print ("Combined: Original: %s, Orig (sim): %s, no stragg: %s (%s stragglers)" %
        (original_finish_time - original_start_time, original_runtime, new_runtime,
         num_stragglers_combined_stages))
      total_no_stragglers_runtime += new_runtime
    return total_no_stragglers_runtime * 1.0 / self.get_simulated_runtime()

  def replace_all_tasks_with_median_speedup(self):
    """ Returns how much faster the job would have run if there were no stragglers.

    Removes stragglers by replacing all task runtimes with the median runtime for tasks in the
    stage.
    """
    total_no_stragglers_runtime = 0
    runtimes_for_combined_stages = []
    for id, stage in self.stages.iteritems():
      runtimes = [task.runtime() for task in stage.tasks]
      median_runtime = numpy.median(runtimes)
      no_straggler_runtimes = [numpy.median(runtimes)] * len(stage.tasks)
      if id in self.stages_to_combine:
        runtimes_for_combined_stages.extend(no_straggler_runtimes)
      else:
        total_no_stragglers_runtime += simulate.simulate(
          no_straggler_runtimes, concurrency.get_max_concurrency(stage.tasks))[0]
    if len(runtimes_for_combined_stages) > 0:
      total_no_stragglers_runtime += simulate.simulate(
        runtimes_for_combined_stages, self.combined_stages_concurrency)[0]
    return total_no_stragglers_runtime * 1.0 / self.get_simulated_runtime()

  def calculate_speedup(self, description, compute_base_runtime, compute_faster_runtime):
    """ Returns how much faster the job would have run if each task had a faster runtime.

    Parameters:
      description: A description for the speedup, which will be printed to the command line.
      compute_base_runtime: Function that accepts a task and computes the runtime for that task.
        The resulting runtime will be used as the "base" time for the job, which the faster time
        will be compared to.
      compute_faster_runtime: Function that accepts a task and computes the new runtime for that
        task. The resulting job runtime will be compared to the job runtime using
        compute_base_runtime.

    Returns:
      A 3-item tuple: [relative speedup, original runtime, estimated new runtime]
        TODO: Right now this doesn't return the actual original runtime; just the estimated one (so
        that it is comparable to the estimated new one).
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
      max_concurrency = concurrency.get_max_concurrency(tasks)

      # Get the runtime for the stage
      task_runtimes = [compute_base_runtime(task) for task in tasks]
      base_runtime = simulate.simulate(task_runtimes, max_concurrency)[0]
      total_time[0] += base_runtime

      faster_runtimes = [compute_faster_runtime(task) for task in tasks]
      faster_runtime = simulate.simulate(faster_runtimes, max_concurrency)[0]
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
    return total_faster_time[0] * 1.0 / total_time[0], total_time[0], total_faster_time[0]

  def fraction_time_scheduler_delay(self):
    """ Of the total time spent across all machines in the cluster, what fraction of time was
    spent waiting on the scheduler?"""
    total_scheduler_delay = 0
    total_runtime = 0
    for id, stage in self.stages.iteritems():
      total_scheduler_delay += sum([t.scheduler_delay for t in stage.tasks])
      total_runtime += stage.total_runtime()
    return total_scheduler_delay * 1.0 / total_runtime

  def fraction_time_waiting_on_shuffle_read(self):
    """ Of the total time spent across all machines in the cluster, what fraction of time was
    spent waiting on the network? """
    total_fetch_wait = 0
    # This is just used as a sanity check: total_runtime_no_shuffle_read + total_fetch_wait
    # should equal total_runtime.
    total_runtime_no_shuffle_read = 0
    total_runtime = 0
    for id, stage in self.stages.iteritems():
      total_fetch_wait += stage.total_fetch_wait()
      total_runtime_no_shuffle_read += stage.total_runtime_no_remote_shuffle_read()
      total_runtime += stage.total_runtime()
    assert(total_runtime == total_fetch_wait + total_runtime_no_shuffle_read)
    return total_fetch_wait * 1.0 / total_runtime

  def no_input_disk_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without disk input",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_input())

  def no_output_disk_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without disk output",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_output())

  def no_shuffle_write_disk_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without disk for shuffle",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_shuffle_write())

  def no_shuffle_read_disk_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without shuffle read",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_shuffle_read())

  def no_network_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without network",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_network())

  def no_disk_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without disk",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_disk())

  def no_gc_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without gc",
      lambda t: t.runtime(),
      lambda t: t.runtime() - t.gc_time)

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
      # Show the scheduler delay at the beginning -- but it could be at the beginning or end or
      # split.
      scheduler_delay_end = start + task.scheduler_delay
      deserialize_end = scheduler_delay_end + task.executor_deserialize_time
      # TODO: input_read_time should only be included when the task reads input data from
      # HDFS, but with the current logging it's also recorded when data is read from memory,
      # so should be included here to make the task end time line up properly.
      hdfs_read_end = deserialize_end + task.input_read_time
      local_read_end = hdfs_read_end
      fetch_wait_end = hdfs_read_end
      if task.has_fetch:
        local_read_end = deserialize_end + task.local_read_time
        fetch_wait_end = local_read_end + task.fetch_wait
      # Here, assume GC happens as part of compute (although we know that sometimes
      # GC happens during fetch wait.
      serialize_millis = task.result_serialization_time
      serialize_end = fetch_wait_end + serialize_millis
      compute_end = (fetch_wait_end + task.compute_time_without_gc() - 
        task.executor_deserialize_time)
      gc_end = compute_end + task.gc_time
      task_end = gc_end + task.shuffle_write_time + task.output_write_time
      if math.fabs((first_start + task_end) - task.finish_time) >= 0.1:
        print "!!!!!!!!!!!!!!!!Mismatch at index %s" % i
        print "%.1f" % (first_start + task_end)
        print task.finish_time
        print task
        assert False

      # Write data to plot file.
      plot_file.write(LINE_TEMPLATE % (start, i, scheduler_delay_end, i, 6))
      plot_file.write(LINE_TEMPLATE % (scheduler_delay_end, i, deserialize_end, i, 8))
      if task.has_fetch:
        plot_file.write(LINE_TEMPLATE % (deserialize_end, i, local_read_end, i, 1))
        plot_file.write(LINE_TEMPLATE % (local_read_end, i, fetch_wait_end, i, 2))
        plot_file.write(LINE_TEMPLATE % (fetch_wait_end, i, serialize_end, i, 9))
      else:
        plot_file.write(LINE_TEMPLATE % (deserialize_end, i, hdfs_read_end, i, 7))
        plot_file.write(LINE_TEMPLATE % (hdfs_read_end, i, serialize_end, i, 9))
      plot_file.write(LINE_TEMPLATE % (serialize_end, i, compute_end, i, 3))
      plot_file.write(LINE_TEMPLATE % (compute_end, i, gc_end, i, 4))
      plot_file.write(LINE_TEMPLATE % (gc_end, i, task_end, i, 5))

    last_end = max([t.finish_time for t in all_tasks])
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
    plot_file.write("-1 ls 9 title 'Data (de)serialization', -1 ls 4 title 'GC', \\\n")
    plot_file.write("-1 ls 5 title 'Output write wait'\\\n")
    plot_file.close()

  def write_stage_info(self, query_id, prefix):
    f = open("%s_stage_info" % prefix, "a")
    last_stage_runtime = -1
    last_stage_finish_time = 0
    for stage in self.stages.values():
      # This is a hack! Count the most recent stage with runtime > 1s as the "last".
      # Shark produces 1-2 very short stages at the end that do not seem to do anything (and
      # certainly aren't doing the output write we're trying to account for).
      if (stage.finish_time() - stage.start_time) > 1000 and stage.finish_time() > last_stage_finish_time:
        last_stage_finish_time = stage.finish_time()
        last_stage_runtime = stage.finish_time() - stage.start_time

    f.write("%s\t%s\t%s\n" % (query_id, last_stage_runtime, self.original_runtime()))
    f.close()

  def make_cdfs_for_performance_model(self, prefix):
    """ Writes plot files to create CDFS of the compute / network / disk rate. """
    all_tasks = self.all_tasks()
    compute_rates = [task.compute_time() * 1.0 / task.input_data for task in all_tasks]
    write_cdf(compute_rates, "%s_compute_rate_cdf" % prefix)
    
    network_rates = [task.compute_time() * 1.0 / task.input_data for task in all_tasks]
    write_cdf(network_rates, "%s_network_rate_cdf" % prefix)

    write_rates = [task.shuffle_write_time * 1.0 / task.shuffle_mb_written for task in all_tasks]
    write_cdf(write_rates, "%s_write_rate_cdf" % prefix)

