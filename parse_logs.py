import bisect
import collections
import math
import sys

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

""" Simulates the completion time of the given set of tasks, assuming 40 slots. """
def simulate(tasks, runtime_function, sort=True, verbose=False):
  if sort:
    # Important to sort tasks by their start time, because if tasks are sorted by
    # finish time, the longest tasks will end up in the last wave, which can artificially
    # inflate job completion time.
    tasks.sort(key = lambda x: x.start_time)

  # Sorted list of task finish times, measured as the time from when the job started.
  finish_times = []
  # Start 40 tasks.
  while len(finish_times) < 40 and len(tasks) > 0:
    runtime = runtime_function(tasks.pop(0))
    if verbose:
      print "Adding task with runtime %s" % runtime
    bisect.insort_left(finish_times, runtime)

  while len(tasks) > 0:
    if verbose:
      print finish_times
    start_time = finish_times.pop(0)
    finish_time = start_time + runtime_function(tasks.pop(0))
    if verbose:
      print "Task starting at ", start_time, " finishing at", finish_time
    bisect.insort_left(finish_times, finish_time)

  # Job finishes when the last task is done.
  return finish_times[-1]

""" Class that replays the execution of the given set of tasks. """
class Simulation:
  """ tasks_lists is a list of tasks, possibly that were in different stages. """
  def __init__(self, tasks_lists, relative_fetch_time):
    self.SLOTS = 40
    self.runtime = 0
    self.runtime_faster_fetch = 0

    original_tasks = []
    tasks_without_stragglers = []
    tasks_without_stragglers_2 = []
    # List of the average runtimes, for each of the stages.
    normalized_runtimes = []
    for tasks in tasks_lists:
      original_tasks.extend(tasks)

      # Drop the tasks with the highest 5% of runtimes.
      runtimes = [t.runtime() for t in tasks]
      runtimes.sort()
      tasks_without_stragglers.extend(
        [t for t in tasks if t.runtime() <= get_percentile(runtimes, 0.95)])

      avg_runtime = sum(runtimes) * 1.0 / len(runtimes)
      normalized_runtimes.extend([avg_runtime for t in tasks])

      # Drop the tasks with the highest 5% of non-network runtime.
      non_net_runtimes = [t.runtime_faster_fetch(relative_fetch_time) for t in tasks]
      non_net_runtimes.sort()
      # TODO: Better to RELACE the highest 95% with median runtime?
      tasks_without_stragglers_2.extend(
        [t for t in tasks
         if (t.runtime_faster_fetch(relative_fetch_time)) <= get_percentile(non_net_runtimes, 0.95)])

    # Make a copy of tasks to pass to simulate, because simulate modifies the list.
    self.runtime = simulate(list(original_tasks), Task.runtime)
    self.runtime_faster_fetch = simulate(list(original_tasks), lambda x: x.runtime_faster_fetch(relative_fetch_time))

    self.runtime_with_normalized_stragglers = simulate(normalized_runtimes, lambda x: x, sort=False)

    self.runtime_with_no_stragglers = simulate(list(tasks_without_stragglers), Task.runtime)
    self.runtime_with_no_stragglers_and_no_fetch = simulate(
      list(tasks_without_stragglers), lambda x: x.runtime_faster_fetch(relative_fetch_time))

    self.runtime_with_no_stragglers_2 = simulate(list(tasks_without_stragglers_2), Task.runtime)
    self.runtime_with_no_stragglers_and_no_fetch_2 = simulate(
      list(tasks_without_stragglers_2), lambda x: x.runtime_faster_fetch(relative_fetch_time))

class Task:
  def __init__(self, start_time, fetch_wait, finish_time, remote_bytes_read):
    self.start_time = start_time
    self.fetch_wait = fetch_wait
    self.finish_time = finish_time
    self.remote_mb_read = remote_bytes_read / 1048576.

  def runtime(self):
    return self.finish_time - self.start_time

  def finish_time_faster_fetch(self, relative_fetch_time):
    return self.finish_time - (1 - relative_fetch_time) * self.fetch_wait

  def runtime_faster_fetch(self, relative_fetch_time):
    return self.finish_time_faster_fetch(relative_fetch_time) - self.start_time

  def __str__(self):
    return ("%s %s %s %s" % (self.start_time, self.finish_time, self.fetch_wait, self.finish_time - self.fetch_wait))
   # return ("%s Runtime %s Fetch %s (%.2fMB) Fetchless runtime %s" %
   #   (self.start_time, self.runtime(), self.fetch_wait, self.remote_mb_read, self.runtime_faster_fetch()))

class Stage:
  def __init__(self):
    # TODO: Add a JobLogger event for when the stage arrives.
    self.start_time = -1
    self.tasks = []
    self.has_fetch = False
    self.num_tasks = 0
    self.fetch_wait_fractions = []

  def __str__(self):
    return "%s tasks Start: %s, finish: %s" % (self.num_tasks, self.start_time, self.finish_time())

  def finish_time(self):
    return max([t.finish_time for t in self.tasks])

  def runtime_with_faster_fetch(self, relative_fetch_time):
    return (max([t.finish_time_faster_fetch(relative_fetch_time) for t in self.tasks]) -
      self.start_time)

  def total_runtime(self):
    return sum([t.finish_time - t.start_time for t in self.tasks])

  def approximate_runtime(self):
    if self.num_tasks > 40:
      return self.total_runtime() / 40.
    return self.total_runtime() * 1.0 / self.num_tasks

  def total_runtime_faster_fetch(self, relative_fetch_time):
    return sum([t.runtime_faster_fetch(relative_fetch_time) for t in self.tasks])

  def approximate_runtime_faster_fetch(self, relative_fetch_time):
    if self.num_tasks > 40:
      return self.total_runtime_faster_fetch(relative_fetch_time) / 40.
    return self.total_runtime_faster_fetch(relative_fetch_time) * 1.0 / self.num_tasks

  def add_event(self, line):
    if line.find("TASK_TYPE") == -1:
      return
    self.num_tasks += 1

    items = line.split(" ")

    start_time = -1
    fetch_wait = -1
    finish_time = -1
    remote_bytes_read = 0
    for pair in items:
      if pair.find("=") == -1:
        continue
      key, value = pair.split("=")
      if key == "START_TIME":
        start_time = int(value)
      elif key == "FINISH_TIME":
        finish_time = int(value)
      elif key == "REMOTE_FETCH_WAIT_TIME":
        fetch_wait = int(value)
      elif key == "REMOTE_BYTES_READ":
        remote_bytes_read = int(value)

    if (start_time == -1 or finish_time == -1 or
        (self.has_fetch and fetch_wait == -1)):
      print ("Missing time on line %s! Start %s, fetch wait %s, finish %s" %
        (line, start_time, fetch_wait, finish_time))

    if self.start_time == -1:
      self.start_time = start_time
    else:
      self.start_time = min(self.start_time, start_time)

    if fetch_wait != -1:
      self.tasks.append(Task(start_time, fetch_wait, finish_time, remote_bytes_read))
      self.has_fetch = True
    else:
      self.tasks.append(Task(start_time, 0, finish_time, 0))

class Analyzer:
  def __init__(self, filename):
    f = open(filename, "r")
    # Map of stage IDs to Stages.
    self.stages = collections.defaultdict(Stage)
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
    start_and_finish_times = [(id, s.start_time, s.finish_time()) for id, s in self.stages.iteritems()]
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

  def analyze_for_speedup(self, relative_fetch_time): 
    print "********* Analyzing for relative fetch time %s ************" % relative_fetch_time
    # Subtract the overlap! No issues with weird fetch subtraction here because
    # the overlapping stages, at least for 3b, aren't the ones with a shuffle.
    total_time = -self.overlap
    total_time_with_faster_fetch = -self.overlap
    approx_total_time = 0
    approx_total_time_with_faster_fetch = 0

    self.simulated_total_time = 0
    self.simulated_total_time_with_faster_fetch = 0
    
    self.simulated_total_normalized_stragglers = 0
    self.simulated_total_no_stragglers = 0
    self.simulated_total_no_stragglers_with_faster_fetch = 0

    self.simulated_total_no_stragglers_2 = 0
    self.simulated_total_no_stragglers_no_fetch_2 = 0

    tasks_for_combined_stages = []
    for id, stage in self.stages.iteritems():
      print "Stage", id, stage
      stage_run_time = stage.finish_time() - stage.start_time
      total_time += stage_run_time
      print "Total time: ", stage.total_runtime(), "total w/o fetch:", stage.total_runtime_faster_fetch(relative_fetch_time), "Approx speedup: ", stage.total_runtime_faster_fetch(relative_fetch_time) * 1.0 / stage.total_runtime()
      print ("Approximate runtime: %s, without fetch: %s, speedup: %s" %
        (stage.approximate_runtime(), stage.approximate_runtime_faster_fetch(relative_fetch_time),
         stage.approximate_runtime_faster_fetch(relative_fetch_time) * 1.0 / stage.approximate_runtime()))
      if stage.has_fetch:
        time_with_faster_fetch = stage.runtime_with_faster_fetch(relative_fetch_time) 
        print ("Real run time: %s, without shuffle (no wave accounting): %s, Speedup: %s" %
          (stage_run_time, time_with_faster_fetch, time_with_faster_fetch * 1.0 / stage_run_time))
        total_time_with_faster_fetch += time_with_faster_fetch
      else:
        total_time_with_faster_fetch += stage.finish_time() - stage.start_time

      # The approximate time doesn't need to factor in the combined stages, because we're
      # effectively assuming that they run in series and each use the entire cluster,
      # which will end up w/ the same result.
      approx_total_time += stage.approximate_runtime()
      approx_total_time_with_faster_fetch += stage.approximate_runtime_faster_fetch(relative_fetch_time)

      if id in self.stages_to_combine:
        tasks_for_combined_stages.append(stage.tasks)
      else:
        self.add_stage_to_simulated_totals([stage.tasks], relative_fetch_time)

    if len(self.stages_to_combine) > 0:
      print "Combining stages:", self.stages_to_combine
      self.add_stage_to_simulated_totals(tasks_for_combined_stages, relative_fetch_time)

    print ("****************************************")
    speedup = total_time_with_faster_fetch * 1.0 / total_time
    print ("Total time: %s, without shuffle (no wave accounting): %s, speedup: %s" %
      (total_time, total_time_with_faster_fetch, speedup))

    approximate_speedup = approx_total_time_with_faster_fetch * 1.0 / approx_total_time
    print ("Approx total: %s, without shuffle: %s, speedup %s" %
      (approx_total_time, approx_total_time_with_faster_fetch, approximate_speedup))

    simulated_speedup = (self.simulated_total_time_with_faster_fetch * 1.0 /
      self.simulated_total_time)
    print ("Sim %s without shuffle %s speedup %s" %
      (self.simulated_total_time, self.simulated_total_time_with_faster_fetch, simulated_speedup))

    norm_stragglers_speedup = self.simulated_total_normalized_stragglers * 1.0 / self.simulated_total_time
    no_stragglers_speedup = self.simulated_total_no_stragglers * 1.0 / self.simulated_total_time
    no_stragglers_no_shuffle_speedup = (self.simulated_total_no_stragglers_with_faster_fetch * 1.0 /
      self.simulated_total_no_stragglers)
    print ("Speedup from normalizing stragglers: %s, no stragglers: %s, nostrag network imp: %s" %
      (norm_stragglers_speedup, no_stragglers_speedup, no_stragglers_no_shuffle_speedup))

    print ("Simulated no straggers (method 2): %s, no straggers or fetch (meth 2): %s" %
      (self.simulated_total_no_stragglers_2, self.simulated_total_no_stragglers_no_fetch_2))

    no_stragglers_speedup_2 = self.simulated_total_no_stragglers_2 * 1.0 / self.simulated_total_time
    no_stragglers_no_shuffle_speedup_2 = (self.simulated_total_no_stragglers_no_fetch_2 * 1.0 /
      self.simulated_total_no_stragglers_2)
    print ("Speedup from normalizing stragglers 2: %s, network imp: %s" %
      (no_stragglers_speedup_2, no_stragglers_no_shuffle_speedup_2))

    return (relative_fetch_time, approximate_speedup, simulated_speedup,
      no_stragglers_no_shuffle_speedup_2)

  def add_stage_to_simulated_totals(self, task_lists, relative_fetch_time):
    s = Simulation(task_lists, relative_fetch_time)
    self.simulated_total_time += s.runtime
    self.simulated_total_time_with_faster_fetch += s.runtime_faster_fetch
    print ("Simulated run time: %s, simulated runtime w/o shuffle: %s, speedup: %s" %
      (s.runtime, s.runtime_faster_fetch, s.runtime_faster_fetch * 1.0 / s.runtime))

    print ("Simulated norm stragglers: %s, no stragglers: %s, no stragglers or fetch: %s, speedup: %s" %
      (s.runtime_with_normalized_stragglers, s.runtime_with_no_stragglers,
       s.runtime_with_no_stragglers_and_no_fetch,
       s.runtime_with_no_stragglers_and_no_fetch * 1.0 / s.runtime_with_no_stragglers))
    self.simulated_total_normalized_stragglers += s.runtime_with_normalized_stragglers
    self.simulated_total_no_stragglers += s.runtime_with_no_stragglers
    self.simulated_total_no_stragglers_with_faster_fetch += s.runtime_with_no_stragglers_and_no_fetch 

    print ("Simulated no stragglers 2: %s, no stragglers or fetch 2: %s, speedup: %s" %
      (s.runtime_with_no_stragglers_2, s.runtime_with_no_stragglers_and_no_fetch_2,
       s.runtime_with_no_stragglers_and_no_fetch_2 * 1.0 / s.runtime_with_no_stragglers_2))
    self.simulated_total_no_stragglers_2 += s.runtime_with_no_stragglers_2
    self.simulated_total_no_stragglers_no_fetch_2 += s.runtime_with_no_stragglers_and_no_fetch_2
      
def main(argv):
  filename = argv[0]
  analyzer = Analyzer(filename)
  results_file = open("%s_improvements" % filename, "w")

  for speedup in [0, 0.25, 0.5, 0.75, 0.9]:
    results = analyzer.analyze_for_speedup(speedup)
    results_file.write("%s" % speedup)
    for result in results:
      results_file.write("\t%s" % result)
    results_file.write("\n")

if __name__ == "__main__":
  main(sys.argv[1:])
