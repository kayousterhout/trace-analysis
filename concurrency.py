import math

import task

def get_avg_concurrency(tasks):
  """ Returns the maximum number of tasks that were running concurrently. """
  total_task_time = sum([t.runtime() for t in tasks])
  total_time = max([t.finish_time for t in tasks]) - min([t.start_time for t in tasks])
  return math.ceil(float(total_task_time) / total_time)

def get_max_concurrency(tasks):
  if len(tasks) > 40:
    # For stages with a ton of tasks, the average is more accurage. With fewer tasks, stragglers
    # can skew results, so the max is better.
    return get_avg_concurrency(tasks)
  else:
    return get_max_concurrency_real(tasks)

def get_max_concurrency_real(tasks):
  """ Returns the maximum number of tasks that were running concurrently. """
  begin_end_events = []
  for t in tasks:
    begin_end_events.append((t.start_time, 1))
    # Use the start time plus runtime instead of finish time, because the finish time is sometimes
    # greater than the next task's start time due to asynchrony in the scheduler.
    begin_end_events.append((t.finish_time - t.scheduler_delay, -1))

  assert(len(begin_end_events) == 2 * len(tasks))
  # Built-in sort function works perfectly here: want to use first item as primary
  # key and second item as secondary key, so that end events end up before start events.
  begin_end_events.sort()
  max_concurrency = 0
  current_concurrency = 0
  for time, event in begin_end_events:
    current_concurrency += event
    max_concurrency = max(max_concurrency, current_concurrency)
  return max_concurrency


