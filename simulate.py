import bisect

""" Simulates the completion time of a set of tasks with the given runtimes, given # of slots. """
def simulate(task_runtimes, num_slots):
  # Sorted list of task finish times, measured as the time from when the job started.
  finish_times = []
  # Start and finish time for each task.
  start_finish_times = []
  # Start num_slots tasks.
  while len(finish_times) < num_slots and len(task_runtimes) > 0:
    task_runtime = task_runtimes.pop(0)
    start_finish_times.append((0, task_runtime))
    bisect.insort_left(finish_times, task_runtime)

  while len(task_runtimes) > 0:
    start_time = finish_times.pop(0)
    finish_time = start_time + task_runtimes.pop(0)
    start_finish_times.append((start_time, finish_time))
    bisect.insort_left(finish_times, finish_time)
    assert(num_slots == len(finish_times))

  # Job finishes when the last task is done.
  return finish_times[-1], start_finish_times
