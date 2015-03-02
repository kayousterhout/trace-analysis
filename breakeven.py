""" 
Outputs some stuff we wanted for a 2014/08/27 meeting.
"""
from collections import defaultdict
import os
import sys

import numpy

import parse_logs

class CpuUtilization(object):
  def __init__(self):
    self.start_millis = -1
    self.start_jiffies = -1
    self.end_millis = -1
    self.end_jiffies = -1

  def add_task(self, task):
    if self.start_millis == -1 or task.start_cpu_utilization_millis < self.start_millis:
      assert(self.start_jiffies == -1 or task.start_total_cpu_jiffies <= self.start_jiffies)
      self.start_millis = task.start_cpu_utilization_millis
      self.start_jiffies = task.start_total_cpu_jiffies

    if task.end_cpu_utilization_millis > self.end_millis:
      assert(task.end_total_cpu_jiffies >= self.end_jiffies)
      self.end_millis = task.end_cpu_utilization_millis
      self.end_jiffies = task.end_total_cpu_jiffies

  def non_idle_millis(self):
    millis = (self.end_jiffies - self.start_jiffies) * 10
    print "Non idle millis: %s, total millis: %s" % (millis, self.end_millis - self.start_millis)
    return millis


class Query:
  def __init__(self, job):
    self.total_disk_input_mb = 0
    self.total_input_mb = 0
    self.total_shuffle_write_mb = 0
    self.total_shuffle_read_mb = 0
    self.total_output_mb = 0
    self.runtime = 0
    self.total_reduce_time = 0
    self.total_reduce_cpu_time = 0
    self.total_runtime = 0
    self.total_cpu_time = 0
    self.executor_id_to_utilization = defaultdict(CpuUtilization)
    self.executor_id_to_reduce_utilization = defaultdict(CpuUtilization)
    self.total_read_estimate = 0
    for stage_id, stage in job.stages.iteritems():
      self.total_disk_input_mb += sum([t.input_mb for t in stage.tasks if t.input_read_method != "Memory"])
      self.total_input_mb += sum([t.input_mb for t in stage.tasks])
      self.total_shuffle_write_mb += sum([t.shuffle_mb_written for t in stage.tasks])
      self.total_shuffle_read_mb += sum([t.remote_mb_read + t.local_mb_read for t in stage.tasks if t.has_fetch])
      self.total_output_mb += sum([t.output_mb for t in stage.tasks])
      self.runtime += stage.finish_time() - stage.start_time
      self.total_reduce_time += sum([t.runtime() for t in stage.tasks if t.has_fetch])
      self.total_runtime += sum([t.runtime() for t in stage.tasks])
      for task in stage.tasks:
        self.executor_id_to_utilization[task.executor_id].add_task(task)
        if task.has_fetch:
          self.executor_id_to_reduce_utilization[task.executor_id].add_task(task)
        for name, block_device_numbers in task.disk_utilization.iteritems():
          if name in ["xvdb", "xvdf"]:
            self.total_read_estimate += task.runtime() * block_device_numbers[1] / 8000.

  def non_idle_reduce_cpu_millis(self):
    print "Non idle reduce millis"
    return sum([u.non_idle_millis() for u in self.executor_id_to_reduce_utilization.values()])

  def non_idle_cpu_millis(self):
    print "All non-idle millis"
    return sum([u.non_idle_millis() for u in self.executor_id_to_utilization.values()])

def main(argv):
  filename = argv[0]
  skip_load = False
  if len(argv) > 1 and argv[1].lower() == "true":
    skip_load = True
  print "Parsing queries in ", filename
  shuffle_bytes_to_input_bytes = []
  reduce_breakeven_speeds = []
  total_breakeven_speeds = []
  disk_breakeven_speeds = []

  analyzer = parse_logs.Analyzer(filename)
  for job_id, job in analyzer.jobs.iteritems():
    query = Query(job)
    non_idle_cpu_millis = query.non_idle_cpu_millis()
    print "Elapsed cpu millis", non_idle_cpu_millis / 8.

    # Compute disk breakeven speed (in MB/s).
    # Shuffled data has to be written to disk and later read back, so multiply by 2.
    # Output data has to be written to 3 disks.
    total_disk_mb = (query.total_disk_input_mb + query.total_shuffle_write_mb +
      query.total_shuffle_read_mb + 3 * query.total_output_mb)
    # To compute the breakeven speed, need to normalize for the number of disks per machine (2) and
    # number of cores (8).
    disk_breakeven_speeds.append((total_disk_mb / 2.) / (non_idle_cpu_millis / (8 * 1000.)))
    print "Disk breakeven speed: %s" % disk_breakeven_speeds[-1]

    total_breakeven_speeds.append((query.total_shuffle_read_mb + 2 * query.total_output_mb) * 8 * 8 /
      (non_idle_cpu_millis / 1000.))
    print "Input MB: %s, Estimate: %s, Shuffle MB: %s, output MB: %s, total compute time: %s" % (
      query.total_disk_input_mb,
      query.total_read_estimate,
      query.total_shuffle_read_mb,
      query.total_output_mb, non_idle_cpu_millis)
    if query.total_shuffle_read_mb > 0:
      # Megabits / second that would result in the network time being the same as the compute time
      # for shuffle phases.
      # Multiply by 8 to account for the fact that there are 8 cores per machine.
      print "Breakeven speed: %s" % reduce_breakeven_speeds[-1]
      reduce_breakeven_speeds.append(reduce_breakeven_speeds[-1] *
        non_idle_cpu_millis / query.non_idle_reduce_cpu_millis())
      shuffle_bytes_to_input_bytes.append(query.total_shuffle_read_mb * 1.0 / query.total_input_mb)

  print shuffle_bytes_to_input_bytes
  print reduce_breakeven_speeds
  print "Total network breakeven", total_breakeven_speeds
  print "Disk breakeven", disk_breakeven_speeds

  query_summary_filename = "%s_query_breakeven_summary" % filename
  query_summary_file = open(query_summary_filename, "w")
  print "Writing results to", query_summary_filename
  for i in range(1, 100):
    query_summary_file.write("%s\t%s\t%s\t%s\t%s\n" % (
      i * 1.0 / 100,
      numpy.percentile(shuffle_bytes_to_input_bytes, i),
      numpy.percentile(reduce_breakeven_speeds, i),
      numpy.percentile(total_breakeven_speeds, i),
      numpy.percentile(disk_breakeven_speeds, i)))

if __name__ == "__main__":
  main(sys.argv[1:])

