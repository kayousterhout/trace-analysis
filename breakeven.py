""" 
Outputs some stuff we wanted for a 2014/08/27 meeting.
"""
from collections import defaultdict
import os
import sys

import numpy

import parse_logs

class Query:
  def __init__(self, filename):
    analyzer = parse_logs.Analyzer(filename)
    self.total_disk_input_mb = 0
    self.total_input_mb = 0
    self.total_shuffle_write_mb = 0
    self.total_shuffle_read_mb = 0
    self.total_output_mb = 0
    self.runtime = 0
    self.total_shuffle_time = 0
    self.total_reduce_time = 0
    self.total_reduce_cpu_time = 0
    self.total_runtime = 0
    self.total_cpu_time = 0
    for stage in analyzer.stages.values():
      self.total_disk_input_mb += sum([t.input_mb for t in stage.tasks if t.input_read_method != "Memory"])
      self.total_input_mb += sum([t.input_mb for t in stage.tasks])
      self.total_shuffle_write_mb += sum([t.shuffle_mb_written for t in stage.tasks])
      self.total_shuffle_read_mb += sum([t.remote_mb_read + t.local_mb_read for t in stage.tasks if t.has_fetch])
      self.total_output_mb += sum([t.output_mb for t in stage.tasks])
      self.runtime += stage.finish_time() - stage.start_time
      self.total_shuffle_time += sum([t.fetch_wait for t in stage.tasks if t.has_fetch])
      self.total_reduce_time += sum([t.runtime() for t in stage.tasks if t.has_fetch])
      self.total_runtime += sum([t.runtime() for t in stage.tasks])
      self.total_cpu_time += sum(
        [t.process_cpu_utilization * t.executor_run_time for t in stage.tasks])
      self.total_reduce_cpu_time += sum(
        [t.process_cpu_utilization * t.executor_run_time for t in stage.tasks if t.has_fetch])
        #Comment this line in to estimate the effective CPU time when multiple tasks are running
        #concurrently.
        #[t.compute_time_without_gc() for t in stage.tasks if t.has_fetch])

    # Get the SQL query for this file.
    self.sql = ""
    for line in open(filename, "r"):
      if line.startswith("STAGE_ID"):
        break
      self.sql += line
    
    self.filename = filename

    self.num_joins = self.sql.lower().count("join")

def main(argv):
  dirname = argv[0]
  skip_load = False
  if len(argv) > 1 and argv[1].lower() == "true":
    skip_load = True
  print "Parsing queries in ", dirname
  shuffle_bytes_to_input_bytes = []
  shuffle_time_to_reduce_time = []
  shuffle_time_to_total_time = []
  reduce_breakeven_speeds = []
  total_breakeven_speeds = []
  disk_breakeven_speeds = []
  for filename in os.listdir(argv[0]):
    full_name = os.path.join(dirname, filename)
    if os.path.isfile(full_name) and filename.endswith("job_log"):
      print "Reading %s" % filename
      query = Query(full_name)

      # Compute disk breakeven speed (in MB/s).
      # Shuffled data has to be written to disk and later read back, so multiply by 2.
      # Output data has to be written to 3 disks.
      total_disk_mb = query.total_disk_input_mb + query.total_shuffle_write_mb + query.total_shuffle_read_mb + 3 * query.total_output_mb
      # To compute the breakeven speed, need to normalize for the number of disks per machine (2) and
      # number of cores (8).
      disk_breakeven_speeds.append((total_disk_mb / 2.) / (query.total_cpu_time / (8 * 1000.)))
      print "Disk breakeven speed: %s" % disk_breakeven_speeds[-1]

      if query.total_shuffle_read_mb > 0:
        # Megabits / second that would result in the network time being the same as the compute time
        # for shuffle phases.
        # Multiply by 8 to account for the fact that there are 8 cores per machine.
        reduce_breakeven_speeds.append((query.total_shuffle_read_mb + 2 * query.total_output_mb) * 8 * 8 /
          (query.total_reduce_cpu_time / 1000.))
        print "Shuffle MB: %s, output MB: %s, total reduce compute time: %s" % (query.total_shuffle_read_mb,
          query.total_output_mb, query.total_reduce_cpu_time)
        print "Breakeven speed: %s" % reduce_breakeven_speeds[-1]
        total_breakeven_speeds.append(reduce_breakeven_speeds[-1] *
          query.total_reduce_cpu_time / query.total_cpu_time)
        shuffle_bytes_to_input_bytes.append(query.total_shuffle_read_mb * 1.0 / query.total_input_mb)
        shuffle_time_to_reduce_time.append(query.total_shuffle_time * 1.0 / query.total_reduce_time)
        shuffle_time_to_total_time.append(query.total_shuffle_time * 1.0 / query.total_runtime)

  query_summary_filename = os.path.join(dirname, "query_breakeven_summary")
  query_summary_file = open(query_summary_filename, "w")
  print "Writing results to", query_summary_filename
  for i in range(1, 100):
    query_summary_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
      i * 1.0 / 100,
      numpy.percentile(shuffle_bytes_to_input_bytes, i),
      numpy.percentile(shuffle_time_to_reduce_time, i),
      numpy.percentile(shuffle_time_to_total_time, i),
      numpy.percentile(reduce_breakeven_speeds, i),
      numpy.percentile(total_breakeven_speeds, i),
      numpy.percentile(disk_breakeven_speeds, i)))

if __name__ == "__main__":
  main(sys.argv[1:])

