import sys

import parse_logs

NUM_MACHINES = 5
DISKS_PER_MACHINE = 2
CPUS_PER_MACHINE = 8
# Estimate of upper bound on disk throughput
DISK_MB_PER_SECOND = 100

def main(argv):
  filename = argv[0]
  analyzer = parse_logs.Analyzer(filename)

  total_job_runtime = 0
  for (id, stage) in analyzer.stages.iteritems():
    total_cpu_milliseconds = 0
    total_input_data = 0
    total_remote_mb_read = 0
    total_shuffle_write_mb = 0
    total_machine_time_spent = 0
    total_runtime_incl_delay = 0

    for task in stage.tasks:
      total_cpu_milliseconds += task.process_cpu_utilization * task.executor_run_time
      total_input_data += task.input_mb
      total_shuffle_write_mb += task.shuffle_mb_written
      total_machine_time_spent += task.executor_run_time
      total_runtime_incl_delay += task.runtime()
      if task.has_fetch:
        total_remote_mb_read += task.remote_mb_read
        total_input_data += task.local_mb_read

    print "Total millis across all tasks: ", total_machine_time_spent
    print "Total millis including scheduler delay: ", total_runtime_incl_delay
    print "Total CPU millis: ", total_cpu_milliseconds
    min_cpu_milliseconds = total_cpu_milliseconds / (NUM_MACHINES * CPUS_PER_MACHINE)
    print "Total input MB: ", total_input_data
    print "Total remote MB: ", total_remote_mb_read
    total_input_disk_milliseconds = 1000 * total_input_data / DISK_MB_PER_SECOND
    # TODO: This doesn't include logging for the disk output size, which currently needs to be done
    # manually.
    print "Total shuffle write MB: ", total_shuffle_write_mb
    total_output_disk_milliseconds = total_shuffle_write_mb / DISK_MB_PER_SECOND

    min_disk_milliseconds = ((total_input_disk_milliseconds + total_output_disk_milliseconds) /
      (NUM_MACHINES * DISKS_PER_MACHINE))

    print "Min disk millis: %s, min cpu millis: %s" % (min_disk_milliseconds, min_cpu_milliseconds)
    total_job_runtime += max(min_disk_milliseconds, min_cpu_milliseconds)
  print "Total job runtime:", total_job_runtime, "milliseconds" 
    

if __name__ == "__main__":
  main(sys.argv[1:])
