import sys

import parse_logs

NUM_MACHINES = 5
DISKS_PER_MACHINE = 2
CPUS_PER_MACHINE = 8
# Estimate of upper bound on disk throughput
DISK_MB_PER_SECOND = 100

# Based on EC2's advertised 1000Mbps. These seem to be full duplex links.
# I've observed MB/s as high as 140.
NETWORK_MB_PER_SECOND = 125

def main(argv):
  filename = argv[0]
  analyzer = parse_logs.Analyzer(filename)

  total_job_runtime = 0
  actual_job_runtime = 0
  for id in sorted(analyzer.stages.keys(), reverse=True):
    stage = analyzer.stages[id]
    total_cpu_milliseconds = 0
    total_input_data = 0
    total_hdfs_output_data = 0
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
      total_hdfs_output_data += task.output_mb
      if task.has_fetch:
        total_remote_mb_read += task.remote_mb_read
        total_input_data += task.local_mb_read
    print "*******************Stage runtime: ", stage.finish_time() - stage.start_time

    print "Total millis across all tasks: ", total_machine_time_spent
    print "Total millis including scheduler delay: ", total_runtime_incl_delay
    print "Total CPU millis: ", total_cpu_milliseconds
    min_cpu_milliseconds = total_cpu_milliseconds / (NUM_MACHINES * CPUS_PER_MACHINE)
    print "Total input MB: ", total_input_data
    print "Total remote MB: ", total_remote_mb_read
    print "Total output MB: ", total_hdfs_output_data
    total_input_disk_milliseconds = 1000 * total_input_data / DISK_MB_PER_SECOND
    # TODO: This doesn't include logging for the disk output size, which currently needs to be done
    # manually.
    print "Total shuffle write MB: ", total_shuffle_write_mb
    total_output_disk_milliseconds = ((total_shuffle_write_mb + total_hdfs_output_data) /
      DISK_MB_PER_SECOND)

    min_disk_milliseconds = ((total_input_disk_milliseconds + total_output_disk_milliseconds) /
      (NUM_MACHINES * DISKS_PER_MACHINE))

    print "Min disk millis: %s, min cpu millis: %s" % (min_disk_milliseconds, min_cpu_milliseconds)
    # Add twice the amount of HDFS output data because the data needs to be sent to two locations.

    total_network_mb = total_remote_mb_read + 2 * total_hdfs_output_data
    total_network_milliseconds = 1000 * total_network_mb / NETWORK_MB_PER_SECOND
    min_network_milliseconds = total_network_milliseconds / NUM_MACHINES
    print "Min network millis: %s" % (min_network_milliseconds)

    min_stage_runtime = max(min_disk_milliseconds, min_cpu_milliseconds, min_network_milliseconds)
    print "Min stage runtime: ", min_stage_runtime
    total_job_runtime += min_stage_runtime
    actual_job_runtime += stage.finish_time() - stage.start_time
  print "Total job runtime:", total_job_runtime, "milliseconds" 
  print "Actual job runtime:", actual_job_runtime, "milliseconds"
    

if __name__ == "__main__":
  main(sys.argv[1:])
