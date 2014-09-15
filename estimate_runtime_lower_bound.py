import sys

import parse_logs

NUM_MACHINES = 5
DISKS_PER_MACHINE = 2
CPUS_PER_MACHINE = 8
# Estimate of reasonable disk throughput
DISK_MB_PER_SECOND = 50

# Based on EC2's advertised 1000Mbps. These seem to be full duplex links.
# I've observed MB/s as high as 140.
NETWORK_MB_PER_SECOND = 125

def estimate(filename):
  analyzer = parse_logs.Analyzer(filename)

  total_job_runtime = 0
  actual_job_runtime = 0
  min_job_cpu_millis = 0
  total_job_cpu_millis = 0
  min_job_network_millis = 0
  min_job_disk_millis = 0

  # Used as a sanity check: shuffle write and shuffle read should be the same.
  all_stages_shuffle_write_mb = 0
  all_stages_shuffle_read_mb = 0
  all_stages_disk_input_mb = 0
  for id in sorted(analyzer.stages.keys(), reverse=True):
    stage = analyzer.stages[id]
    total_cpu_milliseconds = 0
    total_disk_input_data = 0
    total_hdfs_output_data = 0
    total_remote_mb_read = 0
    total_shuffle_read_mb = 0
    total_shuffle_write_mb = 0
    total_machine_time_spent = 0
    total_runtime_incl_delay = 0

    print "*****STAGE has %s tasks" % len(stage.tasks)
    for task in stage.tasks:
      total_cpu_milliseconds += task.process_cpu_utilization * task.executor_run_time
      if task.input_read_method != "Memory":
        total_disk_input_data += task.input_mb
      total_shuffle_write_mb += task.shuffle_mb_written
      total_machine_time_spent += task.executor_run_time
      total_runtime_incl_delay += task.runtime()
      total_hdfs_output_data += task.output_mb
      if task.has_fetch:
        total_remote_mb_read += task.remote_mb_read
        # Remote MB still need to be read from disk.
        shuffle_mb = task.local_mb_read + task.remote_mb_read
        total_disk_input_data += shuffle_mb
        total_shuffle_read_mb += shuffle_mb
    all_stages_shuffle_write_mb += total_shuffle_write_mb
    all_stages_shuffle_read_mb += total_shuffle_read_mb
    all_stages_disk_input_mb += total_disk_input_data
    print "*******************Stage runtime: ", stage.finish_time() - stage.start_time

    print "Total millis across all tasks: ", total_machine_time_spent
    print "Total millis including scheduler delay: ", total_runtime_incl_delay
    print "Total CPU millis: ", total_cpu_milliseconds
    min_cpu_milliseconds = total_cpu_milliseconds / (NUM_MACHINES * CPUS_PER_MACHINE)
    print "Total input MB: ", total_disk_input_data
    print "Total remote MB: ", total_remote_mb_read
    print "Total shuffle read MB: ", total_shuffle_read_mb
    print "Total output MB: ", total_hdfs_output_data
    total_input_disk_milliseconds = 1000 * total_disk_input_data / DISK_MB_PER_SECOND
    total_output_disk_milliseconds = 1000* ((total_shuffle_write_mb + total_hdfs_output_data) /
      DISK_MB_PER_SECOND)

    min_disk_milliseconds = ((total_input_disk_milliseconds + total_output_disk_milliseconds) /
      (NUM_MACHINES * DISKS_PER_MACHINE))

    print "Min disk millis: %s, min cpu millis: %s" % (min_disk_milliseconds, min_cpu_milliseconds)
    # Add twice the amount of HDFS output data because the data needs to be sent to two locations.

    print "Total shuffle write MB: ", total_shuffle_write_mb
    total_network_mb = total_remote_mb_read + 2 * total_hdfs_output_data
    total_network_milliseconds = 1000 * total_network_mb / NETWORK_MB_PER_SECOND
    min_network_milliseconds = total_network_milliseconds / NUM_MACHINES
    print "Min network millis: %s" % (min_network_milliseconds)

    min_stage_runtime = max(min_disk_milliseconds, min_cpu_milliseconds, min_network_milliseconds)
    print "Min stage runtime: ", min_stage_runtime
    total_job_runtime += min_stage_runtime
    actual_job_runtime += stage.finish_time() - stage.start_time

    min_job_cpu_millis += min_cpu_milliseconds
    total_job_cpu_millis += total_cpu_milliseconds
    min_job_network_millis += min_network_milliseconds
    min_job_disk_millis += min_disk_milliseconds

  print "--------------------------------------------------------------"
  print "Total pipelined job runtime:", total_job_runtime, "milliseconds"
  total_not_pipelined_runtime = min_job_cpu_millis + min_job_network_millis + min_job_disk_millis
  print "Total not pipelined job runtime:", total_not_pipelined_runtime, "milliseconds"
  print "Min CPU milliseconds for job: %s milliseconds (%s total)" % (min_job_cpu_millis, total_job_cpu_millis)
  print "Min network milliseconds for job", min_job_network_millis, "milliseconds"
  print "Min disk milliseconds for job", min_job_disk_millis, "milliseconds"
  print "Actual job runtime:", actual_job_runtime, "milliseconds"
  print ("Shuffle write MB: %s, read MB: %s, all input: %s" %
    (all_stages_shuffle_write_mb, all_stages_shuffle_read_mb, all_stages_disk_input_mb))
  return (total_not_pipelined_runtime, total_job_runtime, min_job_cpu_millis,
    min_job_network_millis, min_job_disk_millis, total_job_cpu_millis)

def main(argv):
  print estimate(argv[0])

if __name__ == "__main__":
  main(sys.argv[1:])
