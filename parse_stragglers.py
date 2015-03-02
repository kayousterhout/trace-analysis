import sys

from parse_logs import Analyzer

def output_per_task_info(stage_id, job_id, stage, filename):
  f = open("%s_details_%s_%s" % (filename, job_id, stage_id), "w")
  f.write("Total tasks: %s\tStragglers: %s\\n" %
    (len(stage.tasks), stage.progress_rate_stragglers()[0]))
  headers = ["Start time", "Runtime", "input size (MB)", "Progress rate", "Input read time",
    "Fetch wait", "Compute time", "Compute w/o GC", "GC time", "Broadcast time",
    "Shuffle write", "Executor", "Scheduler Delay"]
  f.write("\t".join(headers))
  f.write("\n")
  for task in stage.tasks:
    if task.input_size_mb() > 0:
      progress_rate = task.runtime() * 1.0 / task.input_size_mb() 
    else:
      progress_rate = float("inf")
    fetch_wait = 0
    if task.has_fetch:
      fetch_wait = task.fetch_wait
    items = [
      task.start_time,
      task.runtime(),
      task.input_size_mb(),
      progress_rate,
      task.input_read_time,
      fetch_wait,
      task.compute_time(),
      task.compute_time_without_gc(),
      task.gc_time,
      task.broadcast_time,
      task.shuffle_write_time,
      task.executor,
      task.scheduler_delay,
      task.data_local]

    f.write("\t".join([str(item) for item in items]))
    f.write("\n")
  f.close()    

def main(argv):
  filename = argv[0]
  analyzer = Analyzer(filename)

  for job_id, job in analyzer.jobs.iteritems():
    if "4_" in job_id:
      for id, stage in job.stages.iteritems():
        output_per_task_info(id, job_id, stage, filename)

if __name__ == "__main__":
  main(sys.argv[1:])
