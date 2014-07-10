import sys

from parse_logs import Analyzer


def output_per_task_info(id, stage, filename):
  f = open("%s_%s" % (filename, id), "w")
  headers = ["Start time", "Runtime", "input size (MB)", "Progress rate", "Input read time",
    "Fetch wait", "Compute time", "Compute w/o GC", "GC time", "Serialization time",
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
      task.estimated_serialization_millis + task.estimated_deserialization_millis,
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

  for id, stage in analyzer.stages.iteritems():
    output_per_task_info(id, stage, filename)

if __name__ == "__main__":
  main(sys.argv[1:])
