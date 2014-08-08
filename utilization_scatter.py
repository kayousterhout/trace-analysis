import sys

import parse_logs

def write_data_to_file(data, file_handle):
  stringified_data = [str(x) for x in data]
  stringified_data += "\n"
  file_handle.write("\t".join(stringified_data))

def main(argv):
  filename = argv[0]
  analyzer = parse_logs.Analyzer(filename)

  start_time = min([x.start_time for x in analyzer.stages.values()])

  for (id, stage) in analyzer.stages.iteritems():
    stage_filename = "%s_%s_utilization" % (filename, id)
    f = open(stage_filename, "w")

    for task in stage.tasks:
      items = [task.start_time, task.executor_run_time, task.total_cpu_utilization]
      for block_device_numbers in task.disk_utilization.values():
        items.extend(block_device_numbers)
      items.append(task.network_bytes_transmitted_ps / 125000000)
      items.append(task.network_bytes_received_ps / 125000000)
      write_data_to_file(items, f)
    f.close()    

    plot_base_file = open("utilization_scatter_base.gp", "r")
    plot_file = open("%s_%s_utilization.gp" % (filename, id), "w")
    for line in plot_base_file:
      plot_file.write(line)
    plot_base_file.close()
    plot_file.write("set output \"%s_%s_utilization.pdf\"\n" % (filename, id))
    plot_file.write("plot \"%s\" using ($1-%s):4 with p title \"Disk1\",\\\n" %
      (stage_filename, start_time))
    plot_file.write("\"%s\" using ($1-%s):7 with p title \"Disk2\",\\\n" %
      (stage_filename, start_time))
    plot_file.write("\"%s\" using ($1-%s):13 with p title\"Network T\",\\\n" %
      (stage_filename, start_time))
    plot_file.write("\"%s\" using ($1-%s):14 with p title\"Network R\",\\\n" %
      (stage_filename, start_time))
    plot_file.write("\"%s\" using ($1-%s):($3/8) with p title \"CPU\"\n" %
      (stage_filename, start_time))
    plot_file.close()
  

if __name__ == "__main__":
  main(sys.argv[1:])
