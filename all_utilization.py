import numpy

import os
import sys

import parse_logs

def plot_cdf(values, filename):
    f = open(filename, "w")

def write_data_to_file(data, file_handle):
  stringified_data = [str(x) for x in data]
  stringified_data += "\n"
  file_handle.write("\t".join(stringified_data))

def main(argv):
  disk_utilizations = []
  cpu_utilizations = []
  network_utilizations = []
  dirname = argv[0]
  for filename in os.listdir(dirname):
    full_name = os.path.join(dirname, filename)
    if os.path.isfile(full_name) and filename.endswith("job_log"):
      print "Reading %s" % filename
      analyzer = parse_logs.Analyzer(full_name)

      for (id, stage) in analyzer.stages.iteritems():
        for task in stage.tasks:
          cpu_utilizations.append(task.total_cpu_utilization / 8.)
          network_utilizations.append(task.network_bytes_transmitted_ps / (1000*1000*1000))
          network_utilizations.append(task.network_bytes_received_ps / (1000*1000*1000))
          for name, block_device_numbers in task.disk_utilization.iteritems():
            if name in ["xvdb", "xvdf"]:
              disk_utilizations.append(block_device_numbers[0])

  output_filename = os.path.join(dirname, "cpu_disk_utilization_cdf")
  f = open(output_filename, "w")
  print max(network_utilizations)
  for percent in range(100):
    f.write("%s\t%s\t%s\t%s\n" % (percent / 100., numpy.percentile(cpu_utilizations, percent),
      numpy.percentile(disk_utilizations, percent),
      numpy.percentile(network_utilizations, percent)))
  f.close()

if __name__ == "__main__":
  main(sys.argv[1:])
