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
  map_utilizations = []
  reduce_utilizations = []
  all_utilizations = []
  dirname = argv[0]
  for filename in os.listdir(dirname):
    full_name = os.path.join(dirname, filename)
    if os.path.isfile(full_name) and filename.endswith("job_log"):
      print "Reading %s" % filename
      analyzer = parse_logs.Analyzer(full_name)

      for (id, stage) in analyzer.stages.iteritems():
        for task in stage.tasks:
          for name, block_device_numbers in task.disk_utilization.iteritems():
            if name in ["xvdb", "xvdf"]:
              effective_util = 0
              if block_device_numbers[0] > 0:
                effective_util = (block_device_numbers[1] + block_device_numbers[2]) / block_device_numbers[0]
              all_utilizations.append(effective_util)
              if task.has_fetch:
                reduce_utilizations.append(effective_util)
              else:
                map_utilizations.append(effective_util)

  output_filename = os.path.join(dirname, "disk_utilization_cdf")
  f = open(output_filename, "w")
  for percent in range(100):
    f.write("%s\t%s\t%s\t%s\n" % (percent / 100., numpy.percentile(map_utilizations, percent),
      numpy.percentile(reduce_utilizations, percent),
      numpy.percentile(all_utilizations, percent)))
  f.close()

if __name__ == "__main__":
  main(sys.argv[1:])
