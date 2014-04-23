import os
import sys

import parse_logs

def main(argv):
  dirname = argv[0]
  print "Parsing files in ", dirname
  agg_results_filename = os.path.join(dirname, "agg_results")
  for filename in os.listdir(argv[0]):
    full_name = os.path.join(dirname, filename)
    if os.path.isfile(full_name) and filename.endswith("job_log"):
      print "Parsing file ", full_name
      parse_logs.parse(full_name, agg_results_filename)

if __name__ == "__main__":
  main(sys.argv[1:])
