""" 
Outputs the number of joins and input size for each
unique query found in the job lobs in the supplied directory.
"""
from collections import defaultdict
import os
import sys

import numpy

import parse_logs

class Query:
  def __init__(self, filename):
    analyzer = parse_logs.Analyzer(filename)
    self.total_input_size = 0
    self.total_shuffle_mb = 0
    self.total_output_mb = 0
    self.runtime = 0
    self.no_disk_runtime = analyzer.no_disk_speedup()[2]
    for stage in analyzer.stages.values():
      self.total_input_size += sum([t.input_mb for t in stage.tasks])
      self.total_shuffle_mb += sum([t.shuffle_mb_written for t in stage.tasks])
      self.total_output_mb += sum([t.output_mb for t in stage.tasks])
      self.runtime += stage.finish_time() - stage.start_time

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
  # Map of SQL queries to the amount of input data for that query.
  query_sql_to_data = {}
  query_sql_to_runtimes = defaultdict(list)
  query_sql_to_no_disk_runtimes= defaultdict(list)
  for filename in os.listdir(argv[0]):
    full_name = os.path.join(dirname, filename)
    if os.path.isfile(full_name) and filename.endswith("job_log"):
      print "Reading %s" % filename
      query = Query(full_name)

      if query.sql in query_sql_to_data:
        expected_input_size = query_sql_to_data[query.sql].total_input_size
        if abs(expected_input_size - query.total_input_size) > 100000:
          print ("Mismatch in query sizes: for query %s, %s not equal to %s" %
            (query.sql, query.total_input_size, expected_input_size))
          assert False
      elif not skip_load or query.num_joins > 0:
        query_sql_to_data[query.sql] = query

      query_sql_to_runtimes[query.sql].append(query.runtime)
      query_sql_to_no_disk_runtimes[query.sql].append(query.no_disk_runtime)

  query_summary_filename = os.path.join(dirname, "query_summary")
  query_summary_file = open(query_summary_filename, "w")
  print "Writing results to", query_summary_filename
  for i, query in enumerate(sorted(query_sql_to_data.values(), key = lambda q: q.filename)):
    # Outputs 0 as the output size for now until more verbose output logging
    # gets added.
    runtimes = query_sql_to_runtimes[query.sql]
    print "%s queries for %s" % (len(runtimes), query.sql[:15])
    no_disk_runtimes = query_sql_to_no_disk_runtimes[query.sql]
    to_write = [i, query.num_joins, query.total_input_size, query.total_shuffle_mb,
      query.total_output_mb, query.filename, numpy.mean(runtimes), min(runtimes), max(runtimes),
      numpy.mean(no_disk_runtimes)]
    to_write_str = "\t".join([str(s) for s in to_write])
    query_summary_file.write(to_write_str)
    query_summary_file.write("\n")

if __name__ == "__main__":
  main(sys.argv[1:])

