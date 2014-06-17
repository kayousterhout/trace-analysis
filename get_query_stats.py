""" 
Outputs the number of joins and input size for each
unique query found in the job lobs in the supplied directory.
"""
import os
import sys

import parse_logs

class Query:
  def __init__(self, filename):
    analyzer = parse_logs.Analyzer(filename)
    self.total_input_size = 0
    self.total_shuffle_mb = 0
    for stage in analyzer.stages.values():
      self.total_input_size += sum([t.input_mb for t in stage.tasks])
      self.total_shuffle_mb += sum([t.shuffle_mb_written for t in stage.tasks])

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
  query_summary_filename = os.path.join(dirname, "query_summary")
  # Map of SQL queries to the amount of input data for that query.
  queries_to_input_size = {}
  for filename in os.listdir(argv[0]):
    full_name = os.path.join(dirname, filename)
    if os.path.isfile(full_name) and filename.endswith("job_log"):
      query = Query(full_name)

      if query.sql in queries_to_input_size:
        assert queries_to_input_size[query.sql].total_input_size == query.total_input_size
      elif not skip_load or query.num_joins > 0:
        queries_to_input_size[query.sql] = query
  for i, query in enumerate(sorted(queries_to_input_size.values(), key = lambda q: q.filename)):
    # Outputs 0 as the output size for now until more verbose output logging
    # gets added.
    print i, query.num_joins, query.total_input_size, query.total_shuffle_mb, 0, query.filename

if __name__ == "__main__":
  main(sys.argv[1:])

