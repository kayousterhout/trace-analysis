""" 
Outputs the number of joins and input size for each
unique query found in the job lobs in the supplied directory.
"""
import os
import sys

import parse_logs

def main(argv):
  dirname = argv[0]
  print "Parsing queries in ", dirname
  query_summary_filename = os.path.join(dirname, "query_summary")
  # Map of SQL queries to the amount of input data for that query.
  queries_to_input_size = {}
  for filename in os.listdir(argv[0]):
    full_name = os.path.join(dirname, filename)
    if os.path.isfile(full_name) and filename.endswith("job_log"):
      print "Parsing file ", full_name
      analyzer = parse_logs.Analyzer(full_name)
      total_input_size = 0
      for stage in analyzer.stages.values():
        total_input_size += sum([t.input_mb for t in stage.tasks]) 

      # Get the SQL query for this file.
      query = ""
      for line in open(full_name, "r"):
        if line.startswith("STAGE_ID"):
          break
        query += line

      if query in queries_to_input_size:
        assert queries_to_input_size[query] == total_input_size
      else:
        queries_to_input_size[query] = total_input_size
  for query, input_size in queries_to_input_size.iteritems():
    print query.count("join"), input_size 

if __name__ == "__main__":
  main(sys.argv[1:])

