class BigDataBenchmarkHelper(object):
  def __init__(self, skip_first_query): 
    self.dropped_job_ids = set()
    self.queries_parsed = set()
    self.prev_job_id = ""
    self.skip_first_query = skip_first_query

  def get_job_id_from_properties(self, spark_job_id, stage_ids, properties):
    job_description = properties["spark.job.description"]
    query_name = ""
    use_previous_job_id = False
    if "pageRank FROM rankings WHERE pageRank" in job_description:
      if "1000" in job_description:
        query_name = "1a"
      elif "100" in job_description:
        query_name = "1b"
      else:
        query_name = "1c"
    elif "SUBSTR(sourceIP" in job_description:
      if "12" in job_description:
        query_name = "2c"
      elif "10" in job_description:
        query_name = "2b"
      else:
        query_name = "2a"
    elif "UV.visitDate" in job_description:
      if "1980-04-01" in job_description:
        query_name = "3a"
      elif "2010-01-01" in job_description:
        query_name = "3c"
      else:
        query_name = "3b"
      # Determine whether this is the wrap-up part of query 3.
      if len(stage_ids) <= 2:
        use_previous_job_id = True
    elif "url_counts_partial" in job_description:
      query_name = "4"
      if "FROM url_counts_partial" in properties["spark.job.description"]:
        # Group this query with the previous one, which was also for query 4.
        use_previous_job_id = True
    else:
      # This isn't a query we care about (might be a caching query, for example).
      self.dropped_job_ids.add(spark_job_id)
      return ""

    if self.skip_first_query and query_name not in self.queries_parsed:
      self.queries_parsed.add(query_name)
      self.dropped_job_ids.add(spark_job_id)
      return ""

    if use_previous_job_id:
      return self.prev_job_id
    elif query_name:
      job_id = "%s_%s" % (query_name, spark_job_id)
      self.prev_job_id = job_id
      return job_id
      
    assert(False)

