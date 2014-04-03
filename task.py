import logging

class Task:
  def __init__(self, line):
    self.logger = logging.getLogger("Task")

    items = line.split(" ")
    items_dict = {}
    for pair in items:
      if pair.find("=") == -1:
        continue
      key, value = pair.split("=")
      items_dict[key] = value

    self.start_time = int(items_dict["START_TIME"])
    self.finish_time = int(items_dict["FINISH_TIME"])
    self.executor_run_time = int(items_dict["EXECUTOR_RUN_TIME"])
    self.scheduler_delay = self.finish_time - self.executor_run_time - self.start_time
    self.gc_time = int(items_dict["GC_TIME"])
    self.executor_deserialize_time = int(items_dict["EXECUTOR_DESERIALIZE_TIME"])

    self.shuffle_write_time = 0
    SHUFFLE_WRITE_TIME_KEY = "SHUFFLE_WRITE_TIME"
    if SHUFFLE_WRITE_TIME_KEY in items_dict:
      # Convert to milliseconds (from nanoseconds).
      self.shuffle_write_time = int(items_dict[SHUFFLE_WRITE_TIME_KEY]) / 1.0e6

    self.has_fetch = True
    if line.find("FETCH") < 0:
      self.has_fetch = False
      return
      
    self.shuffle_finish_time = int(items_dict["SHUFFLE_FINISH_TIME"])
    self.fetch_wait = int(items_dict["REMOTE_FETCH_WAIT_TIME"])
    self.local_blocks_read = int(items_dict["BLOCK_FETCHED_LOCAL"])
    self.remote_blocks_read = int(items_dict["BLOCK_FETCHED_REMOTE"])
    self.remote_mb_read = int(items_dict["REMOTE_BYTES_READ"]) / 1048576.
    self.local_read_time = int(items_dict["LOCAL_READ_TIME"])
    
    total_bytes_fetched = 0
    self.total_time_fetching = 0
    # Total disk read time across all fetches.
    # TODO: To figure out whether disk is the problem, could do a slightly less rudimentary
    # thing where for each individual fetch, we assume it started at the same time, and look
    # at whether if we subtracted the disk time, we there would be no fetch wait time.
    self.total_disk_read_time = 0
    self.first_fetch_start = -1
    # (bytes, disk read time) for each fetch.
    self.disk_times = []
    # (bytes, network time) for each fetch. The network time is the total fetch time
    # minus the disk read time.
    self.network_times = []
    for fetch_info_str in items_dict["FETCH_INFOS"].split(";"):
      items_dict = {}
      if len(fetch_info_str) == 0:
        continue
      for item in fetch_info_str.split(","):
        key, value = item.split(":")
        items_dict[key] = int(value)
      transferred_bytes = items_dict["Bytes"]
      fetch_start = items_dict["Start"]
      if self.first_fetch_start == -1:
        self.first_fetch_start = fetch_start
      else:
        self.first_fetch_start = min(fetch_start, self.first_fetch_start)
      fetch_time = items_dict["FetchProcessingEnd"] - fetch_start
      self.total_time_fetching += fetch_time
      # Convert to milliseconds (from nanoseconds).
      disk_read_time = items_dict["DiskReadTime"] / 1.0e6
      if disk_read_time > fetch_time:
        print "WARNING: Disk read time (%s) > fetch time (%s)" % (disk_read_time, fetch_time)
      self.total_disk_read_time += disk_read_time
      self.disk_times.append((transferred_bytes, disk_read_time))
      self.network_times.append((transferred_bytes, fetch_time - disk_read_time))

  def network_time(self):
    """ Returns the amount of time this task spent using the network.

    This includes time where computation was parallelized with network.
    """
    if self.has_fetch:
      return self.shuffle_finish_time - self.start_time - self.local_read_time
    return 0

  def compute_time(self):
     """ Returns the time this task spent computing.
     
     Assumes shuffle writes don't get pipelined with task execution (TODO: verify this).
     Does not include GC time.
     """
     compute_time = self.runtime() - self.scheduler_delay - self.gc_time - self.shuffle_write_time
     if self.has_fetch:
       # Subtract off of the time to read local data (which typically comes from disk) because
       # this read happens before any of the computation starts.
       compute_time = compute_time - self.fetch_wait - self.local_read_time
     return compute_time

  def runtime_no_compute(self):
    """ Returns how long the task would have run for had it not done any computation. """
    # Time the task spent reading data over the network or from disk for the shuffle.
    # Computation happens during this time, but if the computation were infinitely fast,
    # this phase wouldn't have sped up because it was ultimately waiting on the network.
    # This is an approximation because tasks don't currently log the amount of time where
    # the network is stopped, waiting for the computation to speed up.
    # We're also approximating because there's some disk writing that happens in parallel
    # via the OS buffer cache.  It's basically impossible for us to account for that so
    # we ignore it.
    # The final reason that this is an approximation is that the shuffle write time could overlap with
    # the shuffle time (if a task is both reading shuffle inputs and writing shuffle outputs).
    # We should be able to fix the logging to correct this issue.
    compute_wait_time = self.finish_time - self.start_time - self.shuffle_write_time
    if self.has_fetch:
      shuffle_time = self.shuffle_finish_time - self.start_time
      compute_wait_time = compute_wait_time - shuffle_time
    return self.runtime() - compute_wait_time

  def disk_time(self):
    """ Returns the time writing shuffle output.

    Ignores disk time taken to read shuffle input as part of a transfer over the network because in
    the traces we've seen so far, it's a very small percentage of the network time.
    """
    if self.has_fetch:
      return self.shuffle_write_time + self.local_read_time
    return self.shuffle_write_time

  def __str__(self):
    if self.has_fetch:
      base = self.start_time
      # Print times relative to the start time so that they're easier to read.
      desc = (("Start time: %s, local read time: %s, first fetch start: %s, shuffle finish: %s, " +
            "fetch wait: %s, compute time: %s, gc time: %s, shuffle write time: %s, finish: %s, " +
            "fraction disk time: %s") %
             (self.start_time, self.local_read_time, self.first_fetch_start - base,
              self.shuffle_finish_time - base, self.fetch_wait, self.gc_time,
              self.compute_time(), self.shuffle_write_time, self.finish_time - base,
              self.fraction_time_disk())) 
    else:
      desc = ("Start time: %s, finish: %s, gc time: %s, shuffle write time: %s" %
        (self.start_time, self.finish_time, self.gc_time, self.shuffle_write_time))
    return desc

  def log_verbose(self):
    self.logger.debug(str(self))

  def runtime(self):
    return self.finish_time - self.start_time

  def fraction_time_disk(self):
    """ Should only be called for tasks with a fetch phase. """
    if self.total_disk_read_time == 0:
      return 0
    return self.total_disk_read_time * 1.0 / self.total_time_fetching

  def finish_time_no_disk_for_shuffle(self):
    """ Returns the task finish time if the shuffle data hadn't been written to or read from disk.

    This method relies on the finish_time_faster_fetch method and is thus also approximate. """
    if not self.has_fetch:
      return self.finish_time - self.shuffle_write_time

    # Consider the fraction time spent reading from disk to be the network speedup,
    # and compute the resulting finish time.
    fraction_time_disk = self.fraction_time_disk()
    self.logger.debug("Fraction time task used for disk: %s" % fraction_time_disk)
    finish_faster_fetch = self.finish_time_faster_fetch(1 - fraction_time_disk)

    # Also subtract the entire local fetch time, which we'll assume was all spent reading
    # data from disk.
    return finish_faster_fetch - self.local_read_time - self.shuffle_write_time

  def runtime_no_disk_for_shuffle(self):
    new_runtime = self.finish_time_no_disk_for_shuffle() - self.start_time
    self.logger.debug(new_runtime)
    speedup = new_runtime * 1.0 / self.runtime()
    self.logger.debug("Speedup from eliminating disk: %s" % speedup)
    return new_runtime

  def finish_time_faster_fetch(self, relative_fetch_time):
    """ Returns the finish time of the job if the fetch had completed faster.

    This method is approximate unless relative_fetch_time is 0 (the fetch completed inifinitely
    fast) or 1 (the fetch completed in the same time as in the original job).
    """
    if not self.has_fetch:
      return self.finish_time

    self.log_verbose()

    # If the fetch wait time is > 0, assume the network was in use the entire shuffle time
    # (i.e., we were never blocked waiting on the computation).
    # The problem with doing the more accurate version where we simulate the entire fetch
    # process is that we need to exactly replicate the algorithm used by BlockFetcherIterator,
    # which is quite complex.
    network_time = self.shuffle_finish_time - self.first_fetch_start
    faster_network_time = relative_fetch_time * network_time
    # During the shuffle, if we weren't waiting for the network, then we were computing.
    compute_time = network_time - self.fetch_wait
    new_fetch_wait = max(0, faster_network_time - compute_time)
    return self.finish_time - self.fetch_wait + new_fetch_wait

  def runtime_faster_fetch(self, relative_fetch_time):
    return self.finish_time_faster_fetch(relative_fetch_time) - self.start_time

