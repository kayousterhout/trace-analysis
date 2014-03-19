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
    for fetch_info_str in items_dict["FETCH_INFOS"].split(";"):
      items_dict = {}
      if len(fetch_info_str) == 0:
        continue
      for item in fetch_info_str.split(","):
        key, value = item.split(":")
        items_dict[key] = int(value)
      fetch_start = items_dict["Start"]
      if self.first_fetch_start == -1:
        self.first_fetch_start = fetch_start
      else:
        self.first_fetch_start = min(fetch_start, self.first_fetch_start)
      fetch_time = items_dict["FetchProcessingEnd"] - fetch_start
      self.total_time_fetching += fetch_time
      disk_read_time = items_dict["DiskReadTime"]
      if disk_read_time > fetch_time:
        print "WARNING: Disk read time (%s) > fetch time (%s)" % (disk_read_time, fetch_time)
      self.total_disk_read_time += items_dict["DiskReadTime"]

  def log_verbose(self):
    if self.has_fetch:
      base = self.start_time
      # Print times relative to the start time so that they're easier to read.
      desc = (("Start time: %s, local read time: %s, first fetch start: %s, shuffle finish: %s, " +
            "finish: %s, fraction disk time: %s") %
             (self.start_time, self.local_read_time, self.first_fetch_start - base,
              self.shuffle_finish_time - base, self.finish_time - base, self.fraction_time_disk())) 
    else:
      desc = "Start time: %s, finish: %s" % (self.start_time, self.finish_time)
    self.logger.debug(desc)

  def runtime(self):
    return self.finish_time - self.start_time

  def fraction_time_disk(self):
    """ Should only be called for tasks with a fetch phase. """
    return self.total_disk_read_time * 1.0 / self.total_time_fetching

  def finish_time_no_disk_for_shuffle(self):
    """ Returns the task finish time if the shuffle data hadn't been read from disk.

    This method relies on the finish_time_faster_fetch method and is thus also approximate. """
    if not self.has_fetch:
      return self.finish_time

    # Consider the fraction time spent reading from disk to be the network speedup,
    # and compute the resulting finish time.
    fraction_time_disk = self.fraction_time_disk()
    finish_faster_fetch = self.finish_time_faster_fetch(1 - fraction_time_disk)

    # Also subtract the entire local fetch time, which we'll assume was all spent reading
    # data from disk.
    return finish_faster_fetch - self.local_read_time

  def runtime_no_disk_for_shuffle(self):
    return self.finish_time_no_disk_for_shuffle() - self.start_time

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

