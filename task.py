import numpy

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
    self.executor = items_dict["HOST"]
    self.executor_run_time = int(items_dict["EXECUTOR_RUN_TIME"])
    self.executor_deserialize_time = int(items_dict["EXECUTOR_DESERIALIZE_TIME"])
    self.scheduler_delay = (self.finish_time - self.executor_run_time -
      self.executor_deserialize_time - self.start_time)
    self.gc_time = int(items_dict["GC_TIME"])
    self.executor_id = int(items_dict["EXECUTOR_ID"])

    # Estimate serialization and deserialization time based on samples.
    # TODO: Report an estimated error here based on variation in samples?
    self.estimated_serialization_millis = 0
    if "SERIALIZATED_ITEMS" in items_dict:
      serialized_items = int(items_dict["SERIALIZATED_ITEMS"])
      # Samples are times in nanoseconds.
      serialized_samples = [int(sample) for sample in items_dict["SERIALIZED_SAMPLES"].split(",")]
      print "Serialized %s items, sampled %s" % (serialized_items, len(serialized_samples))
      self.estimated_serialization_millis = serialized_items * numpy.mean(serialized_samples[0::10]) / 1e6

    self.estimated_deserialization_millis = 0
    if "DESERIALIZED_ITEMS" in items_dict:
      deserialized_items = int(items_dict["DESERIALIZED_ITEMS"])
      deserialized_samples = [
        int(sample) for sample in items_dict["DESERIALIZATION_TIME_NANOS"].split(",")]
      print "Deserialized %s items, sampled %s" % (deserialized_items, len(deserialized_samples))
      self.estimated_deserialization_millis = (
        deserialized_items * numpy.median(deserialized_samples[0::1]) / 1e6)

    # Utilization metrics.
    # Map of device name to (utilization, read throughput, write throughout).
    self.disk_utilization = {}
    utilization_str = items_dict["DISK_UTILIZATION"]
    for block_utilization_str in utilization_str.split(";"):
      if block_utilization_str:
        device_name, numbers = block_utilization_str.split(":")
        self.disk_utilization[device_name] = [float(x) for x in numbers.split(",")]

    cpu_utilization_numbers = [
      float(x.split(":")[1]) for x in items_dict["CPU_UTILIZATION"].split(",")]
    # Record the total CPU utilization as the total system CPU use + total user CPU use.
    self.process_cpu_utilization = cpu_utilization_numbers[0] + cpu_utilization_numbers[1]
    self.total_cpu_utilization = cpu_utilization_numbers[2] + cpu_utilization_numbers[3]

    # Should be set to true if this task is a straggler and we know the cause of the
    # straggler behavior.
    self.straggler_behavior_explained = False

    self.shuffle_write_time = 0
    self.shuffle_mb_written = 0
    SHUFFLE_WRITE_TIME_KEY = "SHUFFLE_WRITE_TIME"
    if SHUFFLE_WRITE_TIME_KEY in items_dict:
      # Convert to milliseconds (from nanoseconds).
      self.shuffle_write_time = int(items_dict[SHUFFLE_WRITE_TIME_KEY]) / 1.0e6
      self.shuffle_mb_written = int(items_dict["SHUFFLE_BYTES_WRITTEN"]) / 1048576.

    INPUT_METHOD_KEY = "READ_METHOD"
    self.input_read_time = 0
    self.input_read_method = "unknown"
    self.input_mb = 0
    if INPUT_METHOD_KEY in items_dict:
      self.input_read_time = int(items_dict["READ_TIME_NANOS"]) / 1.0e6
      self.input_read_method = items_dict["READ_METHOD"]
      self.input_mb = float(items_dict["INPUT_BYTES"]) / 1048576.

    self.output_write_time = int(items_dict["OUTPUT_WRITE_BLOCKED_NANOS"]) / 1.0e6
    self.output_mb = 0
    if "OUTPUT_BYTES" in items_dict:
      self.output_mb = int(items_dict["OUTPUT_BYTES"]) / 1048576.

    self.has_fetch = True
    # False if the task was a map task that did not run locally with its input data.
    self.data_local = True
    if line.find("FETCH") < 0:
      if "LOCALITY" in items_dict and items_dict["LOCALITY"] != "NODE_LOCAL":
        self.data_local = False
      self.has_fetch = False
      return
      
    self.shuffle_finish_time = int(items_dict["SHUFFLE_FINISH_TIME"])
    self.fetch_wait = int(items_dict["REMOTE_FETCH_WAIT_TIME"])
    self.local_blocks_read = int(items_dict["BLOCK_FETCHED_LOCAL"])
    self.remote_blocks_read = int(items_dict["BLOCK_FETCHED_REMOTE"])
    self.remote_mb_read = int(items_dict["REMOTE_BYTES_READ"]) / 1048576.
    self.local_mb_read = int(items_dict["LOCAL_READ_BYTES"]) / 1048576.
    # The local read time is not included in the fetch wait time: the task blocks
    # on reading data locally in the BlockFetcherIterator.initialize() method.
    self.local_read_time = int(items_dict["LOCAL_READ_TIME"])
    # TODO: This is not currently accurate due to page mapping.
    self.remote_disk_read_time = 0
      #int(items_dict["REMOTE_DISK_READ_TIME"])
    self.total_time_fetching = int(items_dict["REMOTE_FETCH_TIME"])

  def input_size_mb(self):
    if self.has_fetch:
      return self.remote_mb_read + self.local_mb_read
    else:
      return self.input_mb

  def compute_time_without_gc(self):
     """ Returns the time this task spent computing.
     
     Assumes shuffle writes don't get pipelined with task execution (TODO: verify this).
     Does not include GC time.
     """
     compute_time = (self.runtime() - self.scheduler_delay - self.gc_time -
       self.shuffle_write_time - self.input_read_time - self.output_write_time)
     if self.has_fetch:
       # Subtract off of the time to read local data (which typically comes from disk) because
       # this read happens before any of the computation starts.
       compute_time = compute_time - self.fetch_wait - self.local_read_time
     return compute_time

  def compute_time(self):
    """ Returns the time this task spent computing (potentially including GC time).

    The reason we don't subtract out GC time here is that garbage collection may happen
    during fetch wait.
    """
    return self.compute_time_without_gc() + self.gc_time

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
    compute_wait_time = self.finish_time - self.start_time - self.shuffle_write_time - self.scheduler_delay - self.gc_time - self.input_read_time
    if self.has_fetch:
      #shuffle_time = self.shuffle_finish_time - self.start_time
      #compute_wait_time = compute_wait_time - shuffle_time
      compute_wait_time = compute_wait_time - self.fetch_wait
    return self.runtime() - compute_wait_time

  def runtime_no_disk(self):
    """ Returns a lower bound on what the runtime would have been without disk IO.

    Includes shuffle read time, which is partially spent using the network and partially spent
    using disk.
    """
    disk_time = self.output_write_time + self.shuffle_write_time + self.input_read_time
    if self.has_fetch:
      disk_time += self.local_read_time + self.fetch_wait
    return self.runtime() - disk_time

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
      desc = (("Start time: %s, local read time: %s, shuffle finish: %s, " +
            "fetch wait: %s, compute time: %s, gc time: %s, shuffle write time: %s, finish: %s, " +
            "shuffle bytes: %s, input bytes: %s") %
             (self.start_time, self.local_read_time,
              self.shuffle_finish_time - base, self.fetch_wait, self.compute_time(), self.gc_time,
              self.shuffle_write_time, self.finish_time - base,
              self.local_mb_read + self.remote_mb_read, self.input_mb)) 
    else:
      desc = ("Start time: %s, finish: %s, scheduler delay: %s, input read time: %s, gc time: %s, shuffle write time: %s" %
        (self.start_time, self.finish_time, self.scheduler_delay, self.input_read_time, self.gc_time, self.shuffle_write_time))
    return desc

  def log_verbose(self):
    self.logger.debug(str(self))

  def runtime(self):
    return self.finish_time - self.start_time

  def runtime_no_input(self):
    new_finish_time = self.finish_time - self.input_read_time
    return new_finish_time - self.start_time

  def runtime_no_output(self):
    new_finish_time = self.finish_time - self.output_write_time
    return new_finish_time - self.start_time

  def runtime_no_shuffle_write(self):
    return self.finish_time - self.shuffle_write_time - self.start_time

  def runtime_no_shuffle_read(self):
    if self.has_fetch:
      return self.finish_time - self.fetch_wait - self.local_read_time - self.start_time
    else:
      return self.runtime()

  def runtime_no_output(self):
    new_finish_time = self.finish_time - self.output_write_time
    return new_finish_time - self.start_time

  def runtime_no_input_or_output(self):
    new_finish_time = self.finish_time - self.input_read_time - self.output_write_time
    return new_finish_time - self.start_time

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
    network_time = self.shuffle_finish_time - self.start_time
    faster_network_time = relative_fetch_time * network_time
    # During the shuffle, if we weren't waiting for the network, then we were computing.
    compute_time = network_time - self.fetch_wait
    new_fetch_wait = max(0, faster_network_time - compute_time)
    return self.finish_time - self.fetch_wait + new_fetch_wait

  def runtime_faster_fetch(self, relative_fetch_time):
    return self.finish_time_faster_fetch(relative_fetch_time) - self.start_time

