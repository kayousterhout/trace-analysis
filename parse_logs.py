import collections
import json
import logging
import numpy
from optparse import OptionParser
import sys

from job import Job

def get_json(line): 
  # Need to first strip the trailing newline, and then escape newlines (which can appear
  # in the middle of some of the JSON) so that JSON library doesn't barf.
  return json.loads(line.strip("\n").replace("\n", "\\n"))

class Analyzer:
  def __init__(self, filename):
    self.filename = filename
    self.jobs = collections.defaultdict(Job)
    # For each stage, jobs that rely on the stage.
    self.jobs_for_stage = {}

    f = open(filename, "r")
    test_line = f.readline()
    try:
      get_json(test_line)
      is_json = True
      print "Parsing file %s as JSON" % filename
    except:
      is_json = False
      print "Parsing file %s as JobLogger output" % filename
    f.seek(0)

    for line in f:
      if is_json:
        json_data = get_json(line)
        event_type = json_data["Event"]
        if event_type == "SparkListenerJobStart":
          job_id = json_data["Job ID"]
          stage_ids = [stage_info["Stage ID"] for stage_info in json_data["Stage Infos"]]
          print "Stage ids: %s" % stage_ids
          for stage_id in stage_ids:
            if stage_id not in self.jobs_for_stage:
              self.jobs_for_stage[stage_id] = [job_id]
            else:
              self.jobs_for_stage[stage_id].append(job_id)
        elif event_type == "SparkListenerTaskEnd":
          stage_id = json_data["Stage ID"]
          # Add the event to all of the jobs that depend on the stage.
          for job_id in self.jobs_for_stage[stage_id]:
            self.jobs[job_id].add_event(json_data, True)
      else:
        # The file will only contain information for one job.
        self.jobs[0].add_event(line, False)

    print "Finished reading input data:"
    for job_id, job in self.jobs.iteritems():
      job.initialize_job()
      print "Job", job_id, " has stages: ", job.stages.keys()

  def output_all_waterfalls(self):
    for job_id, job in self.jobs.iteritems():
      filename = "%s_%s" % (self.filename, job_id)
      job.write_waterfall(filename)

  def output_all_job_info(self, agg_results_filename):
    for job_id, job in self.jobs.iteritems():
      filename = "%s_%s" % (self.filename, job_id)
      self.__output_job_info(job, filename, agg_results_filename)

  def __output_job_info(self, job, filename, agg_results_filename):
    job.print_stage_info()

    job.write_task_write_times_scatter(filename)

    #job.make_cdfs_for_performance_model(filename)

    job.write_waterfall(filename)

    fraction_time_scheduler_delay = job.fraction_time_scheduler_delay()
    print ("\nFraction time scheduler delay: %s" % fraction_time_scheduler_delay)
    fraction_time_waiting_on_shuffle_read = job.fraction_time_waiting_on_shuffle_read()
    print "\nFraction time waiting on shuffle read: %s" % fraction_time_waiting_on_shuffle_read
    no_input_disk_speedup = job.no_input_disk_speedup()[0]
    print "Speedup from eliminating disk for input: %s" % no_input_disk_speedup
    no_output_disk_speedup = job.no_output_disk_speedup()[0]
    print "Speedup from elimnating disk for output: %s" % no_output_disk_speedup
    no_shuffle_write_disk_speedup = job.no_shuffle_write_disk_speedup()[0]
    print "Speedup from eliminating disk for shuffle write: %s" % no_shuffle_write_disk_speedup
    no_shuffle_read_disk_speedup, throw_away, no_shuffle_read_runtime = \
      job.no_shuffle_read_disk_speedup()
    print "Speedup from eliminating shuffle read: %s" % no_shuffle_read_disk_speedup
    no_disk_speedup, simulated_original_runtime, no_disk_runtime = job.no_disk_speedup()
    print "No disk speedup: %s" % no_disk_speedup
    fraction_time_using_disk = job.fraction_time_using_disk()
    no_network_speedup, not_used, no_network_runtime = job.no_network_speedup()
    print "No network speedup: %s" % no_network_speedup
    print("\nFraction of time spent writing/reading shuffle data to/from disk: %s" %
      fraction_time_using_disk)
    print("\nFraction of time spent garbage collecting: %s" %
      job.fraction_time_gc())
    no_compute_speedup = job.no_compute_speedup()[0]
    print "\nSpeedup from eliminating compute: %s" % no_compute_speedup
    fraction_time_waiting_on_compute = job.fraction_time_waiting_on_compute()
    print "\nFraction of time waiting on compute: %s" % fraction_time_waiting_on_compute
    fraction_time_computing = job.fraction_time_computing()
    print "\nFraction of time computing: %s" % fraction_time_computing
    
    replace_all_tasks_with_average_speedup = job.replace_all_tasks_with_average_speedup(filename)
    no_stragglers_replace_with_median_speedup = job.replace_all_tasks_with_median_speedup()
    no_stragglers_replace_95_with_median_speedup = \
      job.replace_stragglers_with_median_speedup(lambda runtimes: numpy.percentile(runtimes, 95))
    no_stragglers_replace_ganesh_with_median_speedup = \
      job.replace_stragglers_with_median_speedup(
        lambda runtimes: 1.5 * numpy.percentile(runtimes, 50))
    no_stragglers_perfect_parallelism = \
      job.no_stragglers_perfect_parallelism_speedup()
    median_progress_rate_speedup = job.median_progress_rate_speedup(filename)
    print (("\nSpeedup from eliminating stragglers: %s (perfect parallelism) %s (use average) "
      "%s (use median) %s (1.5=>median) %s (95%%ile=>med) %s (median progress rate)") %
      (no_stragglers_perfect_parallelism, replace_all_tasks_with_average_speedup,
       no_stragglers_replace_with_median_speedup, no_stragglers_replace_ganesh_with_median_speedup,
       no_stragglers_replace_95_with_median_speedup, median_progress_rate_speedup))

    simulated_versus_actual = job.simulated_runtime_over_actual(filename)
    print "\n Simulated versus actual runtime: ", simulated_versus_actual

    if agg_results_filename != None:
      print "Adding results to %s" % agg_results_filename
      f = open(agg_results_filename, "a")
      data = [
        filename.split("/")[1].split("_")[0],
        # 1
        fraction_time_waiting_on_shuffle_read,
        # 2 3
        no_disk_speedup, fraction_time_using_disk,
        no_compute_speedup, -1, fraction_time_computing,
        # 7 8
        replace_all_tasks_with_average_speedup, no_stragglers_replace_with_median_speedup,
        # 9 10
        no_stragglers_replace_95_with_median_speedup, no_stragglers_perfect_parallelism,
        # 11 12
        simulated_versus_actual, median_progress_rate_speedup,
        no_input_disk_speedup, no_output_disk_speedup,
        # 15 16
        no_shuffle_write_disk_speedup, no_shuffle_read_disk_speedup,
        # 17 18 19
        job.original_runtime(), simulated_original_runtime, no_disk_runtime, 
        no_shuffle_read_runtime,
        # 21 22 23
        job.no_gc_speedup()[0], no_network_speedup, no_network_runtime]
      job.write_data_to_file(data, f)
      f.close()
      job.write_straggler_info(filename, agg_results_filename)
      job.write_stage_info(filename, agg_results_filename)

      job.write_hdfs_stage_normalized_runtimes(agg_results_filename)

def main(argv):
  parser = OptionParser(usage="parse_logs.py [options] <log filename>")
  parser.add_option(
      "-d", "--debug", action="store_true", default=False,
      help="Enable additional debug logging")
  parser.add_option(
      "-a", "--agg-results-filename",
      help="File to which to output aggregate statistics")
  parser.add_option(
      "-w", "--waterfall-only", action="store_true", default=False,
      help="Output only the visualization for each job (not other stats)")
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)
 
  if opts.debug:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)
  filename = args[0]
  if filename is None:
    parser.print_help()
    sys.exit(1)
  agg_results_filename = opts.agg_results_filename

  analyzer = Analyzer(filename)

  if opts.waterfall_only:
    analyzer.output_all_waterfalls()
  else:
    analyzer.output_all_job_info(agg_results_filename)

if __name__ == "__main__":
  main(sys.argv[1:])
