import collections
import math
import os
import sys

# This is totally not portable -- just the observed sector size on m2.4xlarge instances.
SECTOR_SIZE_BYTES = 512

MIN_TIME = 0
#112000

def write_template(f):
  template_file = open("running_tasks_template.gp")
  for line in template_file:
    f.write(line)

def write_output_data(filename, data, earliest_time):
  f = open(filename, "w")
  for (time, x) in data:
    f.write("%s\t%s\n" % (time - earliest_time, x))
  f.close()

""" N should be sorted before calling this function. """
def get_percentile(N, percent, key=lambda x:x):
    if not N:
        return 0
    k = (len(N) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0 + d1

def get_delta(items, earliest_time, latest_time):
  filtered_items = [x for x in items if x[0] >= earliest_time and x[0] <= latest_time]
  return filtered_items[-1][1] - filtered_items[0][1]

""" Accepts a list of (time, values) tuples and returns the average value during the time
period after earliest_time and before latest_time. """
def get_average(items, earliest_time, latest_time):
  filtered_values = [x[1] for x in items if x[0] >= earliest_time and x[0] <= latest_time]
  return sum(filtered_values) * 1.0 / len(filtered_values)

def write_running_tasks_plot(file_prefix, y2label, output_filename, running_tasks_plot_file,
    running_tasks_filename, experiment_duration):
  # Warning: this graph ends up looking like a comb -- since the time in between task launches is
  # short relative to task runtime.
  running_tasks_plot_file.write(
    "# Must be plotted from the parent directory for links to work correctly\n")
  write_template(running_tasks_plot_file)
  running_tasks_plot_file.write("set xlabel \"Time (ms)\"\n")
  #running_tasks_plot_file.write("set xrange [%s:%s]\n" %
  #  (experiment_duration - 1200000, experiment_duration - 1000000))
  running_tasks_plot_file.write("set xrange [5000:]\n")
  running_tasks_plot_file.write("set y2tics\n")
  running_tasks_plot_file.write("set y2label \"%s\"\n" % y2label)
  running_tasks_plot_file.write("set output \"%s/%s.pdf\"\n\n" % (file_prefix, output_filename))
  running_tasks_plot_file.write(
    ("plot \"%s\" using 1:2 w l ls 1 title \"Running tasks\" axes x1y1,\\\n" %
      running_tasks_filename))

def parse_proc_file(filename):
  print "Parsing ", filename

  # List of tuples, where the first item is the time and the second
  # item is 1 if a task started at that time and -1 if a task
  # finished at that time.
  task_events = []

  # Time, GC tuples.
  gc_rates = []
  gc_totals = []

  # Time, CPU usage tuples using two different measurement strategies.
  # 1 uses the user/sys jiffies divided by the total elapsed jiffies.
  # 2 estimates the utilization based on the user/sys jiffies divided
  # by the jiffies / second on the machine (which is 100).
  user_cpu_usage1 = []
  sys_cpu_usage1 = []
  proctotal_cpu_usage1 = []
  # Time the CPU was active, across all processes.
  total_cpu_usage1 = []
  # Time the cpu was idle and at least one process was waiting for IO.
  iowait_cpu_usage1 = []
  # Time the cpu was idle.
  idle_cpu_usage1 = []

  user_cpu_usage2 = []
  sys_cpu_usage2 = []
  total_cpu_usage2 = []

  # Time, total CPU counter tuples. Used to compute average CPU usage
  # during experiment.
  user_cpu_totals = []
  sys_cpu_totals = [] 

  # Time, IO rate tuples for the process. Misleading because this is the rate at
  # which reads and writes are issued, and doesn't take into account the time to
  # actually flush them to disk.
  rchar = []
  rchar_totals = []
  wchar = []
  wchar_totals = []
  rbytes_totals = []
  wbytes_totals = []
  # Time, IO rate tuples for the whole system (listed separately for each block device),
  # but as read from the disk stats (so reflect what's actually happening at the device-level).
  sectors_read_rate = collections.defaultdict(list)
  sectors_written_rate = collections.defaultdict(list)
  sectors_total_rate = collections.defaultdict(list)
  sectors_read_total = collections.defaultdict(list)
  sectors_written_total = collections.defaultdict(list)

  # Time, network traffic tuples.
  trans_bytes_rate = []
  trans_packets_rate = []
  recv_bytes_rate = []
  recv_packets_rate = []
  trans_bytes_totals = []
  recv_bytes_totals = []
  
  BYTES_PER_MB = 1048576

  task_durations = []

  file_prefix = "%s_parsed" % filename
  if not os.path.exists(file_prefix):
    print "Making directory for results: %s" % file_prefix
    os.mkdir(file_prefix)

  results_file = open(filename)
  for line in results_file:
    if line.find("Task run") != -1:
      items = line.split(" ")
      start_time = int(items[7])
      task_events.append((start_time, 1))
      end_time = int(items[9][:-1])
      task_events.append((end_time, -1))
      task_durations.append(end_time - start_time)
    elif line.find("GC total") != -1:
      items = line.split(" ")
      time = int(items[4])
      gc_rates.append((time, float(items[13])))
      gc_totals.append((time, long(items[7])))
    elif line.find("CPU utilization (relative metric)") != -1:
      items = line.strip("\n").split(" ")
      time = int(items[4])
      user_cpu_usage1.append((time, float(items[10])))
      sys_cpu_usage1.append((time, float(items[12])))
      proctotal_cpu_usage1.append((time, float(items[14])))
      total_cpu_usage1.append((time, float(items[16])))
      iowait_cpu_usage1.append((time, float(items[18])))
      idle_cpu_usage1.append((time, float(items[20])))
      # Sanity check = 1: print (float(items[16]) + float(items[18]) + float(items[20]))
    elif line.find("CPU utilization (jiffie-based)") != -1:
      items = line.split(" ")
      time = int(items[4])
      user_cpu_usage2.append((time, float(items[9])))
      sys_cpu_usage2.append((time, float(items[11])))
      total_cpu_usage2.append((time, float(items[13][:-1])))
    elif line.find("CPU counters") != -1:
      items = line.split(" ")
      time = int(items[4])
      user_cpu_totals.append((time, float(items[8])))
      sys_cpu_totals.append((time, float(items[10])))
    elif line.find("rchar rate") != -1:
      items = line.split(" ")
      time = int(items[4])
      rchar.append((time, float(items[7]) / BYTES_PER_MB))
      wchar.append((time, float(items[10]) / BYTES_PER_MB))
    elif line.find("IO Totals") != -1:
      items = line.strip("\n").split(" ")
      time = int(items[4])
      rchar_totals.append((time, long(items[8])))
      wchar_totals.append((time, long(items[10])))
      rbytes_totals.append((time, long(items[12])))
      wbytes_totals.append((time, long(items[14])))
    elif line.find("sectors") != -1:
      items = line.strip("\n").split(" ")
      time = int(items[4])
      device_name = items[5]
      sectors_read_rate[device_name].append((time, float(items[9])))
      sectors_written_rate[device_name].append((time, float(items[11])))
      sectors_total_rate[device_name].append((time, float(items[9]) + float(items[11])))
      sectors_read_total[device_name].append((time, int(items[14])))
      sectors_written_total[device_name].append((time, int(items[16])))
    elif line.find("trans rates") != -1:
      items = line.strip("\n").split(" ")
      time = int(items[4])
      trans_bytes_rate.append((time, float(items[7]) / BYTES_PER_MB))
      trans_packets_rate.append((time, float(items[9])))
      recv_bytes_rate.append((time, float(items[13]) / BYTES_PER_MB))
      recv_packets_rate.append((time, float(items[15])))
    elif line.find("totals: trans") != -1:
      items = line.strip("\n").split(" ")
      time = int(items[4])
      trans_bytes_totals.append((time, int(items[8])))
      recv_bytes_totals.append((time, int(items[13])))
  
  print "Found %s task events" % len(task_events)
  # Output file with number of running tasks.
  task_events.sort(key = lambda x: x[0])
  running_tasks_filename = "%s/running_tasks" % file_prefix
  running_tasks_file = open(running_tasks_filename, "w")
  running_tasks = 0
  # Find the first task launch after MIN_TIME.
  absolute_min_time = task_events[0][0] + MIN_TIME
  print absolute_min_time
  earliest_time = min([pair[0] for pair in task_events if pair[0] > absolute_min_time])
  print earliest_time
  #earliest_time += 1000000
  latest_time = int(task_events[-1][0])
  for (time, event) in task_events:
    # Plot only the time delta -- makes the graph much easier to read.
    running_tasks_file.write("%s\t%s\n" % (time - earliest_time, running_tasks))
    running_tasks += event
    running_tasks_file.write("%s\t%s\n" % (time - earliest_time, running_tasks))
  running_tasks_file.close()

  # Output CPU usage data.
  user_cpu_filename = "%s/user_cpu" % file_prefix
  write_output_data(user_cpu_filename, user_cpu_usage1, earliest_time)
  sys_cpu_filename = "%s/sys_cpu" % file_prefix
  write_output_data(sys_cpu_filename, sys_cpu_usage1, earliest_time)
  proctotal_cpu_filename = "%s/proctotal_cpu" % file_prefix
  write_output_data(proctotal_cpu_filename, proctotal_cpu_usage1, earliest_time)
  total_cpu_filename = "%s/total_cpu" % file_prefix
  write_output_data(total_cpu_filename, total_cpu_usage1, earliest_time)
  idle_cpu_filename = "%s/idle_cpu" % file_prefix
  write_output_data(idle_cpu_filename, idle_cpu_usage1, earliest_time)
  iowait_cpu_filename = "%s/iowait_cpu" % file_prefix
  write_output_data(iowait_cpu_filename, iowait_cpu_usage1, earliest_time)

  # Output CPU use percentiles in order to make a box/whiskers plot.
  total_cpu_values = [pair[1] for pair in proctotal_cpu_usage1]
  total_cpu_values.sort()
  box_plot_file = open("%s/total_cpu_box" % file_prefix, "w")
  box_plot_file.write("%s\t%s\t%s\t%s\t%s\n" %
    (get_percentile(total_cpu_values, 0.05),
     get_percentile(total_cpu_values, 0.25),
     get_percentile(total_cpu_values, 0.5),
     get_percentile(total_cpu_values, 0.75),
     get_percentile(total_cpu_values, 0.95)))
  box_plot_file.close()
 
  # Output CPU usage data using second metric.
  user_cpu_filename2 = "%s/user_cpu2" % file_prefix
  write_output_data(user_cpu_filename2, user_cpu_usage2, earliest_time)
  sys_cpu_filename2 = "%s/sys_cpu2" % file_prefix
  write_output_data(sys_cpu_filename2, sys_cpu_usage2, earliest_time)
  proctotal_cpu_filename2 = "%s/total_cpu2" % file_prefix
  write_output_data(proctotal_cpu_filename2, total_cpu_usage2, earliest_time)
 
  # Output CPU use percentiles in order to make a box/whiskers plot.
  total_cpu2_values = [pair[1] for pair in total_cpu_usage2]
  total_cpu2_values.sort()
  box_plot_file2 = open("%s/total_cpu2_box" % file_prefix, "w")
  box_plot_file2.write("%s\t%s\t%s\t%s\t%s\n" %
    (get_percentile(total_cpu2_values, 0.05),
     get_percentile(total_cpu2_values, 0.25),
     get_percentile(total_cpu2_values, 0.5),
     get_percentile(total_cpu2_values, 0.75),
     get_percentile(total_cpu2_values, 0.95)))
  box_plot_file2.close()
   
  job_duration = latest_time - earliest_time

  # Output GC data.
  gc_filename = "%s/gc" % file_prefix
  write_output_data(gc_filename, gc_rates, earliest_time)
  total_gc_time = get_delta(gc_totals, earliest_time, latest_time)
  print "Total GC millis:", total_gc_time
  print "Fraction of time doing GC: ", total_gc_time * 1.0 / job_duration

  # Print average CPU usage during time when tasks were running. This assumes that
  # all the measurement intervals were the same.
  print "Average user CPU use: ", get_average(user_cpu_usage1, earliest_time, latest_time)
  print "Average total CPU use: ", get_average(proctotal_cpu_usage1, earliest_time, latest_time)
  print "Average IO-wait CPU use: ", get_average(iowait_cpu_usage1, earliest_time, latest_time)

  print "Average rchar rate: ", get_average(rchar, earliest_time, latest_time)
  print "  rchar delta: ", get_delta(rchar_totals, earliest_time, latest_time) * 1.0 / BYTES_PER_MB
  print ("Average rchar (MB/s, from totals): %s" %
    (get_delta(rchar_totals, earliest_time, latest_time) * 1000.0 / (job_duration * BYTES_PER_MB)))
  print "Average wchar rate: ", get_average(wchar, earliest_time, latest_time)
  print "  wchar delta: ", get_delta(wchar_totals, earliest_time, latest_time) * 1.0 / BYTES_PER_MB
  print ("Average wchar (from totals): %s" %
    (get_delta(wchar_totals, earliest_time, latest_time) * 1000.0 / (job_duration * BYTES_PER_MB)))
  print ("Average rbytes (from totals): %s" %
    (get_delta(rbytes_totals, earliest_time, latest_time) * 1000.0 / (job_duration * BYTES_PER_MB)))
  print ("Average wbytes (from totals): %s" %
    (get_delta(wbytes_totals, earliest_time, latest_time) * 1000.0 / (job_duration * BYTES_PER_MB)))

  # Print averages based on the total numbers.
  user_cpu_delta = get_delta(user_cpu_totals, earliest_time, latest_time)
  sys_cpu_delta = get_delta(sys_cpu_totals, earliest_time, latest_time)
  # Multiply by 10 because there are 100 jiffies / second and normalize by the number of cores.
  avg_user_cpu = 10 * user_cpu_delta / (8.0 * job_duration)
  avg_sys_cpu = 10 * sys_cpu_delta / (8.0 * job_duration)
  print "Average user CPU (based on totals): ", avg_user_cpu
  print "Average sys CPU (based on totals): ", avg_sys_cpu

  # Output job duration ESTIMATE (just last task end - first task start; this is just one worker
  # so not totally accurate) and average task duration.
  print "Job duration ESTIMATE (ms): ", job_duration
  print "Average task duration (ms): ", sum(task_durations) * 1.0 / len(task_durations)
  print "Total tasks:", len(task_durations)

  # Output IO usage data.
  rchar_filename = "%s/rchar" % file_prefix
  write_output_data(rchar_filename, rchar, earliest_time)
  wchar_filename = "%s/wchar" % file_prefix
  write_output_data(wchar_filename, wchar, earliest_time)

  for device_name in sectors_read_rate.keys():
    print "*******", device_name
    sectors_read_filename = "%s/%s_sectors_read" % (file_prefix, device_name)
    write_output_data(sectors_read_filename, sectors_read_rate[device_name], earliest_time)
    sectors_written_filename = "%s/%s_sectors_written" % (file_prefix, device_name)
    write_output_data(sectors_written_filename, sectors_written_rate[device_name], earliest_time)
    sectors_total_filename = "%s/%s_sectors_total" % (file_prefix, device_name)
    write_output_data(sectors_total_filename, sectors_total_rate[device_name], earliest_time)
    sectors_read = get_delta(sectors_read_total[device_name], earliest_time, latest_time)
    # Need to multiple by 1000 to convert from milliseconds to seconds.
    avg_mbps_read = sectors_read * SECTOR_SIZE_BYTES * 1000.0 / (job_duration * BYTES_PER_MB)
    print ("Avg MB/s read for %s: %f" % (device_name, avg_mbps_read))
    sectors_written = get_delta(sectors_written_total[device_name], earliest_time, latest_time)
    avg_mbps_written = sectors_written * SECTOR_SIZE_BYTES * 1000.0 / (job_duration * BYTES_PER_MB)
    print ("Avg MB/s written for %s: %f" % (device_name, avg_mbps_written))
    print "Total MB/s read/written for %s: %f" % (device_name, avg_mbps_read + avg_mbps_written)
    print "%s: %d sectors written; %d sectors read" % (device_name, sectors_written, sectors_read)

  # Output network data.
  trans_bytes_rate_filename = "%s/trans_bytes_rate" % file_prefix
  write_output_data(trans_bytes_rate_filename, trans_bytes_rate, earliest_time)
  trans_packets_rate_filename = "%s/trans_packets_rate" % file_prefix
  write_output_data(trans_packets_rate_filename, trans_packets_rate, earliest_time)
  recv_bytes_rate_filename = "%s/recv_bytes_rate" % file_prefix
  write_output_data(recv_bytes_rate_filename, recv_bytes_rate, earliest_time)
  recv_packets_rate_filename = "%s/recv_packets_rate" % file_prefix
  write_output_data(recv_packets_rate_filename, recv_packets_rate, earliest_time)

  trans_mbytes = get_delta(trans_bytes_totals, earliest_time, latest_time) * 1.0 / BYTES_PER_MB
  print "Avg MB/s transmitted:", trans_mbytes * 1000.0 / job_duration, "total:", trans_mbytes
  recv_mbytes = get_delta(recv_bytes_totals, earliest_time, latest_time) * 1.0 / BYTES_PER_MB
  print "Avg MB/s received:", recv_mbytes * 1000.0 / job_duration, "total:", recv_mbytes

  # Output one file with running tasks, CPU, and IO usage.
  running_tasks_plot_file = open("%s/running_tasks_cpu.gp" % file_prefix, "w")
  experiment_duration = latest_time - earliest_time
  write_running_tasks_plot(file_prefix, "Percent", "running_tasks_cpu", running_tasks_plot_file,
    running_tasks_filename, experiment_duration)
  running_tasks_plot_file.write(
    ("\"%s\" using 1:2 w l ls 4 title \"GC\" axes x1y2,\\\n" % gc_filename))
  running_tasks_plot_file.write(
    ("\"%s\" using 1:2 w l ls 2 title \"User CPU\" axes x1y2,\\\n" %
      user_cpu_filename))
  running_tasks_plot_file.write(
    ("\"%s\" using 1:2 w l ls 3 title \"System CPU\" axes x1y2,\\\n" %
      sys_cpu_filename))
 # running_tasks_plot_file.write(
 #   ("\"%s\" using 1:2 w l ls 4 title \"Total Process CPU\" axes x1y2,\\\n" %
 #     proctotal_cpu_filename))
  running_tasks_plot_file.write(
    ("\"%s\" using 1:2 w l ls 5 title \"Total CPU\" axes x1y2,\\\n" %
      total_cpu_filename))
  running_tasks_plot_file.write(
    ("\"%s\" using 1:2 w l ls 6 title \"Idle CPU\" axes x1y2,\\\n" %
      idle_cpu_filename))
  running_tasks_plot_file.write(
    ("\"%s\" using 1:2 w l ls 7 title \"IO Wait CPU\" axes x1y2" %
      iowait_cpu_filename))

  # Output two network files: one with bytes and another with packets.
  network_plot_file = open("%s/running_tasks_network_bytes.gp" % file_prefix, "w")
  write_running_tasks_plot(file_prefix, "MB", "running_tasks_network_bytes",
    network_plot_file, running_tasks_filename, experiment_duration)
  network_plot_file.write(
    ("\"%s\" using 1:2 w l ls 2 title \"Transmitted bytes\" axes x1y2,\\\n" %
      trans_bytes_rate_filename))
  network_plot_file.write(
    "\"%s\" using 1:2 w l ls 3 title \"Received bytes\" axes x1y2\n" % recv_bytes_rate_filename)

  io_plot_file = open("%s/running_tasks_io.gp" % file_prefix, "w")
  write_running_tasks_plot(file_prefix, "MB/s", "running_tasks_io", io_plot_file,
    running_tasks_filename, experiment_duration)
  next_line_style = 2
  for device_name in sectors_read_rate.keys():
    if next_line_style > 2:
      io_plot_file.write(",\\\n")
    sectors_read_filename = "%s/%s_sectors_read" % (file_prefix, device_name)
    io_plot_file.write(
      "\"%s\" using 1:($2/%s) w l ls %d title \"%s read\" axes x1y2,\\\n" %
      (sectors_read_filename, BYTES_PER_MB / 512, next_line_style, device_name))
    next_line_style += 1
    sectors_written_filename = "%s/%s_sectors_written" % (file_prefix, device_name)
    io_plot_file.write(
      "\"%s\" using 1:($2/%s) w l ls %d title \"%s written\" axes x1y2" %
      (sectors_written_filename, BYTES_PER_MB / 512, next_line_style, device_name))
    next_line_style += 1
  io_plot_file.write("\n")

  network_plot_file = open("%s/running_tasks_network_packets.gp" % file_prefix, "w")
  write_running_tasks_plot(file_prefix, "packets", "running_tasks_network_packets",
    network_plot_file, running_tasks_filename, experiment_duration)
  network_plot_file.write(
    ("\"%s\" using 1:2 w l ls 2 title \"Transmitted packets\" axes x1y2,\\\n" %
      trans_packets_rate_filename))
  network_plot_file.write(
    "\"%s\" using 1:2 w l ls 3 title \"Received packets\" axes x1y2\n" % recv_packets_rate_filename)

def main(argv):
  file_prefix = argv[0].strip("/")
  print "Parsing logs in directory %s" % file_prefix

  for filename in os.listdir(file_prefix):
    if filename.endswith("proc_log"):
      parse_proc_file(os.path.join(file_prefix, filename))

if __name__ == "__main__":
  main(sys.argv[1:])
