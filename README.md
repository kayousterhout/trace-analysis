# Understanding Spark Performance

This repository contains scripts to understand the performance of jobs run with [Apache Spark](https://spark.apache.org/).

## Configuring Spark to log performance data

In order to use these tools, you'll first need to configure Spark to log performance data while jobs are running
by setting the Spark configuration parameter `spark.eventLog.enabled` to `true`.  This configuration parameter
causes the Spark master to write a log with information about each completed task to a file on the master. The master
already tracks this information (much of it is displayed in Spark's web UI); setting this configuration option
just causes the master to output all of the data for later consumption.  By default, the event log is written to
a series of files in the folder `/tmp/spark-events/` on the machine where the Spark master runs.
Spark creates a folder within that directory for each application, and logs are stored in a file
named `EVENT_LOG_1` within the application's folder. You can change the parameter
`spark.eventLog.dir` to write the event log elsewhere (e.g., to HDFS).  See the
[Spark configuration documentation](http://spark.apache.org/docs/1.2.1/configuration.html) for more
information about configuring logging.

These scripts are written to work with data output by Spark version 1.2.1 or later.

## Analyzing performance data

After you have collected an event log file with JSON data about the job(s) you'd like to understand, run
the `parse_logs.py` script to generate a visualization of the jobs' performance:

    python parse_logs.py EVENT_LOG_1 --waterfall-only

The `--waterfall-only` flag tells the script to just generate the visualization, and skip more
complex performance analysis. To see all available options, use the flag `--help`.

For each job in the `EVENT_LOG_1` file, the Python script will output a gnuplot file that, when
plotted, will generate a waterfall depicting how time was spent by each of the tasks in the job.
The plot files are named `[INPUT_FILENAME]_[JOB_ID]_waterfall.gp`. To plot the waterfall for job 0, for
example:

    gnuplot EVENT_LOG_1_0_waterfall.gp

will create a file `EVENT_LOG_1_0_waterfall.pdf.  The waterfall plots each task as a horizontal
line.  The horizontal line is colored by how tasks spend time. Tics on the y-axis delineate
different stages of tasks.

Here's an example waterfall:

![Waterfall example](sample_waterfall.jpg)

This waterfall shows the runtime of a job that has 4 stages. The first stage has 2037 tasks, the second
stage has 100 tasks, and the final two stages each have 200 tasks. One thing that stands out for
this job is that tasks in the second stage are spending a long time writing output data to disk
(shown in teal). In this case, this is because the job was running on top of the ext3 file system,
which performs poorly when writing many small files; once we upgraded to a different file system,
the job completed much more quickly and most of the teal-colored time spent writing shuffle output
data disappeared.

## Missing data

Parts of the visualization are currently inaccurate due to incomplete parts of Spark's logging.
In particular, the HDFS read time and output write time (when writing to HDFS) are only accurate
if you are running a special version of Spark and HDFS. Contact Kay Ousterhout if you are interested
in doing this; otherwise, just be aware that part of the pink compute time may be spent read from
or writing to HDFS.

Another problem is that the shuffle write time is currently incorrect (it doesn't include much of
the time spent writing shuffle output) for many versions of Spark. [This Spark JIRA search](https://issues.apache.org/jira/browse/SPARK-3570?jql=project%20%3D%20SPARK%20AND%20text%20~%20%22shuffle%20write%20time%22%20AND%20reporter%20in%20(kayousterhout))
tracks the various issues with the shuffle write time.
This will result in the shuffle write time showing up as compute time.

## FAQ

####I'm getting an error that says "'AttributeError: 'module' object has no attribute 'percentile'"

If you get an error that ends with:

    median_runtime = numpy.percentile(runtimes, 50)
    AttributeError: 'module' object has no attribute 'percentile'

you need to upgrade your version of `numpy` to at least 1.5.

####Parts of my plot are outside of the plot area, and/or some tasks seem to be overlapping others.

This typically happens when you try to plot multiple gnuplot files with one command, e.g.,
with a command like:

    gnuplot *.gp

Gnuplot will put all of the data into a single plot, rather than in separate PDFs.  Try plotting
each gnuplot file separately.

#### How can I figure out which parts of the code each stage executes?

While a common cause of confusion in Spark, this question is unfortunately not answered by this
tool.  Right now, the event logs don't include this information.  There have been murmurs about
adding more detail about this to the Spark UI, but as far as I know, this hasn't been done yet.

#### The scheduler delay (yellow) seems to take a large amount of time. What might the cause be?

At a very high level, usually the best way to reduce scheduler delay is to consolidate jobs into
fewer tasks.

The scheduler delay is essentially message propagation delay to (1) send a message from the
scheduler to an executor to launch a task and (2) to send a message from the executor back to the
echeduler stating that the task has completed.  This can be high when the task is large or the task
result is large, because then it takes longer for the scheduler to ship the task to the executor,
and vice versa for the result. To diagnose this problem, take a look at the Spark UI (or directly
look at the JSON in your event log) to look at the result size of each task, to see if this is
large.

Another reason the scheduler delay can be high is if the scheduler is launching a large number of
tasks over a short period.  In this case, the task completed messages get queued at the scheduler
and can't be processed immediately, which also increases scheduler delay.  When I've benchmarked
the Spark scheduler in the past, I've found it can handle about 1.5K tasks / second (see section 7.6
in [this paper](http://delivery.acm.org/10.1145/2530000/2522716/p69-ousterhout.pdf). This was for
a now-antiquated version of Spark, but in theory this shouldn't have changed much, because the Spark
performance regression tests run before each release have a test that measures this.

One last reason we've sometimes seen in the AMPLab is that it can take a while for the executor to
actually launch the task (which involves getting a thread -- possibly a new one -- from a thread
pool for the task to run in).  This is currently included in scheduler delay.  A few months ago,
I proposed adding metrics about this to the Spark UI, but it was deemed too confusing and not
useful to a suffuciently broad audience (see
discussion here: https://github.com/apache/spark/pull/2832).  If you want to understand this
metric, you can implement the reverse of 
this commit](https://github.com/kayousterhout/spark-1/commit/531575d381b5e4967d5b2f4385c5135040f98165)
(which is part of the aforementioned pull request) to measure whether this time is the cause of the
long scheduler delay.

