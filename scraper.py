'''
Created on Mar 27, 2014

@author: ryan
'''
from re import compile
import json
from itertools import chain, islice, izip, imap
from collections import defaultdict
from matplotlib import pyplot
from sys import stderr
from parse_logs import AbstractTask, make_waterfall
from numpy import allclose as eq
from collections import OrderedDict
import time
import calendar
import scheduler
import os
from numpy import percentile

median = lambda lst: percentile(lst, 50)

TIMER_REGEX = compile('magic=ryanlog.*?data=({.*})')
MS_REGEX = compile(r'(2014.*),([0-9]{3,3})')

def log_time_to_nanos(string, ms_string=0):
    FORMAT = "%Y-%m-%d %H:%M:%S"
    t = time.strptime(string, FORMAT)
    return calendar.timegm(t) * 10 ** 9 + int(ms_string) * 10 ** 6

def flatten(listOfLists):
    "Flatten one level of nesting"
    return list(chain.from_iterable(listOfLists))

def flatMap(f, lst):
    return flatten(map(f, lst))

def tuples_to_multidict(tups):
    d = defaultdict(list)
    for k, v in tups:
        d[k].append(v)
    return d

def tuples_to_sum_map(tups):
    d = {}
    for k, v in tups:
        d[k] = v + d.get(k, 0)
    return d

class Timer:
    
    def __init__(self, message, startTime, endTime, aggregates, children, info, combinedInfo):
        #print data
        self.message = message
        self.startTime = startTime
        self.endTime = endTime
        self.elapsedTime = endTime - startTime
        self.aggregates = aggregates
        self.children = children
        self.info = info
        self.combinedInfo = combinedInfo
        self.netTime = self.elapsedTime - sum(c.elapsedTime for c in self.children) - sum(self.aggregates.values())
        
        assert(all(x > 0 for x in self.aggregates.values()))
        assert(self.netTime >= 0), self.netTime
        assert(self.elapsedTime >= 0)
        
        inner2 = sum(time for (_, time) in chain(self.net_time_by_function(), self.aggregate_times()))
        assert(eq(self.elapsedTime, inner2)), '%f != %f; \n\t%s' %(self.elapsedTime, inner2, self)
    
    @classmethod
    def from_data(cls, data):
        me = Timer(data["data"], data["startTime"], data["endTime"], data['aggregates'], [Timer.from_data(c) for c in data["children"]],
                   data.get("info", -20), data.get('combinedInfo', {}))
        me._data = data
        
        assert(data['elapsedTime'] == data['endTime'] - data['startTime'])
        return me
    
    def scale(self, offset, factor):
        def s(time):
            return (time + offset) * factor
        aggregates = {k : v*factor for (k, v) in self.aggregates.iteritems()}
        children = [c.scale(offset, factor) for c in self.children]
        t = Timer(self.message, s(self.startTime), s(self.endTime), aggregates, children, self.info, self.combinedInfo)
        t._data = self._data
        return t

    def fix_time(self, absolute_end_time_nanos):
        #print 'fixing time by %f' %absolute_end_time_nanos
        return self.scale(absolute_end_time_nanos - self.endTime, 1)

    def fix_start(self, start_time):
        return self.scale(start_time - self.startTime, 1)

    def net_time_by_function(self):
        return list(self.flatMap(lambda node : [(node.message, node.netTime)]))
        
    def info_by_function(self):
        return list(self.flatMap(lambda node : [(node.message, node.info)] + node.combinedInfo.items()))

    def aggregate_times(self):
        return list(self.flatMap(lambda node : node.aggregates.iteritems()))

    def flatMap(self, mapper):
        return chain(mapper(self), flatMap(lambda c : c.flatMap(mapper), self.children))

    def is_reduce(self):
        return 'reduce' in str(self._data) # total hack

    def __repr__(self):
        return {'message' : self.message, 'elapsed' : self.elapsedTime, 'children' : self.children, 'aggregates' : self.aggregates, 'info' : self.info}.__repr__()


class TimerEntry:
    
    def __init__(self, timer, threadId, gcTime):
        self.timer = timer
        self.threadId = threadId
        self.gcTime = gcTime

    def __repr__(self):
        return {'root' : self.timer, 'threadId' : self.threadId, 'gcTime' : self.gcTime}.__repr__()


def scraped(log_lines):
    for number, line in enumerate(log_lines):
        match = TIMER_REGEX.search(line)
        if match is not None:
            #print 'Line# %d' %number
            #print '\tstuff %s' %match.group(1)
            try:
                data = json.loads(match.group(1))
            except ValueError:
                print >> stderr, "WARNING: truncated json"
                continue
            message = data['message']
            match = MS_REGEX.search(line)
            nano_time = log_time_to_nanos(match.group(1), match.group(2))
            yield nano_time, data #TimerEntry(Timer.from_data(message['timerRoot']), data['threadId'])


def timers(lines):
    raw_timers_by_time = ((time, data) for (time, data)in scraped(lines) if 'timerRoot' in data['message'])

    return (TimerEntry(Timer.from_data(data['message']['timerRoot']).fix_time(time), data['threadId'], data['message'].get('gcTime')) for (time, data) in raw_timers_by_time)


def all_distributions(timer_trees):
    return tuples_to_multidict(chain.from_iterable(imap(lambda tree : chain(tree.net_time_by_function(), tree.aggregate_times()), timer_trees)))


def graph_by_time(plot, times_by_f, aggregator=lambda lst : sum(lst)):
    #print times_by_f
    def sane_aggregator(lst):
        ret = aggregator(lst)
        if ret < 0:
            raise "Aggregation was less than 0!"
        return ret
        
    if len(times_by_f) == 0:
        return
    time_by_f = sorted([(f, sane_aggregator(lst)) for (f, lst) in times_by_f.iteritems()], key=lambda (_, v) : -v)
    total = sum(val for _, val in time_by_f)
    for k, v in time_by_f:
        print '%s -> %.2f s (%.5f%%)' %(k, float(v) / 10**9, 100 * float(v)/total)
    
    names, times = zip(*time_by_f)
    plot.pie(times, labels=names)

DISK = set([])
IO = set([])
CPU = set([])
IO_OR_DISK = set([])

GROUP_LIST = ['SHUFFLE', 'DISK', 'NETWORK', 'CPU', 'SPILL', 'UNKNOWN', 'GC', 'FAKE_SHUFFLE', 'TEST']
GROUPS = OrderedDict((e, e) for e in GROUP_LIST)
STYLES = {name : i for (name, i) in zip(GROUPS, range(1, len(GROUPS)+1))}

SPECIAL = {'spill' : 'SPILL',
           'ConsumerPlugin.run()' : 'SHUFFLE',
           'readNextPacket' : 'NETWORK',
           'DFSOutputStream.writeChunk': 'NETWORK'} # hacky stuff for current run

def is_contained_by(a, b):
    return a == b or (a == 'SHUFFLE' and b == 'GENERAL_IO') or (a == 'SPILL' and b == 'DISK' or b == 'UNKNOWN') or (a == 'NETWORK' and b == 'UNKNOWN')

def to_resource(name):
    if name is None:
        return 'UNKNOWN'
    split = name.split('//')
    resource = 'UNKNOWN'
    if len(split) == 2:
        name, resource = split

    matches = [v for (k, v) in SPECIAL.iteritems() if k in name]
    if len(matches) > 1:
        raise
    elif len(matches) == 1:
        assert(is_contained_by(matches[0], resource))
        return matches[0]
    else:
        return GROUPS[resource]

def group(times_by_f):
    '''
    group up the times into 3 groups: IO, disk, CPU
    '''
    tups = [(to_resource(name), times) for (name, times) in times_by_f.iteritems()]
    return {k : sum(sum(times_lists, [])) for (k, times_lists) in tuples_to_multidict(tups).iteritems()}

def to_abstract_task(entry, timer):
    times_by_f = tuples_to_multidict(chain(timer.net_time_by_function(), timer.aggregate_times()))
    
    time_by_resource = group(times_by_f)

    gc_time = (entry.gcTime or 0) * 10**-6
    time_by_resource['GC'] = gc_time
    if gc_time is not None and gc_time < 0:
        raise ValueError('gc_time is less that 0: %d' %gc_time)
    #print 'gc time is %d' %gc_time
    time_by_resource['CPU'] = time_by_resource['CPU'] - gc_time

    return AbstractTask(timer.startTime, timer.endTime, time_by_resource, timer.is_reduce())

def normalize_and_ms(timers):
    min_value = min(t.startTime for t in timers)
    return [t.scale(-min_value, 10**-6) for t in timers]

def entries_from_files(names):
    files = [open(f) for f in names]
    print "using %d files %s: " %(len(files), files)
    entries = list(timers(chain.from_iterable(files)))
    for f in files:
        f.close()
    return entries

def telemetry(timer):
    """return map of {fn name : sum(infos)}"""
    return tuples_to_sum_map(timer.info_by_function())

def parse_exclude(string):
    return [x.upper() for x in string.split(',')]

def to_jobs(file_names):

    def file_id(name):
        return name[-10:]

    def query_id(name):
        p1, _, p3 = name.split('/')[-1].split('_')
        return '%s_%s' %(p1, p3)

    def stage(file_name):
        entries = entries_from_files([file_name])
        timers = normalize_and_ms([e.timer for e in entries])

        timer_and_task = [(t, to_abstract_task(e, t)) for (e, t) in zip(entries, timers)]
        sorted_timer_tasks = sorted(timer_and_task, key=lambda (_, task) : task.start)

        return [task for (_, task) in sorted_timer_tasks]

    def job(file_names):
        return [stage(file_name) for file_name in file_names]

    files_by_id = tuples_to_multidict([(file_id(name), name) for name in file_names])
    return {query_id(file_names[0]): job(file_names) for (_, file_names) in files_by_id.iteritems()}


def speed_ups(job, to_remove, scheduler_fn, hive_delay=11.5*1000):
    start = 0
    scheds = []
    for stage in job:
        shifted = shift_tasks(stage, start)
        next = scheduler_fn(shifted, to_remove)
        scheds.append(next)
        start = time_taken_ms(next) + hive_delay
    return scheds

def order(job, hive_delay=11.5*1000):
    start = 0
    ordering = []
    for stage in job:
        shifted = shift_tasks(stage, start)
        ordering.append(shifted)
        start = time_taken_ms(shifted) + hive_delay
    return ordering

def shift_tasks(tasks, start):
    offset = start - tasks[0].start
    return [t.shift_start(offset) for t in tasks]

class remove_fn(tuple):

    def __init__(self, to_remove):
        super(remove_fn, self).__init__(to_remove)

    def __call__(self, task, start_time):
        return task.without(self, start_time)

    def with_tasks(self, tasks):
        return self

DUMB_REMOVE = [g for g in GROUP_LIST if g not in ['SHUFFLE', 'FAKE_SHUFFLE']]
MY_REMOVE = [g for g in GROUP_LIST if g not in ['SHUFFLE', 'FAKE_SHUFFLE', 'CPU']]

class reduce_remove_fn(tuple):

    def __init__(self, to_remove, name):
        self.to_remove = to_remove

    def __new__(cls, to_remove, name):
        return tuple.__new__(cls, [name])

    def __call__(self, task, start_time):
        if task.is_reduce:
            return task.without(self.to_remove, start_time)
        else:
            return task.without([], start_time)

    def with_tasks(self, tasks):
        return self

MY_REMOVE_FN = reduce_remove_fn(MY_REMOVE, 'real')
DUMB_REMOVE_FN = reduce_remove_fn(DUMB_REMOVE, 'bad_assumption')

WRITE_RESOURCES = ['DISK', 'NETWORK', 'SPILL', 'UNKNOWN', 'GC']

def reduce_times(tasks):
    '''
    get difference in reduce times between real and "dumb" way
    '''
    def real(task):
        return task.time_in(WRITE_RESOURCES)

    def bad_assumption(task):
        return task.elapsed - task.time_in(['SHUFFLE'])

    return [[task.elapsed / 1000., bad_assumption(task) / 1000., real(task) / 1000.] for task in tasks if task.is_reduce]

TO_TEST = [remove_fn(tuple(x)) for x in [['TEST'], ['SHUFFLE'], ['DISK'], ['NETWORK'], ['SPILL'], ['DISK', 'SPILL'], ['DISK', 'NETWORK'], ['NETWORK', 'SHUFFLE'], ['SHUFFLE', 'DISK', 'NETWORK', 'SPILL']]]

def do_it_all(out_folder_name, in_files, TO_TEST, job_mutator=None):
    jobs = to_jobs(in_files)
    results = {}

    def do_orig(job, name='original', total=None):
        orig = flatten(order(job))
        total_time = time_taken(orig)
        if total is None:
            percent = 100
        else:
            percent = 100 * total_time / float(total)
        results[qid][name] = str((total_time, '%.2f%%' % (percent)))
        make_waterfall('%s/%s_%s' %(out_folder_name, qid, name), orig, STYLES)
        return total_time

    for qid, job in jobs.iteritems():
        results[qid] = {}
        total_time = do_orig(job)
        if job_mutator is not None:
            do_orig(job_mutator(job), 'map_only', total_time)

        for to_remove in TO_TEST:
            additional = '-'.join(to_remove)
            sped_up = speed_ups(job, to_remove, scheduler.schedule_simple)
            what_if = flatten(sped_up)
            make_waterfall(out_folder_name + '/%s_%s' %(qid, additional), what_if, STYLES)
            time = time_taken(what_if)
            results[qid][str(to_remove)] = str((time, '%.2f%%' %(100.*float(time)/total_time)))
    return results

class Medianizer(tuple):

    def __new__(cls):
        return tuple.__new__(cls, ['medianized'])

    def with_tasks(self, tasks):
        return medianize_tasks_remover(tasks)


def medianize_tasks_remover(tasks):
    map_task_elapsed = [task.elapsed for task in tasks if not task.is_reduce]
    map_median = median(map_task_elapsed)
    if len(map_task_elapsed) < len(tasks):
        reduce_median = median([task.elapsed for task in tasks if task.is_reduce])

    def medianize_task(task, start_time):
        if not task.is_reduce:
            return task.change_elapsed(start_time, map_median)
        else:
            return task.change_elapsed(start_time, reduce_median)

    return medianize_task

def map_only_jobs(job):
    for tasks in job:
        for (i, task) in enumerate(tasks):
            print 'is reduce ? %d: %s' %(i, task.is_reduce)
    new_job = [[task for task in tasks if not task.is_reduce] for tasks in job]
    return new_job


def time_taken_ms(tasks):
    return max(t.stop for t in tasks)


def time_taken(tasks):
    return max(t.stop for t in tasks) / 1000.


def main():
    from argparse import ArgumentParser
    
    parser = ArgumentParser()
    parser.add_argument('logfiles', nargs="*")
    parser.add_argument('-p', '--pie-chart', default=False, action='store_true')
    parser.add_argument('-w', '--what-if', type=parse_exclude, default=[])
    parser.add_argument('-d', '--debug', default=False, action='store_true')
    parser.add_argument('-b', '--batch', type=str, default='')
    parser.add_argument('-r', '--dumb-reduce', type=str, default='')
    parser.add_argument('-m', '--map-only', type=str, default='')
    parser.add_argument('--medianize', type=str, default='')
    parser.add_argument('-f', '--fake', type=str, default='')

    args = parser.parse_args()

    if args.fake != '':
        summary = do_it_all(args.fake , args.logfiles, [remove_fn(['FAKE_SHUFFLE']), remove_fn(['TEST'])])
        with open(os.path.join(args.fake, 'summary.json'), 'w+') as f:
            json.dump(summary, f, indent=1, sort_keys=True)
        return

    if args.medianize != '':
        summary = do_it_all(args.medianize , args.logfiles, [Medianizer(), remove_fn(['TEST'])])
        with open(os.path.join(args.medianize, 'summary.json'), 'w+') as f:
            json.dump(summary, f, indent=1, sort_keys=True)
        return

    if args.map_only != '':
        summary = do_it_all(args.map_only, args.logfiles, [], job_mutator=map_only_jobs)
        with open(os.path.join(args.map_only, 'summary.json'), 'w+') as f:
            json.dump(summary, f, indent=1, sort_keys=True)
        return

    if args.dumb_reduce != '':
        jobs = to_jobs(args.logfiles)
        to_write = {k: reduce_times(flatten(job)) for (k, job) in jobs.iteritems()}
        with open(args.dumb_reduce, 'w+') as f:
            json.dump(to_write, f, indent=1, sort_keys=True)
        return

    if args.batch != '':
        summary = do_it_all(args.batch, args.logfiles, TO_TEST)
        with open(os.path.join(args.batch, 'summary.json'), 'w+') as f:
            json.dump(summary, f, indent=1, sort_keys=True)
        return

    def debug(string):
        if args.debug:
            print string

    to_exclude = args.what_if
    assert(all(x in GROUP_LIST for x in to_exclude)), '%s' %to_exclude

    entries = entries_from_files(args.logfiles)
    timers = normalize_and_ms([e.timer for e in entries])
    
    timer_and_task = [(t, to_abstract_task(e, t)) for (e, t) in zip(entries, timers)]
    sorted_timer_tasks = sorted(timer_and_task, key=lambda (_, task) : task.start)
    print 'Total time: %f' %sorted_timer_tasks[-1][1].stop

    if len(to_exclude) > 0:
        timers, old_tasks = zip(*sorted_timer_tasks)
        new_tasks = scheduler.schedule(old_tasks, to_exclude)
        sorted_timer_tasks = zip(timers, new_tasks)
        print 'Time with speed-up (%s) %f' %(args.what_if, sorted_timer_tasks[-1][1].stop)

    if args.debug:
        for i, (timer, task) in enumerate(sorted_timer_tasks):
            print 'task %d:' %i
            for resource in GROUP_LIST:
                print '\tportion %s: %.2f%%' %(resource, 100*task.portion(resource))
            for tel, val in telemetry(timer).iteritems():
                if val > 1000*1000:
                   print '\ttelemetry (%s): %.3fM' %(tel, val/(1000*1000))

    make_waterfall('test', [task for (_, task) in sorted_timer_tasks], STYLES)
    
    if args.pie_chart:
        for (timer, _) in sorted_timer_tasks:
            graph_by_time(pyplot, tuples_to_multidict(chain(timer.net_time_by_function(), timer.aggregate_times())))
            print json.dumps(timer._data, indent=1)
            pyplot.show()
        print 'found %d tasks' %len(entries)

if __name__ == '__main__':
    main()