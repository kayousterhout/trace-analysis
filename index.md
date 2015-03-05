___
title: Trace Analysis
layout: default
___
# Introduction

There has been significant effort dedicated towards improving the performance of data analytics frameworks, but comparatively little effort has been spent systematically identifying the performance bottlenecks of these systems.  We set out to make data analytics framework performance easier to understand, both by quantifying the performance bottlenecks in distributed computation frameworks on a few benchmarks, and by providing tools that others can use to understand performance of the workloads we ran and of their own workloads.  This webpage links to both (1) scripts you can use to understand and visualize Spark's performance on workloads you run and (2) traces that we have collected from running the Big Data Benchmark and TPC-DS workloads on Spark.

This project is summarized in a [paper](http://www.eecs.berkeley.edu/~keo/publications/nsdi15-final147.pdf) that will appear at USENIX NSDI, 2015.

# I want to visualize performance of my own workload.

A suite of visualization tools that you can use to understand Spark's performance are publicly avialable [here](https://github.com/kayousterhout/trace-analysis).  The README on that page describes how those scripts can be used.

# I want do my own analysis on the traces you collected.

We collected traces from running the big data benchmark and TPC-DS benchmark on clusters of 5-60 machines.  All of the traces are licensed under a [Creative Commons Attribution 4.0 International License](http://creativecommons.org/licenses/by/4.0/).

