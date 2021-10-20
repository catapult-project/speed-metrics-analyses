#!/usr/bin/env python

# Copyright (c) 2020 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

"""Cluster Telemetry Logs Processor

Cluster Telemetry log files contains detailed unaggregated result for each run,
which are useful for doing analysis like a variability study with high number
of page repeat. The produced csv file does not contain these individual
results; the results are aggregated together for each URL instead.

This file provides functions to parse these logs and produce a CSV file that
contains all the information, which can then be easily used in tools like R or
Sheets. The log file mixes various different log sources together, so this
script uses regex parsing to pull out the relevant information. The parsing
code may need to be updated as cluster telemetry log structure changes.
"""

from __future__ import print_function

import ast
import csv
import os
import re
import sys
import traceback
import argparse
from collections import defaultdict
from pprint import pprint


class TraceUrlMismatch(Exception):
  def __init__(self, expected_url, found_url, metric_name, run_index):
    print("Expected", expected_url)
    print("but found")
    print(found_url)
    print("Metric:", metric_name)
    print("Storyset repeat: ", run_index)


def string_to_list(string):
  try:
    # ast.literal_eval only parses static data so safer than eval.
    return ast.literal_eval(string)
  except:
    print("Encountered error while processing this:\n" + string)
    raise


# Histograms are referred to a "rows" in CT logs.
def get_histograms(filename):
  output = []
  with open(filename) as f:
    # Iterate until you find the merge log line.
    while True:
      line = f.readline()
      if line == '':
        return output  # Reached EOF. No histograms in this file.
      if re.search('Merging \d+ csv files into \d+ columns', line):
        break

    # Read the rest of the file.
    logs = f.read()
    # First I remove the exec.go:83 prefix from some lines, because in the logs
    # they look like this:
    # ["For rows: [{'productVersions': '', 'osVersi
    # I0105 18:10:08.017015   26595 exec.go:83] exec.go:83 ons': 'M', ...
    # NOTE: The line number of 83 here may change. Please fix it if to be the
    # right number if it does so.
    # TODO: Automate finding this line number.
    logs = re.sub('\nI.*exec\.go\:83] exec\.go\:83 ', '\n', logs)
    # Then I remove all lines that are not from other log sources, for example,
    # util.go, exec.go:223 etc.
    logs = re.sub('\nI.*?\.go\:\d+] .*?\.go\:\d+.*?(?=\n)', '\n', logs)
    # Then I remove all new lines.
    logs = re.sub('\n', '', logs);

    for histograms_str in re.findall("For rows: (.*?)Avg row is",
                                     logs, re.DOTALL):
      output.extend(string_to_list(histograms_str))

  print("%d histograms processed." % len(output))
  return output


# Returns url -> run_index -> dict of metrics
def get_run_results(ct_histograms):
  url_to_run_index_to_rows = defaultdict(lambda: defaultdict(dict))
  fieldnames = set(['page_name', 'run_index', 'trace_url'])
  stats = {'more_than_one_value': defaultdict(int)}

  for histogram in ct_histograms:
    if histogram['avg'] == '': continue
    if int(histogram['count']) < 1: continue

    metric_name = histogram['name']
    # Stories can be like "https://google.com (#12)".
    # Strip the story number at the end.
    url = histogram['stories'].split("(")[0].strip()
    run_index = int(histogram['storysetRepeats'])

    if histogram['count'] > '1':
      # Track this case.
      stats['more_than_one_value'][metric_name] += 1

    metric_value = float(histogram['avg'])
    metrics_dict = url_to_run_index_to_rows[url][run_index]
    metrics_dict['page_name'] = url
    metrics_dict[metric_name] = float(metric_value)
    trace_url = histogram['traceUrls']
    if 'trace_url' in metrics_dict:
      # All histograms for the same url and run index should have the same
      # trace url.
      if (metrics_dict['trace_url'] != trace_url):
        raise TraceUrlMismatch(metrics_dict['trace_url'], trace_url,
                               metric_name, run_index)
    else:
      metrics_dict['trace_url'] = trace_url
    fieldnames.add(metric_name)
  return {'run_results': url_to_run_index_to_rows, 'fieldnames': fieldnames}


def write_results_to_csv(out_filename, run_results, fieldnames):
  rows = 0
  with open(out_filename, 'w') as f:
    writer = csv.DictWriter(f, fieldnames)
    writer.writeheader()
    for run_index_to_metrics in run_results.values():
      all_runs = run_index_to_metrics.values()
      for run_result in all_runs:
        writer.writerow(run_result)
        rows += 1
  print("Wrote %d rows to %s" % (rows, out_filename))


def transform_single_file(args):
  if not os.path.exists(args.outdir):
    os.makedirs(args.outdir, mode=0o755)

  for input_file in args.input_files:
    print("Processing " + input_file)
    basename, _ = os.path.splitext(input_file)
    output_file = os.path.join(args.outdir, basename + ".csv")
    results = get_run_results(get_histograms(input_file))
    write_results_to_csv(output_file, **results)

  # Not using f-strings to keep compatibility with python2.
  print("Transformed %d files to csv." % len(args.input_files))


def transform_and_merge(args):
  out_filename = args.merge
  all_results = []

  for input_file in args.input_files:
    print("Processing " + input_file)
    basename, _ = os.path.splitext(input_file)
    output_file = os.path.join(args.outdir, basename + ".csv")
    all_results.append(get_run_results(get_histograms(input_file)))

  all_fieldnames = set()
  for fieldnames in [x['fieldnames'] for x in all_results]:
    all_fieldnames.union(fieldnames)

  rows = 0
  with open(out_filename, 'w') as f:
    writer = csv.DictWriter(f, list(fieldnames))
    writer.writeheader()
    for run_results in [x['run_results'] for x in all_results]:
      for run_index_to_metrics in run_results.values():
        all_runs = run_index_to_metrics.values()
        for run_result in all_runs:
          rows += 1
          writer.writerow(run_result)

  print("Wrote %d rows to %s" % (rows, out_filename))


def main():
  argparser = argparse.ArgumentParser(
    description="Transform a cluster telemetry log file to csv.")
  argparser.add_argument("--outdir", help="path to output directory",
                         default=os.curdir)
  argparser.add_argument('--merge', nargs='?', const="merged.csv",
                         metavar="MERGED_FILENAME",
                         help="merge all outputs into one csv")
  argparser.add_argument("input_files", nargs="+",
                         help="""Path to one or more input files. Output
                         filenames are deduced by replacing the extension with
                         csv.""")
  args = argparser.parse_args()

  if args.merge:
    transform_and_merge(args)
  else:
    transform_single_file(args)


if __name__ == "__main__":
  main()
