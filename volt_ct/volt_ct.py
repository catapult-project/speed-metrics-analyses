from google.cloud import datastore
from google.cloud import storage

import argparse
import csv
import os
import pathlib

CSV_OUTPUT_DIR = 'csv-outputs'
MAX_QUERY_LIMIT = 1000
TS_PROPERTY = 'TsCompleted'

# Map of original column names -> modified column names.
COLUMN_MAP = {
  'traceUrls': 'trace',
  'timeToFirstContentfulPaint (ms)': 'FCP',
  'totalBlockingTime (ms)': 'TBT',
  'largestContentfulPaint (ms)': 'LCP',
  'mainFrameCumulativeLayoutShift': 'CLS',
}

FINAL_FIELDS = ('url trace run_date_str FCP TBT CLS LCP').split(' ')


def CleanRow(input_row):
  output_row = {
    'url': input_row['page_name'].split(' ')[0],
    'run_date_str': input_row['run_date_str']
  }
  for (input_col, output_col) in COLUMN_MAP.items():
    # .get because sometimes a column is missing in older CSVs.
    output_row[output_col] = input_row.get(input_col, None)
  return output_row


def DateStingToCtTime(date_str):
  """Converts yyyy-mm-dd string to yyyymmddhhmmss int.

  This is the the timestamp format Cluster Telemetry uses.
  """
  if date_str is None:
    return None

  try:
    (year, month, day) = date_str.split('-')
  except ValueError:
    raise Exception("Date in incorrect format. Use yyyy-mm-dd")

  return int(f'{year}{month}{day}000000')


def GetAllVoltRuns(since_str):
  client = datastore.Client(project='skia-public', namespace='cluster-telemetry')
  query = client.query(kind='ChromiumAnalysisTasks')
  query.add_filter('GroupName', '=', 'volt10k-m80')
  since_int = DateStingToCtTime(since_str)

  for res in query.fetch(limit=MAX_QUERY_LIMIT):
    # Can't make this part of query because there is no index on
    # 'GroupName' and 'TsCompleted' together. Filtering for GroupName
    # first should yeild a smaller resultset.
    if since_int is not None and res[TS_PROPERTY] < since_int:
      continue
    yield res


def CtTimeToDateString(ct_time):
  time_str = str(ct_time)
  year = time_str[:4]
  month = time_str[4:6]
  day = time_str[6:8]
  return f"{year}-{month}-{day}"


def DownloadOutputs(runs):
  # TODO: Add a directory.
  pathlib.Path(CSV_OUTPUT_DIR).mkdir(exist_ok=True)
  storage_client = storage.Client()
  for run in runs:
    date_str = str(run[TS_PROPERTY])
    raw_output = run['RawOutput']
    if raw_output.strip() == "":
      print("No url found for", date_str, "| Skipping.")
      continue
    blob_url = raw_output.replace('https://ct.skia.org/results/', 'gs://')
    download_path = os.path.join(CSV_OUTPUT_DIR, str(date_str) + '.csv')
    if pathlib.Path(download_path).exists():
      print(f'{download_path} already exists. Skipping download.')
      continue
    with open(download_path, 'wb') as f:
      storage_client.download_blob_to_file(blob_url, f)
      print('Downloaded csv to ' + download_path)


def AddDateAndMergeCsvs(merged_filename, since_str):
  # NOTE: We first accumulate all the rows because there is no guarantee
  # that every CSV will have the same field names. If memory becomes an issue
  # change this to only read the header of each file.
  all_csvs = pathlib.Path(CSV_OUTPUT_DIR).glob('*')
  all_fields = {'ct_raw_ts_completed', 'run_date_str'}
  all_rows = []
  since_int = DateStingToCtTime(since_str)
  files_processed = 0

  for csv_file in all_csvs:
    # Check if file is greater than since
    date_str = csv_file.name.split('.')[0]
    date_int = int(date_str)
    if since_int is not None and date_int < since_int:
      continue
    files_processed += 1
    with open(csv_file) as f:
      reader = csv.DictReader(f)
      for row in reader:
        row['ct_raw_ts_completed'] = date_int
        row['run_date_str'] = CtTimeToDateString(date_int)
        cleaned_row = CleanRow(row)
        all_rows.append(cleaned_row)
      all_fields.update(reader.fieldnames)

  with open(merged_filename, 'w') as f:
    writer = csv.DictWriter(f, FINAL_FIELDS)
    writer.writeheader()
    writer.writerows(all_rows)
    print(f'Processed {files_processed} files.')
    print(f'Wrote {len(all_rows)} rows into {merged_filename}')


def Main():
  parser = argparse.ArgumentParser(description='Get output from Volt 10k CT runs')
  parser.add_argument('--since', type=str,
                      help='Min date of run. In yyyy-mm-dd format.')
  parser.add_argument('--merged-filename', type=str, default='merged.csv',
                      help='Merged output filename. Default: merged.csv')
  args = parser.parse_args()
  all_volt_runs = GetAllVoltRuns(args.since)
  DownloadOutputs(all_volt_runs)
  AddDateAndMergeCsvs(args.merged_filename, args.since)


if __name__ == "__main__":
  Main()
