import subprocess
import time
import argparse
import sys
import json

from urllib.parse import urlparse


def run_hive_query(query_str):
    # Start the timer
    start_time = time.time()

    # Construct command
    command = [
        'hive',
        '-e', query_str
    ]

    # Run the command
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"{result.stdout}")
        print(f"{result.args} done")
      
    except subprocess.CalledProcessError as e:
        print(f"Error: {command}")
        print(f"Error: {e.stdout}")
        print(f"Error: {e.stderr}")
        return

    # Stop the timer
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    return elapsed_time


def benchmark_hive_to_s3(elapsed_time, s3_path):

    s3_bucket, s3_prefix = split_s3_url(s3_path)
    print(f"bucket: {s3_bucket}; prefix: {s3_prefix}")
    
    command = [
        'aws',
        's3api',
        'list-objects',
        '--bucket', s3_bucket,
        '--prefix', s3_prefix,
        '--output', 'json',
        '--query', '[sum(Contents[].Size), length(Contents[])]'
    ]
    
    # Run the command
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)

        parsed_output = json.loads(result.stdout)
        total_size_byte = parsed_output[0]
        num_objects = parsed_output[1]

        total_size_mb = bytes_to_megabytes(total_size_byte)
        throughput = total_size_mb / elapsed_time

        print(f"Elapsed time: {elapsed_time:.2f} seconds")
        print(f"Total size: {total_size_mb} MB")
        print(f"Num of objects: {num_objects}")
        print(f"Throughput: {throughput:.2f} MB/s")


    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return

def clean_s3(s3_path):
    command = [
        'aws',
        's3',
        'rm',
        s3_path,
        '--recursive'
    ]
    
    # Run the command
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"{result.stdout.decode('utf-8')}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: {e.stderr.decode('utf-8')}")
        return

def bytes_to_megabytes(bytes):
    return bytes / (1024 * 1024)



def split_s3_url(s3_url):
    parsed_url = urlparse(s3_url)
    bucket = parsed_url.netloc
    prefix = parsed_url.path.lstrip('/')
    return bucket, prefix



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run s3-dist-cp and record the running time.')
    parser.add_argument('--s3', required=True, help='S3 output path (e.g., s3://your-source-bucket/path/)')
    parser.add_argument('--hiveSrcTable', default='store_sales', help='Hive source table name')
    parser.add_argument('--hiveDstTable', default='store_sales_benchmark_test', help='Hive destination table name')

    args = parser.parse_args()

    # Init
    clean_s3(args.s3)
    drop_table_query = f"DROP TABLE IF EXISTS {args.hiveDstTable};"
    run_hive_query(drop_table_query)

    # Benchmark
    hive_s3_query = f"CREATE TABLE {args.hiveDstTable} STORED AS PARQUET LOCATION '{args.s3}' AS SELECT * FROM {args.hiveSrcTable} where ss_sold_date_sk>=2450816;"
    elapsed_time = run_hive_query(hive_s3_query)
    benchmark_hive_to_s3(elapsed_time, args.s3)

    
