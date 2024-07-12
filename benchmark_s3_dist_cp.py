import subprocess
import time
import argparse
import sys
import json

from urllib.parse import urlparse


def run_s3_dist_cp(src, dest, mode):
    # Start the timer
    start_time = time.time()

    # Construct the s3-dist-cp command
    command = [
        '/usr/bin/s3-dist-cp',
        '--src', src,
        '--dest', dest
    ]

    # Run the command
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"s3-dist-cp cmd: {result.args} done")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return

    # Stop the timer
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    return elapsed_time


def benchmark_s3_to_hdfs(elapsed_time, hdfs_path):
    command = [
        'hadoop',
        'fs',
        '-du',
        '-s',
        hdfs_path
    ]
    
    # Run the command
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        size_info = result.stdout.strip().split()
        single_size = int(size_info[0])
        replicated_size = int(size_info[1])
        path = size_info[2]

        single_size_mb = bytes_to_megabytes(single_size)
        replicated_size_mb = bytes_to_megabytes(replicated_size)
        #throughput = replicated_size_mb / elapsed_time
        s3_throughput = single_size_mb / elapsed_time
        e2e_throughput = replicated_size_mb / elapsed_time

        print(f"Elapsed time: {elapsed_time:.2f} seconds")
        print(f"Path: {path}")
        print(f"Actual Size: {single_size} bytes ({single_size_mb:.2f} MB)")
        print(f"Replicated Size: {replicated_size} bytes ({replicated_size_mb:.2f} MB)")
        print(f"Throughput: {s3_throughput:.2f} MB/s")


    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return

def benchmark_hdfs_to_s3(elapsed_time, s3_path):

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

def clean_hdfs(hdfs_path):
    command = [
        'hadoop',
        'fs',
        '-rm',
        '-r',
        hdfs_path
    ]
    
    # Run the command
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"{result.stdout.decode('utf-8')}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: {e.stderr.decode('utf-8')}")
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
    parser.add_argument('--mode', required=True, help='valid option: s3-to-hdfs|hdfs-to-s3|]')
    parser.add_argument('--s3', required=True, help='S3 path (e.g., s3://your-source-bucket/path/)')
    parser.add_argument('--hdfs', required=True, help='HDFS path (e.g., hdfs:///your-destination-path/)')
    parser.add_argument('--dataSrc', required=False, help='S3 path for source data (e.g., s3://your-source-bucket/path/)')

    args = parser.parse_args()

    if args.mode=='s3-to-hdfs':
        elapsed_time = run_s3_dist_cp(args.s3, args.hdfs, args.mode)
        benchmark_s3_to_hdfs(elapsed_time, args.hdfs)
        clean_hdfs(args.hdfs)

    elif args.mode=='hdfs-to-s3':
        # First, copy src data from s3 to hdfs 
        if args.dataSrc is None:
            print(f"Error: Should provide --dataSrc in mode {mode}")
            sys.exit(1)
        clean_hdfs(args.hdfs)
        run_s3_dist_cp(args.dataSrc, args.hdfs, args.mode)

        # Second, benchmark
        clean_s3(args.s3)
        elapsed_time = run_s3_dist_cp(args.hdfs, args.s3, args.mode)
        benchmark_hdfs_to_s3(elapsed_time, args.s3)

    else:
        print(f"Error: benchmark mode is not valid")
        sys.exit(1)

    

