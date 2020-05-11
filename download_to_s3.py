import os
import synapseclient as sc
import boto3 as boto
import argparse

TREMOR_TABLE = "syn12977322"

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket-name", default='phil-sample-sensor-data')
    parser.add_argument("--base-key", default="")
    parser.add_argument("--aws-profile",
            help="A named profile to use with awscli",
            default="phil@sandbox")
    parser.add_argument("--limit", default=1000, type=int)
    args = parser.parse_args()
    return(args)


def download_sensor_data(syn, synapse_id, col, limit=1000):
    standard_query = (f"select recordId, \"{col}\" from {synapse_id} "
                      f"where \"{col}\" is not null")
    if isinstance(limit, int):
        if limit > 0:
            q = syn.tableQuery(f"{standard_query} limit {limit}")
        else:
            raise ValueError("limit must be greater than 0")
    elif limit is None:
        q = syn.tableQuery(standard_query)
    else:
        raise TypeError("Parameter `limit` must be an integer")
    mapping = syn.downloadTableColumns(q, col)
    return mapping


def store_to_s3(bucket_name, base_key, file_handle_mapping,
                profile_name="phil@sandbox"):
    session = boto.Session(profile_name=profile_name)
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    while len(file_handle_mapping):
        handle, path = file_handle_mapping.popitem()
        bucket.upload_file(path, os.path.join(base_key, os.path.basename(path)))


def main():
    args = read_args()
    syn = sc.login()
    left = download_sensor_data(
            syn = syn,
            synapse_id = TREMOR_TABLE,
            col = "left_motion.json",
            limit = args.limit)
    right = download_sensor_data(
            syn = syn,
            synapse_id = TREMOR_TABLE,
            col = "right_motion.json",
            limit = args.limit)
    store_to_s3(
            bucket_name = args.bucket_name,
            base_key = args.base_key,
            file_handle_mapping = left,
            profile_name = args.aws_profile)
    store_to_s3(
            bucket_name = args.bucket_name,
            base_key = args.base_key,
            file_handle_mapping = right,
            profile_name = args.aws_profile)


if __name__ == "__main__":
    main()
