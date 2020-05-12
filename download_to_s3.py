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
    df = q.asDataFrame()
    df['path'] = df[col].astype(str).map(mapping)
    return df


def store_to_s3(bucket_name, base_key, file_handle_df,
                profile_name="phil@sandbox"):
    session = boto.Session(profile_name=profile_name)
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    for i, r in file_handle_df.iterrows():
        record_id, path = r["recordId"], r["path"]
        metadata = {"record-id": record_id}
        bucket.upload_file(
                path,
                os.path.join(base_key, os.path.basename(path)),
                ExtraArgs={"Metadata":metadata})


def main():
    args = read_args()
    syn = sc.login()
    left_col = "left_motion.json"
    right_col = "right_motion.json"
    left = download_sensor_data(
            syn = syn,
            synapse_id = TREMOR_TABLE,
            col = left_col,
            limit = args.limit)
    right = download_sensor_data(
            syn = syn,
            synapse_id = TREMOR_TABLE,
            col = right_col,
            limit = args.limit)
    store_to_s3(
            bucket_name = args.bucket_name,
            base_key = os.path.join(args.base_key, left_col),
            file_handle_df = left,
            profile_name = args.aws_profile)
    store_to_s3(
            bucket_name = args.bucket_name,
            base_key = os.path.join(args.base_key, right_col),
            file_handle_df = right,
            profile_name = args.aws_profile)


if __name__ == "__main__":
    main()

