# aws-s3-tables-pyarrow-example
aws-s3-tables-pyarrow-example
![image](https://github.com/user-attachments/assets/e63a2556-dee7-4ff1-9894-b61d31dfc3bf)

# How to use 
```
main(
        table_bucket_arn='XXXX',
        namespace='example_namespace',
        table_name='silver_orders'
    )
```

# Complete code 
```
import boto3
import json
import fastavro
from io import BytesIO
import base64
import pyarrow as pa
import pyarrow.parquet as pq

# Create an S3Tables client
s3tables_client = boto3.client('s3tables')


def get_table_metadata(table_bucket_arn, namespace, table_name):
    """Retrieve the table metadata from S3 Tables."""
    response = s3tables_client.get_table(
        tableBucketARN=table_bucket_arn,
        namespace=namespace,
        name=table_name
    )
    return response.get('metadataLocation')


def get_s3_object(bucket_name, object_key):
    """Read an object from S3 and return its content."""
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    return response['Body'].read()


def parse_metadata(metadata_location):
    """Parse the JSON metadata file from S3."""
    bucket_name = metadata_location.split('//')[1].split('/')[0]
    object_key = '/'.join(metadata_location.split('//')[1].split('/')[1:])
    file_content = get_s3_object(bucket_name, object_key).decode('utf-8')
    return json.loads(file_content)


def read_avro_file(s3_path):
    """Read and return Avro file contents."""
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    file_content = get_s3_object(bucket, key)
    with BytesIO(file_content) as buffer:
        reader = fastavro.reader(buffer)
        return list(reader)


def get_latest_snapshot(metadata):
    """Get the latest snapshot from the metadata."""
    return max(metadata['snapshots'], key=lambda x: x['sequence-number'])


def json_serial(obj):
    """Handle binary data for JSON serialization."""
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('utf-8')
    raise TypeError(f"Type {type(obj)} not serializable")


def process_manifest(manifest_path):
    """Process each manifest file and return its contents."""
    manifest_content = read_avro_file(manifest_path)
    for entry in manifest_content:
        print(json.dumps(entry, indent=2, default=json_serial))
    return manifest_content


def get_combined_parquet_dataframe(parquet_file_paths):
    """Read and combine multiple Parquet files into a single PyArrow DataFrame."""
    tables = []
    for parquet_file_path in parquet_file_paths:
        bucket, key = parquet_file_path.replace('s3://', '').split('/', 1)
        file_content = get_s3_object(bucket, key)
        with BytesIO(file_content) as buffer:
            table = pq.read_table(buffer)
            tables.append(table)

    combined_table = pa.concat_tables(tables)
    return combined_table


def collect_parquet_file_paths(manifest_list):
    """Collect parquet file paths from the manifest list."""
    parquet_file_paths = []

    for manifest in manifest_list:
        print(f"\nProcessing manifest: {manifest['manifest_path']}")
        manifest_content = process_manifest(manifest['manifest_path'])

        for entry in manifest_content:
            if 'data_file' in entry:
                parquet_file_path = entry['data_file']['file_path']
                print("Adding parquet file path:", parquet_file_path)
                parquet_file_paths.append(parquet_file_path)

    return parquet_file_paths


def main(table_bucket_arn, namespace, table_name):
    # Define parameters for getting the table

    # Get the metadata location and parse it
    metadata_location = get_table_metadata(table_bucket_arn, namespace, table_name)
    print("metadata_location", metadata_location)

    metadata = parse_metadata(metadata_location)

    # Get the latest snapshot and process its manifests
    latest_snapshot = get_latest_snapshot(metadata)
    print(f"\nProcessing latest snapshot {latest_snapshot['snapshot-id']}:")

    manifest_list_location = latest_snapshot['manifest-list']
    manifest_list = read_avro_file(manifest_list_location)

    # Collect all parquet file paths from the manifests
    parquet_file_paths = collect_parquet_file_paths(manifest_list)

    # Get the combined DataFrame from Parquet files
    combined_df = get_combined_parquet_dataframe(parquet_file_paths)

    # Optionally convert to Pandas DataFrame for analysis
    df = combined_df.to_pandas()

    print("\nContents of combined Parquet files:")
    print(df.head())


if __name__ == "__main__":
    main(
        table_bucket_arn='XXX',
        namespace='example_namespace',
        table_name='silver_orders'
    )


```
