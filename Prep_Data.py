import boto3

def list_s3_folders(bucket_name):
    """
    Get a list of top-level folders.
    """
    # Initialize S3 client
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    folders = set()  # Use a set to avoid duplicate folders

    # Iterate through pages
    for page in paginator.paginate(Bucket=bucket_name, Delimiter='/'):
        # Add prefixes (folders) to the set
        for prefix in page.get('CommonPrefixes', []):
            folders.add(prefix['Prefix'])

    return list(folders)

def get_s3_data_sources(bucket_name):
    """
    Get a list of data sources to create a glue crawler
    """
    # list of data sources
    s3_top_level_folders = list_s3_folders(bucket_name)
    data_sources = []
    for folder in s3_top_level_folders:
        path_dict = {"path": f"s3://{bucket_name}/{folder}AWSDynamoDB/data/"}
        data_sources.append(path_dict)
    return data_sources

d
