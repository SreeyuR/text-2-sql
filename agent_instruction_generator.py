import os
import time
import json
import gzip
from io import BytesIO
import boto3
import pandas as pd


def process_folder(s3_client, bucket_name, folder_name):
    """
    Process JSON files within a specified S3 folder.

    :param s3_client: Boto3 S3 client
    :param bucket_name: Name of the S3 bucket
    :param folder_name: Path to the folder in the S3 bucket
    :return: Dictionary with columns and sample data
    """
    folder_context = {
        'columns': set(),  # Collect unique column names
        'sample_data': None  # Store sample data if needed
    }

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_name, Delimiter='/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json.gz'):
                try:
                    response = s3_client.get_object(Bucket=bucket_name, Key=key)
                    with gzip.GzipFile(fileobj=BytesIO(response['Body'].read())) as gz:
                        for line in gz:
                            try:
                                json_data = json.loads(line.decode('utf-8'))
                                if isinstance(json_data, dict):
                                    if folder_context['sample_data'] is None:
                                        folder_context['sample_data'] = json_data
                                    folder_context['columns'].update(json_data.keys())
                                else:
                                    print(f"Unexpected JSON object in file: {key}")
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON line from file {key}: {e}")
                except Exception as e:
                    print(f"Error processing file {key}: {e}")

    folder_context['columns'] = list(folder_context['columns'])
    return folder_context


def analyze_json_gz_files(bucket_name, top_level_root_folders, region):
    """
    Analyze JSON files stored as .json.gz in S3.

    :param bucket_name: Name of the S3 bucket
    :param top_level_root_folders: List of top-level folders in the S3 bucket
    :param region: AWS region of the S3 bucket
    :return: Dictionary with folder-wise context (columns and sample data)
    """
    s3_client = boto3.client('s3', region_name=region)
    data_context = {}

    root_folders = [f'{folder.rstrip("/")}/data/' for folder in top_level_root_folders]

    for i, root_folder in enumerate(root_folders):
        folder_context = process_folder(s3_client, bucket_name, root_folder)
        data_context[top_level_root_folders[i]] = folder_context

    return data_context


def analyze_csv_files(root_folder):
    """
    Analyze CSV files in a local directory.

    :param root_folder: Root folder containing CSV files
    :return: Dictionary with file-wise context (columns and sample data)
    """
    data_context = {}

    for dirpath, _, filenames in os.walk(root_folder):
        for file in filenames:
            if file.endswith('.csv'):
                file_path = os.path.join(dirpath, file)
                df = pd.read_csv(file_path)
                columns = df.columns.tolist()
                sample_data = df.head(1).to_dict(orient='records')[0]
                relative_path = os.path.relpath(file_path, start=root_folder).split(os.sep)[0].replace(' ', '_')
                data_context[relative_path] = {'columns': columns, 'sample_data': sample_data}

    return data_context


def get_all_tables(athena_client, database_name, athena_results_bucket):
    """
    Retrieve all tables and views from an Athena database.

    :param athena_client: Boto3 Athena client
    :param database_name: Name of the Athena database
    :param athena_results_bucket: S3 bucket to store Athena results
    :return: List of table and view names
    """
    query = f"SHOW TABLES IN {database_name}"
    result = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': f's3://{athena_results_bucket}/'}
    )

    query_execution_id = result['QueryExecutionId']
    _wait_for_query_execution(athena_client, query_execution_id)

    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    return [row['Data'][0]['VarCharValue'] for row in results['ResultSet']['Rows'][1:]]


def get_table_schema(athena_client, database_name, table_name, env='poc'):
    """
    Retrieve the schema of a given table from Athena.

    :param athena_client: Boto3 Athena client
    :param database_name: Name of the Athena database
    :param table_name: Name of the table/view
    :param env: Environment (default is 'poc')
    :return: List of columns in the table
    """
    athena_output = 'athena-destination-store-texttosql' if env == 'poc' else 'athena-destination-chatbot'
    query = f"DESCRIBE {database_name}.{table_name}"
    result = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': f's3://{athena_output}/'}
    )

    query_execution_id = result['QueryExecutionId']
    _wait_for_query_execution(athena_client, query_execution_id)

    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    return [row['Data'][0]['VarCharValue'] for row in results['ResultSet']['Rows'][1:]]


def generate_instruction(database_name, data_context=None, env='poc'):
    """
    Generate SQL queries based on the schema information from Athena.

    :param database_name: Name of the Athena database
    :param data_context: Context containing schema information (optional)
    :param env: Environment (default is 'poc')
    :return: Instruction text with SQL queries
    """
    instruction_parts = [
        f"Role: You are an advanced database querying agent crafted specifically for "
        f"generating precise SQL queries for Amazon Athena concerning the {database_name}.",
        "Objective: Generate SQL queries to return data based on the provided schema "
        "and user request. Ultimately, answer the user's question regarding the data "
        "generated using SQL Query.",
        "1. Query Decomposition and Understanding:",
        "- Analyze the user’s request to understand the main objective.",
        "- Break down requests into sub-queries that can each address a part of the "
        "user's request, using the schema provided.",
        "2. SQL Query Creation:",
        "- For each sub-query, use the relevant tables and fields from the provided schema.",
        "- Construct SQL queries that are precise and tailored to retrieve the exact "
        "data required by the user’s request.",
        "- Use table joins when combining data from two or more tables based on related "
        "columns. For example, if data is split across multiple tables, each containing "
        "different attributes about a common entity (such as building id), you may need "
        "to use a table join. Table joins are also useful when filtering data based on "
        "conditions that span multiple tables. Lastly, table joins are useful when "
        "aggregating data from multiple tables or enriching a dataset with additional "
        "context or descriptive information stored in another table. The types of joins "
        "are: INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL JOIN.",
        "- Avoid joins if all the required data is available in a single table.",
        "3. Query Execution and Response:",
        "- Execute the constructed SQL queries against the Amazon Athena database.",
        "- Return the results of the SQL query in a format that answers the user's "
        "question, ensuring data integrity and accuracy.",
        "If you get the following Lambda error:",
        "<lambda_error>",
        "Lambda response exceeds maximum size 25KB: 123644",
        "</lambda_error>",
        "Then: LIMIT to 10 rows.",
        "The following examples illustrate the kind of queries you should be able to "
        "construct based on the available data:"
    ]

    athena_output = 'athena-destination-store-texttosql' if env == 'poc' else 'athena-destination-chatbot'

    if data_context:
        for folder, context in data_context.items():
            table_name = folder
            columns = ', '.join([f'"{col}"' for col in context['columns']])
            sample_query = f"SELECT {columns} FROM \"{table_name}\" LIMIT 5;"
            instruction_parts.append(f"- Table `{table_name}` example query: {sample_query}")
    else:
        athena_client = boto3.client('athena')
        tables = get_all_tables(athena_client, database_name, athena_output)
        for table_name in tables:
            columns = get_table_schema(athena_client, database_name, table_name, env)
            formatted_columns = ', '.join([f'"{col}"' for col in columns])
            sample_query = f"SELECT {formatted_columns} FROM \"{table_name}\" LIMIT 5;"
            instruction_parts.append(f"- Table `{table_name}` example query: {sample_query}")

    return ' '.join(instruction_parts)


def _wait_for_query_execution(athena_client, query_execution_id):
    """
    Helper function to wait for an Athena query to complete.

    :param athena_client: Boto3 Athena client
    :param query_execution_id: Athena query execution ID
    """
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if state != 'SUCCEEDED':
        raise Exception(f"Query failed with state {state}")

