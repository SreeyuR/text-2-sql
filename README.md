# text-2-sql
Web server (backend only) for SQL chatbot that converts text into SQL queries.

This code sample showcases the backend server for a Text-to-SQL chatbot powered by a Bedrock Agent. It converts natural language queries into SQL statements, enabling users to interact with complex databases using straightforward English prompts. The backend facilitates seamless access to data insights by translating user inquiries into precise SQL queries and fetching relevant results from the database.

Leveraging AWS Bedrock's agent technology, this code base provides a complete setup for deploying and testing a Text-to-SQL chatbot integrated with AWS services. It encompasses all the necessary files to create and operate a chatbot that understands and processes natural language, executes SQL queries, and retrieves database schemas.

When a query’s response is too lengthy, the chatbot generates a link for users to download the data in a report format, making it easier to review and analyze. The infrastructure is built using AWS CDK, ensuring flexibility to work with any dataset as long as it is stored in an S3 bucket. This repository offers a comprehensive solution for developing a user-friendly data retrieval system, simplifying interactions with complex database structures.

## Before You Begin

Ensure you have the following:

### AWS Account Permissions

- **Create and manage IAM roles and policies.**
- **Create and invoke AWS Lambda functions.**
- **Create, read, and write to Amazon S3 buckets.**
- **Access and manage Amazon Bedrock agents and models.**
- **Create and manage Amazon Glue databases and crawlers.**
- **Execute queries and manage workspaces in Amazon Athena.**
- **Access Amazon Bedrock foundation models, specifically Anthropic’s Claude 3 Sonnet model for this solution.**

### Local Setup

- **Python and Jupyter Notebooks installed.**
- **AWS CLI installed and configured.**

### For AWS SageMaker

- **Ensure your domain has the required permissions.**
- **Use the Data Science 3.0 kernel in SageMaker Studio.**


## Installation
```
git clone https://github.com/SreeyuR/text-2-sql.git
export AWS_PROFILE=<YOUU_AWS_PROFILE>
python3.9 -m venv .venv
source .venv/bin/activate
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
chmod +x setup.sh
./setup.sh
```
- Upload `vehicle-data` with the same directory structure to S3
- Create an S3 bucket with the name `athena-destination-store-texttosql`

## Deployment
```
cdk deploy --profile XXX --context--context region=us-east-1
```

## Usage
"What are 10 models and types of vehicles available in Illinois?"

## Cleaning Up
```
cdk destroy --profile XXX --context zip_file_name=EV_WA.zip --context region=us-east-1
```



