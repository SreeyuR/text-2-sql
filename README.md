# text-2-sql
Web server (backend only) for SQL chatbot that converts text into SQL queries.


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



