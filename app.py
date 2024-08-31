import time
import random
import string

import cdk

from aws_cdk import App, Stack, Duration, aws_lambda as lambda_, aws_glue as glue, aws_iam as iam, aws_s3 as s3
from cdklabs.generative_ai_cdk_constructs.bedrock import (
    Agent, ApiSchema, BedrockFoundationModel, AgentActionGroup, ActionGroupExecutor
)

from agent_instruction_generator import generate_instruction
from claude_3 import invoke_claude_3_with_text
from Prep_Data import get_s3_data_sources

ENV = 'poc'


class MyStack(Stack):
    def __init__(self, scope: App, id: str, region: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        account_id = cdk.Stack.of(self).account
        foundation_model = BedrockFoundationModel(
            'anthropic.claude-3-sonnet-20240229-v1:0',
            supports_agents=True
        )

        # S3 Buckets
        bucket_name = "vehicle-data"
        athena_result_loc = "athena-destination-store-texttosql"

        schema_bucket = s3.Bucket.from_bucket_arn(self, "SchemaBucket", f"arn:aws:s3:::{bucket_name}")
        athena_output_bucket = s3.Bucket.from_bucket_arn(self, "AthenaOutputBucket",
                                                         f"arn:aws:s3:::{athena_result_loc}")

        # IAM Role for Glue
        glue_role = iam.Role(self, "GlueRole",
                             assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                             managed_policies=[
                                 iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                                 iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
                             ])

        # Glue Database
        glue_database_name = "vehicle-data"
        glue.CfnDatabase(self, "GlueDatabase",
                         catalog_id=account_id,
                         database_input=glue.CfnDatabase.DatabaseInputProperty(name=glue_database_name)
                         )

        # Glue Crawler
        glue.CfnCrawler(
            self,
            "AI_Chatbot_Crawler",
            role=glue_role.role_arn,
            database_name=glue_database_name,
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(0/1 * * * ? *)"),
            targets={"s3Targets": get_s3_data_sources(schema_bucket.bucket_name)}
        )

        # Lambda Function for Action Group
        action_group_function = lambda_.Function(
            self,
            "ActionGroupFunction",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.lambda_handler",
            code=lambda_.Code.from_asset("./lambda/agent/"),
            environment={
                'outputLocation': f's3://{athena_result_loc}/',
                'glue_database_name': glue_database_name,
                'region': region,
                'bucket_name': schema_bucket.bucket_name
            },
            timeout=Duration.minutes(5),
            memory_size=1024,
        )

        # Attach IAM policies to Lambda role
        glue_permissions = [
            "glue:StartJobRun", "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable",
            "glue:GetTables", "glue:BatchGetPartition", "glue:GetPartition", "glue:GetPartitions",
            "glue:BatchCreatePartition", "glue:CreatePartition", "glue:DeletePartition",
            "glue:UpdatePartition", "glue:BatchDeletePartition"
        ]

        s3_permissions = [
            "s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:CreateBucket", "s3:GetBucketLocation"
        ]

        action_group_function.role.add_to_policy(iam.PolicyStatement(
            actions=glue_permissions,
            resources=[
                f"arn:aws:glue:{region}:{account_id}:catalog",
                f"arn:aws:glue:{region}:{account_id}:database/{glue_database_name}",
                f"arn:aws:glue:{region}:{account_id}:table/{glue_database_name}/*",
            ]
        ))

        action_group_function.role.add_to_policy(iam.PolicyStatement(
            actions=s3_permissions,
            resources=[
                schema_bucket.bucket_arn, f"{schema_bucket.bucket_arn}/*",
                athena_output_bucket.bucket_arn, f"{athena_output_bucket.bucket_arn}/*"
            ]
        ))

        action_group_function.role.add_to_policy(iam.PolicyStatement(
            actions=[
                "athena:StartQueryExecution", "athena:GetQueryExecution",
                "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup"
            ],
            resources=[f"arn:aws:athena:{region}:{account_id}:workgroup/primary"]
        ))

        # Generate Agent Instruction
        instruction_text = generate_instruction(database_name=glue_database_name, data_context=None, env=ENV)
        question = (
            "Craft a comprehensive and cohesive paragraph instruction for the Bedrock agent, ensuring the instruction "
            "text includes all 7 contextual details and examples provided. The instruction should be detailed, precise "
            "with a maximum length of 4000 characters. Clearly outline the agent's tasks and how it should interact "
            "with users, incorporating the provided contextual details and examples with minimal changes. Avoid any "
            "introductory phrases such as 'Here is your...'.\n\n"
            f"<Contextual details and examples>\n{instruction_text}\n</Contextual details and examples>"
        )
        agent_instruction = invoke_claude_3_with_text(question)
        print(agent_instruction)

        # Create a unique agent ID
        def generate_unique_id(prefix="agent"):
            timestamp = int(time.time())
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
            return f"{prefix}-{timestamp}-{random_suffix}"

        agent_id = generate_unique_id("sql-agent")

        # Create Agent and Action Group
        api_schema = ApiSchema.from_asset("./text_to_sql_openapi_schema.json")
        agent = Agent(
            self,
            agent_id,
            foundation_model=foundation_model,
            instruction=agent_instruction,
            description="Agent for performing SQL queries.",
            should_prepare_agent=True,
            name='SQL-Agent-CDK',
        )

        action_group = AgentActionGroup(
            self,
            "MyActionGroup",
            action_group_name="QueryAthenaActionGroup",
            description="Actions for getting the database schema and querying the Athena database for sample data or "
                        "final query.",
            action_group_executor=ActionGroupExecutor(lambda_=action_group_function),
            action_group_state="ENABLED",
            api_schema=api_schema
        )

        agent.add_action_group(action_group)

        # IAM Role for Agent
        agent_role = iam.Role(self, "AgentRole", assumed_by=iam.ServicePrincipal("bedrock.amazonaws.com"))
        agent_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[schema_bucket.bucket_arn, f"{schema_bucket.bucket_arn}/*"]
        ))

        action_group_function.add_permission(
            "AllowBedrockInvoke",
            principal=iam.ServicePrincipal("bedrock.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:bedrock:{region}:{account_id}:agent/*"
        )

        print("Agent ID:", agent.agent_id, agent.agent_arn)


app = App()
region = app.node.try_get_context("region")
MyStack(app, "text-2-sql", region=region)
app.synth()
