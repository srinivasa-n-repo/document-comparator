import os
import boto3
from botocore.config import Config

REGION = "ap-south-1"
SCANNED_MAX_WORKERS = 5

bucket = os.environ['bucket']
db_port = os.environ['db_port']

_BEDROCK_RETRY = Config(retries={"max_attempts": 5, "mode": "standard"})

# Module-level clients — reused across warm Lambda invocations
lambda_client = boto3.client('lambda', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)
bedrock_client = boto3.client("bedrock-runtime", region_name=REGION)
