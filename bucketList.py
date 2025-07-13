import yaml
import os
import boto3

# === Step 1: Load YAML config ===
with open("Monthly_Report_AWS.yaml", "r") as file:
    config = yaml.safe_load(file)

# === Step 2: Set environment variables dynamically ===
os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]
os.environ["AWS_DEFAULT_REGION"] = config["AWS_DEFAULT_REGION"]

# Optional: Set RDS or RITIS env vars too
os.environ["RDS_HOST"] = config["RDS_HOST"]
os.environ["RDS_USERNAME"] = config["RDS_USERNAME"]
os.environ["RDS_PASSWORD"] = config["RDS_PASSWORD"]
os.environ["RDS_DATABASE"] = config["RDS_DATABASE"]
os.environ["RITIS_KEY"] = config["RITIS_KEY"]

# === Step 3: Initialize boto3 using env vars (auto-detected) ===
s3 = boto3.client("s3")

# === Test ===
response = s3.list_buckets()
print("Available Buckets:")
for b in response["Buckets"]:
    print(" -", b["Name"])

# === Step 4: List buckets using boto3 ===
response = s3.list_buckets()
print("Available Buckets:")
for b in response["Buckets"]:
    print(" -", b["Name"])