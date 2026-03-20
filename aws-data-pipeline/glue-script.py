import sys
import json
import boto3
import pandas as pd

s3 = boto3.client('s3')

bucket = "de-project-ngugi"
key = "raw/crypto.json"

obj = s3.get_object(Bucket=bucket, Key=key)
data = json.loads(obj['Body'].read())

df = pd.DataFrame(data)

df = df[[
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume"
]]

df.columns = [
    "coin_id",
    "symbol",
    "name",
    "price",
    "market_cap",
    "volume"
]

# Save processed data back to S3
df.to_csv(f"s3://{bucket}/processed/crypto_clean.csv", index=False)
