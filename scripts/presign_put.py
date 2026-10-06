import os
import boto3

session = boto3.Session(profile_name="default")
s3 = session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
print(s3.generate_presigned_url(
    "put_object",
    Params={"Bucket": os.environ["LAB_BUCKET_NAME"], "Key": "bronze/upload.csv"},
    ExpiresIn=300,
))
