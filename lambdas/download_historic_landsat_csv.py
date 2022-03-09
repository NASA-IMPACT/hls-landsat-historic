import datetime
import gzip
import json
import os
import zipfile

import boto3


def handler(event, context) -> None:
    dl_bucket = os.getenv("BUCKET")
    inv_key = os.getenv("KEY")
    copy_bucket = os.getenv("COPY_BUCKET")
    copy_key = "inventory_product_list.zip"
    copy_source = {
        "Bucket": copy_bucket,
        "Key": copy_key,
    }

    s3_client = boto3.client("S3")

    s3_client.copy_object(
        Bucket=dl_bucket, Key=inv_key, RequestPayer="requester", CopySource=copy_source
    )
