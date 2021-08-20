import os
import boto3


def select_granules(start_date, end_date, bucket, key):
    s3 = boto3.client("s3")
    granules = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType="SQL",
        Expression="select * from S3Object[*].available_products[*] s where"
        " TO_TIMESTAMP(s.date_acquired, 'y/MM/dd') BETWEEN"
        f" TO_TIMESTAMP('{start_date}', 'y/MM/dd') AND"
        f" TO_TIMESTAMP('{end_date}', 'y/MM/dd') AND"
        " s.processing_level = '1' AND"
        " s.sensor_id = 'OLI_TIRS' AND"
        " s.spacecraft_id = 'LANDSAT_8' AND"
        " s.product_id LIKE '%_T1'",
        InputSerialization={
            "JSON": {
                "Type": "DOCUMENT"
            },
            "CompressionType": "GZIP",
        },
        OutputSerialization={"JSON": {}},
    )
    return granules


def process_payload(granules):
    for event in granules["Payload"]:
        if "Records" in event:
            records = event["Records"]["Payload"].decode("utf-8")
            print(records)
        elif "Stats" in event:
            statsDetails = event["Stats"]["Details"]
            print(statsDetails["BytesScanned"])
            print("Stats details bytesProcessed: ")
            print(statsDetails["BytesProcessed"])


def handler(event, context):
    bucket = os.getenv("BUCKET")
    key = os.getenv("KEY")
    start_date = event["start_date"]
    end_date = event["end_date"]
    granules = select_granules(start_date, end_date, bucket, key)
    process_payload(granules)


#  test_event = {
    #  "start_date": "2021/08/13",
    #  "end_date": "2021/08/14"
#  }
#  handler(test_event)
