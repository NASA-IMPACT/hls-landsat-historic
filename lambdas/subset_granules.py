import datetime
import json
import os

import boto3


def select_granules(start_date, end_date, bucket, key):
    s3 = boto3.client("s3")
    response = s3.select_object_content(
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
            "JSON": {"Type": "DOCUMENT"},
            "CompressionType": "GZIP",
        },
        OutputSerialization={"JSON": {}},
    )
    return response


def publish_message(granule):
    print(granule)
    topic_arn = os.getenv("TOPIC_ARN")
    sns = boto3.client("sns")
    message = {
        "landsat_product_id": granule["product_id"],
        "s3_location": granule["s3_location"],
    }
    message_string = json.dumps(message)
    sns.publish(
        TopicArn=topic_arn,
        Message=message_string,
    )


def process_payload(response):
    split_prefix = ""
    for event in response["Payload"]:
        if "Records" in event:
            records = event["Records"]["Payload"].decode("utf-8")
            granules = records.split("\n")
            for granule in granules:
                try:
                    granule_dict = json.loads(granule)
                    publish_message(granule_dict)
                except json.decoder.JSONDecodeError:
                    print("Stream event split")
                    if len(split_prefix) > 0:
                        granule_concat = split_prefix + granule
                        split_prefix = ""
                        granule_dict = json.loads(granule_concat)
                        publish_message(granule_dict)
                    else:
                        split_prefix = granule
        elif "Stats" in event:
            statsDetails = event["Stats"]["Details"]
            print("Stats details bytesScanned: ")
            print(statsDetails["BytesScanned"])
            print("Stats details bytesProcessed: ")
            print(statsDetails["BytesProcessed"])


def get_date_range(ssm_client, parameter_name, days_range):
    date_format = "%Y/%m/%d"
    last_date = ssm_client.get_parameter(Name=parameter_name)
    end_date = datetime.datetime.strptime(last_date, date_format)
    start_date = end_date - datetime.timedelta(days=days_range)
    return {
        "start_date": start_date.strftime(date_format),
        "end_date": end_date.strftime(date_format),
    }


def set_last_date(ssm_client, parameter_name, last_date):
    ssm_client.put_parameter(Name=parameter_name, Value=last_date)


def handler(event, context):
    """
    Query Historical Landsat gzipped json file and publish SNS message for
    returned granules.

    Parameters
    ----------
    event : dict
        Structure of {"start_date": "2021/08/13", "end_date": "2021/08/14"}
    """
    bucket = os.getenv("BUCKET")
    key = os.getenv("KEY")
    parameter_name = os.getenv("LAST_DATE_PARAMETER_NAME")
    days_range = os.getenv("DAYS_RANGE")
    try:
        start_date = event["start_date"]
        end_date = event["end_date"]
        granules = select_granules(start_date, end_date, bucket, key)
        process_payload(granules)
    except KeyError:
        ssm_client = boto3.client("ssm")
        date_range = get_date_range(ssm_client, parameter_name, int(days_range))
        start_date = date_range["start_date"]
        end_date = date_range["end_date"]
        print(start_date)
        granules = select_granules(start_date, end_date, bucket, key)
        process_payload(granules)
        set_last_date(ssm_client, parameter_name, start_date)
