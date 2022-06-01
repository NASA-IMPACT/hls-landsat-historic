import datetime
import json
import os
import re
from typing import Any, Dict

import boto3


def landsat_parse_scene_id(sceneid):
    """
    Parse Landsat-8 scene id.
    Author @perrygeo - http://www.perrygeo.com
    Attributes
    ----------
        sceneid : str
            Landsat sceneid.
    Returns
    -------
        out : dict
            dictionary with metadata constructed from the sceneid.
    """

    precollection_pattern = (
        r"^L"
        r"(?P<sensor>\w{1})"
        r"(?P<satellite>\w{1})"
        r"(?P<path>[0-9]{3})"
        r"(?P<row>[0-9]{3})"
        r"(?P<acquisitionYear>[0-9]{4})"
        r"(?P<acquisitionJulianDay>[0-9]{3})"
        r"(?P<groundStationIdentifier>\w{3})"
        r"(?P<archiveVersion>[0-9]{2})$"
    )

    collection_pattern = (
        r"^L"
        r"(?P<sensor>\w{1})"
        r"(?P<satellite>\w{2})"
        r"_"
        r"(?P<processingCorrectionLevel>\w{4})"
        r"_"
        r"(?P<path>[0-9]{3})"
        r"(?P<row>[0-9]{3})"
        r"_"
        r"(?P<acquisitionYear>[0-9]{4})"
        r"(?P<acquisitionMonth>[0-9]{2})"
        r"(?P<acquisitionDay>[0-9]{2})"
        r"_"
        r"(?P<processingYear>[0-9]{4})"
        r"(?P<processingMonth>[0-9]{2})"
        r"(?P<processingDay>[0-9]{2})"
        r"_"
        r"(?P<collectionNumber>\w{2})"
        r"_"
        r"(?P<collectionCategory>\w{2})$"
    )

    for pattern in [collection_pattern, precollection_pattern]:
        match = re.match(pattern, sceneid, re.IGNORECASE)
        if match:
            meta: Dict[str, Any] = match.groupdict()
            break

    meta["scene"] = sceneid
    if meta.get("acquisitionJulianDay"):
        date = datetime.datetime(
            int(meta["acquisitionYear"]), 1, 1
        ) + datetime.timedelta(int(meta["acquisitionJulianDay"]) - 1)

        meta["date"] = date.strftime("%Y-%m-%d")
    else:
        meta["date"] = "{}-{}-{}".format(
            meta["acquisitionYear"], meta["acquisitionMonth"], meta["acquisitionDay"]
        )

    collection = meta.get("collectionNumber", "")
    if collection != "":
        collection = "c{}".format(int(collection))

    return meta


def build_landsat_s3_path(granule):
    scene_id = granule["landsat_product_id"]
    if len(scene_id) < 40:
        pass
    meta = landsat_parse_scene_id(scene_id)
    if meta["sensor"] != "C":
        raise NameError("USGS Landsat Scene sensor is not OLI-TIRS.\n")
    ls_s3_path = (
        f's3://usgs-landsat/collection{meta["collectionNumber"]}'
        f'/level-{meta["processingCorrectionLevel"][1:2]}'
        f'/standard/oli-tirs/{meta["acquisitionYear"]}'
        f'/{meta["path"]}/{meta["row"]}/{scene_id}/'
    )
    granule["s3_location"] = ls_s3_path
    return


def select_granules(start_date, end_date, ls_platform, bucket, key):
    date_format = "%Y-%m-%d %H:%M:%S"
    dt_start_date = datetime.datetime.strptime(start_date, date_format)
    dt_end_date = datetime.datetime.strptime(end_date, date_format)
    assert dt_start_date < dt_end_date
    s3 = boto3.client("s3")
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType="SQL",
        Expression="select * from S3Object s where"
        " TO_TIMESTAMP(s.date_acquired,'yyyy-MM-dd HH:mm:ss') BETWEEN"
        f" TO_TIMESTAMP('{start_date}','yyyy-MM-dd HH:mm:ss') AND"
        f" TO_TIMESTAMP('{end_date}','yyyy-MM-dd HH:mm:ss') AND"
        " s.processing_level_short = '1' AND"
        " s.sensor_id = 'OLI_TIRS' AND"
        " s.landsat_product_id LIKE '%_T1' AND"
        f" s.landsat_product_id LIKE 'LC0{ls_platform}_%' AND"
        " s.collection_category = 'T1'",
        InputSerialization={
            "CSV": {"FileHeaderInfo": "Use"},
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
        "landsat_product_id": granule["landsat_product_id"],
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
                    build_landsat_s3_path(granule_dict)
                    publish_message(granule_dict)
                except json.decoder.JSONDecodeError:
                    print("Stream event split")
                    if len(split_prefix) > 0:
                        granule_concat = split_prefix + granule
                        split_prefix = ""
                        granule_dict = json.loads(granule_concat)
                        build_landsat_s3_path(granule_dict)
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
    date_format = "%Y-%m-%d %H:%M:%S"
    response = ssm_client.get_parameter(Name=parameter_name)
    print(response)
    last_date = response["Parameter"]["Value"]
    end_date = datetime.datetime.strptime(last_date, date_format)
    start_date = end_date - datetime.timedelta(days=days_range - 1)
    new_last_date = start_date - datetime.timedelta(days=1)

    return {
        "start_date": start_date.strftime(date_format),
        "end_date": end_date.strftime(date_format),
        "new_last_date": new_last_date.strftime(date_format),
    }


def set_last_date(ssm_client, parameter_name, last_date):
    ssm_client.put_parameter(Name=parameter_name, Value=last_date, Overwrite=True)


def handler(event, context):
    """
    Query Historical Landsat gzipped json file and publish SNS message for
    returned granules.

    Parameters
    ----------
    event : dict
        Structure of {"start_date": "2021-06-02 00:00:00", "2021-07-01 23:59:00"}
    """
    bucket = os.getenv("BUCKET")
    key = os.getenv("KEY")
    parameter_name = os.getenv("LAST_DATE_PARAMETER_NAME")
    days_range = os.getenv("DAYS_RANGE")
    ls_platform = os.getenv("LANDSAT_PLATFORM")
    try:
        start_date = event["start_date"]
        end_date = event["end_date"]
        granules = select_granules(start_date, end_date, ls_platform, bucket, key)
        process_payload(granules)
    except KeyError:
        ssm_client = boto3.client("ssm")
        date_range = get_date_range(ssm_client, parameter_name, int(days_range))
        start_date = date_range["start_date"]
        end_date = date_range["end_date"]
        granules = select_granules(start_date, end_date, ls_platform, bucket, key)
        process_payload(granules)
        set_last_date(ssm_client, parameter_name, date_range["new_last_date"])
