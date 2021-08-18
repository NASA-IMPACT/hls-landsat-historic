import boto3
s3 = boto3.client("s3")

start_date = "2021/08/13"
end_date = "2021/08/14"

r = s3.select_object_content(
    Bucket="landsat-inventory",
    Key="inventory_product_list.json.gz",
    ExpressionType="SQL",
    Expression="select * from S3Object[*].available_products[*] s where"
    " TO_TIMESTAMP(s.date_acquired, 'y/MM/dd') BETWEEN"
    f" TO_TIMESTAMP('{start_date}', 'y/MM/dd') AND"
    f" TO_TIMESTAMP('{end_date}', 'y/MM/dd') AND"
    " s.processing_level = '1' AND"
    " s.sensor_id = 'OLI_TIRS' AND"
    " s.spacecraft_id = 'LANDSAT_8'",
    InputSerialization={
        "JSON": {
            "Type": "DOCUMENT"
        },
        "CompressionType": "GZIP",
    },
    OutputSerialization={"JSON": {}},
)

for event in r["Payload"]:
    if "Records" in event:
        records = event["Records"]["Payload"].decode("utf-8")
        print(records)
    elif "Stats" in event:
        statsDetails = event["Stats"]["Details"]
        print("Stats details bytesScanned: ")
        print(statsDetails["BytesScanned"])
        print("Stats details bytesProcessed: ")
        print(statsDetails["BytesProcessed"])
