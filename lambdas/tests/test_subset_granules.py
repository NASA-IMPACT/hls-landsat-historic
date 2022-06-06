import os
from unittest.mock import MagicMock, patch

import pytest

from lambdas.subset_granules import (
    get_date_range,
    handler,
    process_payload,
    set_last_date,
)


class SSM_Client:
    def get_parameter(self, Name):
        return {"Parameter": {"Value": "2021-07-01 00:00:00"}}


@pytest.fixture
def ssm_client():
    return SSM_Client()


@patch("lambdas.subset_granules.publish_message")
def test_process_payload_granules(publish):
    granules = '{"landsat_product_id":\
        "LC08_L1TP_218002_20220327_20220329_02_T1"}\n\
    {"landsat_product_id":\
        "LC08_L1TP_202030_20220327_20220330_02_T1"}\n\
        BytesScanned 1'
    encoded = granules.encode("utf-8", "strict")
    response = {"Payload": [{"Records": {"Payload": encoded}}]}
    process_payload(response)
    assert publish.call_count == 2


@patch("lambdas.subset_granules.publish_message")
def test_process_payload_granules_split_events(publish):
    granules_prefix = '{"landsat_product_id":\
        "LC08_L1TP_218002_20220327_20220329_02_T1"}\n\
    {"landsat_product_id":'
    granules_suffix = '"LC08_L1TP_202030_20220327_20220330_02_T1"}\n'
    encoded_prefix = granules_prefix.encode("utf-8", "strict")
    encoded_suffix = granules_suffix.encode("utf-8", "strict")
    response = {
        "Payload": [
            {"Records": {"Payload": encoded_prefix}},
            {"Records": {"Payload": encoded_suffix}},
        ]
    }
    process_payload(response)
    assert publish.call_count == 2


@patch("lambdas.subset_granules.publish_message")
def test_process_payload_stats(publish, capfd):
    response = {
        "Payload": [{"Stats": {"Details": {"BytesScanned": 1, "BytesProcessed": 2}}}]
    }
    process_payload(response)
    assert publish.call_count == 0

    out, err = capfd.readouterr()
    assert out == "Stats details bytesScanned: \n1\nStats details bytesProcessed: \n2\n"


@pytest.mark.parametrize(
    "days_range,expected",
    [
        (
            30,
            {
                "start_date": "2021-06-02 00:00:00",
                "end_date": "2021-07-01 00:00:00",
                "new_last_date": "2021-06-01 00:00:00",
            },
        ),
        (
            15,
            {
                "start_date": "2021-06-17 00:00:00",
                "end_date": "2021-07-01 00:00:00",
                "new_last_date": "2021-06-16 00:00:00",
            },
        ),
    ],
)
def test_get_date_range(ssm_client, days_range, expected):
    assert get_date_range(ssm_client, "name", days_range) == expected


def test_set_last_date():
    ssm_client = MagicMock()
    parameter_name = "name"
    last_date = "2021-06-16 00:00:00"
    set_last_date(ssm_client, parameter_name, last_date)
    ssm_client.put_parameter.assert_called_with(
        Name=parameter_name, Value=last_date, Overwrite=True
    )


bucket = "bucket"
key = "key"
last_date = "last_date"


@patch("lambdas.subset_granules.process_payload")
@patch("lambdas.subset_granules.get_date_range")
@patch("lambdas.subset_granules.boto3")
@patch("lambdas.subset_granules.select_granules")
@patch.dict(
    os.environ,
    {
        "BUCKET": bucket,
        "KEY": key,
        "LAST_DATE_PARAMETER_NAME": last_date,
        "DAYS_RANGE": "30",
        "LANDSAT_PLATFORM": "8",
        "TOPIC_ARN": "topic_arn",
    },
)
def test_handler(select_granules, boto3, get_date_range, *args):
    start_date = "2021-08-13 00:00:00"
    end_date = "2021-08-14 00:00:00"
    new_last_date = "2021-08-12 00:00:00"
    ls_platform = "8"
    handler({"start_date": start_date, "end_date": end_date}, {})
    select_granules.assert_called_with(start_date, end_date, ls_platform, bucket, key)
    ssm_client = MagicMock()
    boto3.client.return_value = ssm_client

    get_date_range.return_value = {
        "start_date": "2021-07-14 00:00:00",
        "end_date": start_date,
        "new_last_date": new_last_date,
    }
    handler({}, {})
    boto3.client.assert_called_with("ssm")
    select_granules.assert_called_with(
        "2021-07-14 00:00:00", start_date, ls_platform, bucket, key
    )
