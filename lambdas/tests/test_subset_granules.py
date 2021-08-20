import os
from unittest.mock import patch

from lambdas.subset_granules import process_payload


@patch("lambdas.subset_granules.publish_message")
def test_process_payload_granules(publish):
    granules = '{"product_id": "1"}\n{"product_id": "2"}\nBytesScanned 1'
    encoded = granules.encode("utf-8", "strict")
    response = {"Payload": [{"Records": {"Payload": encoded}}]}
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
