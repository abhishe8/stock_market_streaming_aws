import boto3
import json
from stock_producer.constants import kinesis_stream_name


kinesis = boto3.client("kinesis")


def data_stream(stock_json, logger):
    response = kinesis.put_record(
        StreamName=kinesis_stream_name,
        Data=json.dumps(stock_json),
        PartitionKey=str(stock_json['stock'][0]['date'])
    )
    logger.info(response)

