import base64
import json
import boto3
import decimal


def lambda_handler(event, context):
    item = None
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('stock_nasdaq')
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in
                           event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    clean_deserialized_data = deserialized_data[0]['stock']

    with table.batch_writer() as batch_writer:
        for item in clean_deserialized_data:
            symbol = item['symbol']
            shortname = item['shortName']
            country = item['country']
            currency = item['currency']
            current_price = item['currentPrice']
            current_ratio = item['currentRatio']
            open_price = item['open']
            previous_close = item['previousClose']
            day_high = item['dayHigh']
            day_low = item['dayLow']
            date = item['date']

            batch_writer.put_item(
                Item={
                    'symbol': symbol,
                    'shortname': shortname,
                    'country': country,
                    'currency': currency,
                    'current_price': decimal.Decimal(str(current_price)),
                    'current_ratio': decimal.Decimal(str(current_ratio)),
                    'open_price': decimal.Decimal(str(open_price)),
                    'previous_close': decimal.Decimal(str(previous_close)),
                    'day_high': decimal.Decimal(str(day_high)),
                    'day_low': decimal.Decimal(str(day_low)),
                    'date': date,
                }
            )
