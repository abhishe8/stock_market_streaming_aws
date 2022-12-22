import yfinance as yf
import json
import urllib.error
from pathlib import Path
from pytz import timezone
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import logging
from logging import Formatter
import time

from constants import LOGS_FILE_NAME
from script.yaml_reader import read_yaml_file
from script.kinesis_stream_file import data_stream

BASE_DIRECTORY = str(Path(__file__).parent)
RESOURCES_DIRECTORY = str(Path(BASE_DIRECTORY).joinpath('resource')).strip()
PROPERTIES_DIRECTORY = str(Path(RESOURCES_DIRECTORY).joinpath('properties.yml')).strip()
TEMP_DIRECTORY = str(Path(RESOURCES_DIRECTORY).joinpath('stock_temp.json')).strip()
FILE_PATH_LOG = str(Path(RESOURCES_DIRECTORY).joinpath('logs')).strip()
FILE_NAME = LOGS_FILE_NAME + '.txt'
FILE_NAME_PATH = str(Path(FILE_PATH_LOG).joinpath(FILE_NAME)).strip()


def logger_function():
    """
    This function is used to initiate the logging mechanism
    :return logger: logging object
    """
    logger = logging.getLogger('logger')
    # create handler
    handler = TimedRotatingFileHandler(filename=FILE_NAME_PATH, when='M', interval=10,
                                       backupCount=55, encoding='utf-8', delay=False)
    # create formatter and add to handler
    formatter = Formatter(fmt='%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    # add the handler to named logger
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def read_temp_file(temp_dir):
    with open(temp_dir) as json_file:
        json_data = json.load(json_file)
    return json_data


def get_call(x, logger):
    flag = False
    query = {}
    try:
        query = yf.Ticker(x).info
        flag = True
    except urllib.error.URLError as e:
        logger.info("Error while fetching the stock details : symbol {0}".format(str(x)))
        logger.info(e)
    except Exception as err:
        logger.info("Error while fetching the stock details : symbol {0}".format(str(x)))
        logger.info(err)
    return query, flag


def stock_details(yaml_data, logger):
    for x in yaml_data['stock_name'].keys():
        print(str(x))
        stock_dic = {"stock": [], "time": ""}
        query, flag = get_call(x, logger)
        if flag:
            data_stock = read_temp_file(TEMP_DIRECTORY)
            time_est = datetime.now(timezone("US/Eastern")).strftime('%Y-%m-%d %H:%M:%S')
            data_stock['symbol'] = query['symbol']
            data_stock['shortName'] = query['shortName']
            data_stock['country'] = query['country']
            data_stock['currency'] = query['currency']
            data_stock['currentPrice'] = query['currentPrice']
            data_stock['currentRatio'] = query['currentRatio']
            data_stock['open'] = query['open']
            data_stock['previousClose'] = query['previousClose']
            data_stock['dayHigh'] = query['dayHigh']
            data_stock['dayLow'] = query['dayLow']
            data_stock['date'] = str(time_est)
            stock_dic['stock'].append(data_stock)
            # send data to kinesis
            current_time = time.time()
            stock_dic['time'] = current_time
            with open('test_1.json', 'a')as file:
                json.dump(stock_dic, file, indent=4)
            data_stream(stock_dic, logger)
        else:
            logger.info("Error while fetching the results. symbol {0}".format(x))


def main():
    logger = logger_function()
    yaml_data = read_yaml_file(PROPERTIES_DIRECTORY)
    stock_details(yaml_data, logger)


if __name__ == "__main__":
    main()
