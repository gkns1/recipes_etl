import os
import logging
from pyspark.sql import SparkSession
import json
import requests


def load_config(config=None, config_manual=None):
    config_dict = config
    if config is None and config_manual is None:
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config = os.path.join(os.path.join(root_dir, 'config'), 'config.json')
        try:
            with open(config, 'r') as config_file:
                config_dict = json.loads(config_file.read())
                config_file.close()
        except Exception as e:
            print(e)
    elif config_manual is not None:
        try:
            config_dict = json.loads(config_manual)
        except Exception as e:
            print("Can't load the config! {0}".format(e))
    elif config is not None:
        try:
            with open(config, 'r') as config_file:
                config_dict = json.loads(config_file.read())
                config_file.close()
        except Exception as e:
            print(e)

    return config_dict


def start_session(mode, app_name='recipes_etl', files=None):
    # files are a placeholder in case we want to supply files to spark with --py-files
    spark = None  # to make the linter happy
    if files is None:
        files = []

    if mode == 'standalone':
        master = 'local[*]'
        env = 'local'
    else:
        master = 'yarn'
        env = 'prod'

    if mode != 'yarn' and mode != 'standalone':
        print("The only acceptable inputs are 'yarn' and 'standalone'.")

    elif mode == 'yarn':
        spark_warehouse = os.path.abspath('spark-warehouse')
        try:
            print("the current mode is yarn, trying to get or create a spark session.")
            spark = SparkSession \
                .builder \
                .appName(app_name) \
                .master(master) \
                .config("spark.sql.warehouse.dir", spark_warehouse) \
                .enableHiveSupport() \
                .getOrCreate()
            print("Session created.")
        except Exception as e:
            print("Could not create the Spark session. Exception: {0}".format(e))
            exit()

    elif mode == 'standalone':
        try:
            print("the current mode is standalone, trying to get or create a spark session.")
            spark = SparkSession \
                .builder \
                .appName(app_name) \
                .master(master) \
                .getOrCreate()
            print("Spark session created.")
        except Exception as e:
            print("Could not create the Spark session. Exception: {0}".format(e))
            exit()
    logger = logging.getLogger('py4j')

    return spark, env, logger


class Handler(object):
    def __init__(self, logger, config=load_config()):
        self.logger = logger
        self.config_dict = config

    def _report_discord(self, message, webhook_manual=''):
        if self.config_dict is not None:
            webhook = self.config_dict['webhook'].get('path')
        else:
            webhook = None

        if webhook is None and webhook_manual != '':
            webhook = webhook_manual
        elif webhook is None and webhook_manual == '':
            print("No webhook found. Discord messages will not be sent!")

        message = {'content': message}

        if webhook is not None:
            # print("sending the message to discord webhook...")
            requests.post(webhook, message)
        return "Discord message sent! {0}".format(message)

    def info(self, message):
        self.logger.info(message)

        return self._report_discord(message)

    def warn(self, message):
        self.logger.warning(message)

        return self._report_discord(message)

    def error(self, message):
        self.logger.error(message)

        return self._report_discord(message)