"""
Utils function.
taken from https://github.com/hellofresh/spark-testing-base/blob/master/sparktestingbase/utils.py
"""
import sys
import os
import logging
from pyspark.sql import SparkSession
import json
from glob import glob
import requests


def add_pyspark_path_if_needed():
    """Add PySpark to the library path based on the value of SPARK_HOME if
    pyspark is not already in our path"""
    try:
        from pyspark import context
    except ImportError:
        # We need to add PySpark, try findspark if we can but it has an
        # undeclared IPython dep.
        try:
            import findspark
            findspark.init()
        except ImportError:
            add_pyspark_path()


def add_pyspark_path():
    """Add PySpark to the library path based on the value of SPARK_HOME."""

    try:
        spark_home = os.environ['SPARK_HOME']

        sys.path.append(os.path.join(spark_home, 'python'))
        py4j_src_zip = glob(os.path.join(spark_home, 'python',
                                         'lib', 'py4j-*-src.zip'))
        if len(py4j_src_zip) == 0:
            raise ValueError('py4j source archive not found in %s'
                             % os.path.join(spark_home, 'python', 'lib'))
        else:
            py4j_src_zip = sorted(py4j_src_zip)[::-1]
            sys.path.append(py4j_src_zip[0])
    except KeyError:
        print("""SPARK_HOME was not set. please set it. e.g.
        SPARK_HOME='/home/...' ./bin/pyspark [program]""")
        exit(-1)
    except ValueError as e:
        print(str(e))
        exit(-1)


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


"""
Added by KG, not in the original code.
"""


def load_config(config=None, config_manual=''):
    config_dict = config
    if config is None and config_manual == '':
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config = os.path.join(os.path.join(root_dir, 'config'), 'config.json')
        try:
            with open(config) as config_file:
                config_dict = json.loads(config_file.read())
                config_file.close()
        except Exception as e:
            print(e)
    elif config_manual != '':
        try:
            config_dict = json.loads(config_manual)
        except Exception as e:
            print("Can't load the config! {0}".format(e))

    return config_dict


def start_session(mode, app_name='hellofresh_th_etl', files=None):
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
            add_pyspark_path_if_needed()
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

    def info(self, message):
        self.logger.info(message)
        self._report_discord(message)

    def warn(self, message):
        self.logger.warn(message)
        self._report_discord(message)

    def error(self, message):
        self.logger.error(message)
        self._report_discord(message)