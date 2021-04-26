import unittest
from hellofresh_takehome.etl import *
from hellofresh_takehome import utils
import json


class SparkETLTests(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        super(SparkETLTests, cls).setUpClass()
        cls.config = """{
                "webhook": {
                    "path": "https://discord.com/api/webhooks/834903224410439751/mEhxBKfl8oxsm3Z-9Igt4vFUyxaWqoR4RNLtAcxR7oF3UA0FamXZFCXa1n_OXffSTOh4"},
                "extract": {"path": "data/test_data.json",
                            "table": "recipes"},
                "transform": {},
                "load": {"database": "hellofresh_takehome",
                         "table": "recipes",
                         "path": "user/hive/warehouse/hellofresh_takehome.db/recipes",
                         "partitions": {"difficulty": "string"}
                         },
                "impala": {"impala_host": "localhost",
                           "database": "hellofresh_takehome",
                           "table": "recipes"
                           }
            }"""
        mode = 'standalone'
        cls.spark, cls.env, cls.logger = utils.start_session(mode)
        cls.config_dict = utils.load_config(config_manual=cls.config)
        cls.handler = Handler(cls.logger)

    @classmethod
    def tearDownClass(cls):
        super(SparkETLTests, cls).setUpClass()
        cls.spark.stop()

    def test_config_loader_manual(self):
        _config_test_manual = """{
                "webhook": {
                    "path": "https://discord.com/api/webhooks/834903224410439751/mEhxBKfl8oxsm3Z-9Igt4vFUyxaWqoR4RNLtAcxR7oF3UA0FamXZFCXa1n_OXffSTOh4"},
                "extract": {"path": "data/test_data.json",
                            "table": "recipes"},
                "transform": {},
                "load": {"database": "hellofresh_takehome",
                         "table": "recipes",
                         "path": "user/hive/warehouse/hellofresh_takehome.db/recipes",
                         "partitions": {"difficulty": "string"}
                         },
                "impala": {"impala_host": "localhost",
                           "database": "hellofresh_takehome",
                           "table": "recipes"
                           }
            }"""
        _config_dict_test_manual = utils.load_config(config_manual=_config_test_manual)

        self.assertEqual(_config_dict_test_manual, json.loads(_config_test_manual))

    def test_config_loader_file(self):
        _config_test = 'config/config.json'
        _config_dict_test = utils.load_config(_config_test)

        with open(_config_test, 'r') as config_file:
            _config_file_dict_test = json.loads(config_file.read())
            config_file.close()

        self.assertEqual(_config_dict_test,_config_file_dict_test)

    def test_error_handler(self):
        info = self.handler.info("test")
        warn = self.handler.warn("test")
        error = self.handler.error("test")

        checks = [info, warn, error]
        for check in checks:
            self.assertEqual(check, "Discord message sent! {'content': 'test'}")

    def test_extract(self):
        extract = Extract(self.config_dict).execute(self.spark, self.handler)
        count_check = extract.count()

        self.assertEqual(count_check, 5)

    def test_transform(self):
        extract = Extract(self.config_dict).execute(self.spark, self.handler)
        transformed = Transform(self.config_dict).execute(self.spark, self.handler, extract)
        transformed_count = transformed.count()

        self.assertEqual(transformed_count, 2)

    def test_load(self):
        extract = Extract(self.config_dict).execute(self.spark, self.handler)
        transformed = Transform(self.config_dict).execute(self.spark, self.handler, extract)
        Load(self.config_dict).execute(self.spark, self.handler, transformed)

    def test_udf(self):
        Durations = ['PT10M', 'PT1H20M', 'PT', 'PT120M']
        Expectations = [10, 80, 0, 120]
        for i, dur in enumerate(Durations):
            self.assertEqual(duration_minutes_udf(dur), Expectations[i])


if __name__ == '__main__':
    unittest.main()
