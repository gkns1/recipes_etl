import isodate
from pyspark.sql.functions import *
from hellofresh_takehome.utils import Handler
from hellofresh_takehome import utils


class Executor(object):
    def __init__(self, run, tasks, mode, config=None, config_manual=None):
        if config_manual is None:
            config_manual = {}
        if config is None:
            config = utils.load_config()
        self.spark, self.env, self.logger = utils.start_session(mode)
        self.handler = Handler(self.logger)
        self.run = run
        self.tasks = tasks

    def execute(self, df):
        if self.spark is None:
            print("The spark session has not been started! Exiting.")
            exit()

        if self.run != 'Pipeline' and self.run != 'Extract' and self.run != 'Transform' and self.run != 'Load':
            self.handler.error("The only acceptable inputs are 'Extract', 'Transform', 'Load' and 'Pipeline'.")

        elif self.run == 'Pipeline':

            self.tasks = [Extract(), Transform(), Load()]
            for task in self.tasks:
                try:
                    self.handler.info('Starting the {0}...'.format(str(task)))
                    if isinstance(task, Extract):
                        df = task.execute(self.spark, self.handler)
                    else:
                        task.execute(self.spark, self.handler, df)
                    self.handler.info('{0} finished!'.format(str(task)))
                except Exception as e:
                    self.handler.error(e)
                    self.spark.stop()
                    exit()

        elif self.run == 'Extract':
            try:
                self.handler.info('Starting the Extract...')
                df = Extract().execute(self.spark, self.handler)
                
                self.handler.info('Extract finished!')
            except Exception as e:
                self.handler.error(e)
                self.spark.stop()

                exit()
        elif self.run == 'Transform':

            try:
                self.handler.info('Starting the Transform...')
                Transform().execute(self.spark, self.handler, df)
                self.handler.info('Transform finished!')
            except Exception as e:
                self.handler.error(e)
                self.spark.stop()
                exit()

        elif self.run == 'Load':

            try:
                self.handler.info('Starting the Load...')
                Load().execute(self.spark, self.handler, df)
                self.handler.info('Load finished!')
            except Exception as e:
                self.handler.error(e)
                self.spark.stop()
                exit()

        self.spark.stop()


class Extract(object):
    def __init__(self, config=utils.load_config()):
        self.path = config['extract'].get('path')
        self.table = config['extract'].get('table')

    def execute(self, spark, handler):
        extractDF = None  # make the linter happy
        if self.path is None:
            self.path = "https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json"
        try:
            handler.info('Starting to extract data...')
            extractDF = spark.read.json(self.path)
            handler.info('Data extracted!')
            handler.info('Creating temp table')
            handler.info('Table created.')
        except Exception as e:
            handler.error(e)

        # add cleanup
        return extractDF


class Transform(object):
    def __init__(self, config=utils.load_config()):
        self.config = config['transform']

    def execute(self, spark, handler, df):
        if df is None:
            "The extract data has to be supplied!"
            exit()
        handler.info("Filtering for beef.")
        df_filtered = df.filter(lower("ingredients").contains("beef"))

        # prepTime and cookTime uses ISO 8601 Duration.
        # We have to get rid of "PT" and convert M to minutes, Hours to 60 minutes.
        # We can use also use isodate's parse_duration to get timedelta in seconds.
        handler.info("Registering UDF.")
        spark.register('parse_duration', isodate.parse_duration())
        handler.info("Parsing durations.")
        df_transformed = df_filtered \
        .withColumn('prepTime', isodate.parse_duration().seconds / 60) \
        .withColumn('cookTime', isodate.parse_duration().seconds / 60)
        """
        ingestDF_filtered \
            .withColumn('prepTime', regexp_replace('prepTime', 'PT', '')) \
            .withColumn('prepTime', regexp_replace('prepTime', 'H', '*60+')) \
            .withColumn('prepTime', regexp_replace('prepTime', 'M', '*1')) \
            .withColumn('cookTime', regexp_replace('cookTime', 'PT', '')) \
            .withColumn('cookTime',regexp_replace('cookTime', 'H','*60+')) \
            .withColumn('cookTime', regexp_replace('cookTime', 'M', '*1'))
            add eval to fields before calculating differences
        """
        handler.info("Adding difficulty.")
        df_transformed.withColumn("difficulty", when(col("cookTime") + col("prepTime") > 60, "hard")
            .when(col("cookTime") + col("prepTime").between(31,60), "medium")
            .otherwise("easy"))
        handler.info("Adding date of execution.")
        df_transformed.withColumn(col('date_of_execution'), current_date()).show()
        return df_transformed


class Load(object):
    def __init__(self, config = utils.load_config(), env = 'local'):
        self.db = config['load'].get('database')
        self.table = config['load'].get('table')
        self.path = config['load'].get('path')
        self.partition_config = config['load'].get('partitions')
        self.partition_cols = ",".join(self.partition_config)
        self.env = env

    def execute(self, spark, handler, df = None,):
        if self.env == 'dev':
            pass

        handler.info("Starting to load transformed DF...")
        (df
         .write
         .format('parquet')
         .mode('append') #leaves the historical records in
         .partitionBy(self.partition_cols)
         .save(self.path))
        handler.info("DF loaded successfully!")