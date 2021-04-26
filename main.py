import click
from hellofresh_takehome.etl import *


@click.command()
@click.option('--run', default='Pipeline', help='Whether the pipeline or an individual task should be run')
@click.option('--tasks', default=None, help='List of tasks to run')
@click.option('--config_manual', default=None, help='Use a json string instead of a config file.')
@click.option('--df', default=None, help='If you are only transforming or loading, you have to supply the dataframe.')
@click.option('--mode', default='yarn', help='Whether it should be run in yarn or standalone mode.')
def main(run, tasks, mode, config_manual, df):
    print("Selected options {0} {1} {2} {3} standalone".format(run, tasks, config_manual, df))
    executor = Executor(run, tasks, 'standalone', config_manual).execute(df)


if __name__ == '__main__' or __name__ == '__file__':
    main()
