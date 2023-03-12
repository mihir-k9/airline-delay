from src.constant import *
from src.read import *
from src.write import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, udf
from pyspark.sql.types import *

from datetime import datetime
import configparser


# transformations
def get_cancelled_flights(df):

    """ Get details of cancelled flights. """

    cancelled_df = df.filter(col('CancellationCode') != 'NA')\
        .groupby('CancellationCode')\
        .count().alias('CancellationCount')

    return cancelled_df


def main():

    """ ETL script to process airline data. """

    # parse configs data
    cfg = configparser.ConfigParser()
    cfg.read(f'{PARENT_DIR}/configs/config.ini')

    bucket_path = cfg['s3']['bucket_path']

    input_filename = 'flights_00.csv'
    write_name = 'cancelled_flights'

    job_start = datetime.now()

    # create spark session
    spark = SparkSession.builder \
        .appName("etl_airline") \
        .getOrCreate()

    try:
        # extract
        # df_airline = read_local(PARENT_DIR, test_filename, spark)
        df_airline = read_s3(bucket_path, input_filename, spark)

        # transform
        df_cancelled = get_cancelled_flights(df_airline)

        # load
        # write_local(df_cancelled, PARENT_DIR, test_output)
        write_s3(df_cancelled, bucket_path, write_name)

    except Exception as E:
        print('Exception Occurred: \n')
        print(E)

    finally:
        job_end = datetime.now()
        spark.stop()


# entry point for spark app
if __name__ == '__main__':
    main()
