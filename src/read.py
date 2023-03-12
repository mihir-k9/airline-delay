""" Contains various read functions."""


def read_local(root_path, file_name, spark):

    df = spark.read \
        .option("header", True) \
        .csv(f"{root_path}/{file_name}")

    return df


def read_s3(bucket_path, table_name, spark):

    df = spark.read \
        .parquet(f's3a://{bucket_path}/{table_name}')

    return df


def read_redshift(spark, aws_endpoint, port, db_name, table_name, username, password):

    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:redshift://{aws_endpoint}:{port}/{db_name}") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("dbtable", f"{table_name}") \
        .option("user", f"{username}") \
        .option("password", f"{password}") \
        .load()

    return df
