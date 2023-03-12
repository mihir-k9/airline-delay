""" Contains various write functions."""


def write_local(df, root_path, file_name):
    df.write.mode('append') \
        .format('csv') \
        .option('header', 'true') \
        .save(f'{root_path}/temp/{file_name}')


def write_redshift(df, aws_endpoint, port, db_name, table_name, username, password):
    df.write.mode('append') \
        .format("jdbc") \
        .option("url", f"jdbc:redshift://{aws_endpoint}:{port}/{db_name}") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("dbtable", f"{table_name}") \
        .option("user", f"{username}") \
        .option("password", f"{password}") \
        .save()
