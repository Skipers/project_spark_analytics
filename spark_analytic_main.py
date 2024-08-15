from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def extract_postgres_table(driver_path, tables_list, file_type, url, user_name, password_db):
    spark = SparkSession.builder \
        .appName('Analytics') \
        .config("spark.driver.extraClassPath", driver_path) \
        .getOrCreate()
        
    dataframes = {}

    for table_name in tables_list:
        df = spark.read.format(file_type) \
            .option('url', url) \
            .option("dbtable", table_name) \
            .option("user", user_name) \
            .option("password", password_db) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        dataframes[table_name] = df
    
    return dataframes

# Вызов функции 
dataframes = extract_postgres_table(
    "/home/vboxuser/Documents/GitHub/project_spark_analytics/postgresql-42.7.3.jar",
    ['film', 'category', 'film_category', 'film_actor', 'actor', 'rental', 'inventory', 'customer', 'address', 'city'],
    'jdbc',
    "jdbc:postgresql://localhost:5432/pagila",
    'postgres',
    '123456'
)


def join_tables(dataframes, join_conditions, join_to_result_table=None):
    if not join_conditions:
        raise ValueError("join_conditions не может быть пустым")

    # Присоединение первой таблицы
    left_table_name, right_table_name, on_condition, join_type, drop_id = join_conditions[0]
    joined_df = dataframes[left_table_name].join(dataframes[right_table_name], on_condition, join_type) \
        .drop(drop_id)

    # Проходим по оставшимся условиям соединения
    for left_table_name, right_table_name, on_condition, join_type, drop_id in join_conditions[1:]:
        right_df = dataframes[right_table_name]
        joined_df = joined_df.join(right_df, on_condition, join_type) \
            .drop(drop_id)

    # Присоединение дополнительных таблиц к результирующей таблицы
    if join_to_result_table:
        for second_table_name, on_condition, join_type, drop_id in join_to_result_table:
            right_df = dataframes[second_table_name]
            joined_df = joined_df.join(right_df, on_condition, join_type) \
                .drop(drop_id)

    return joined_df


# Вызов функции
result_df = join_tables(dataframes, [
    ('film_category', 'film', dataframes['film_category'].film_id == dataframes['film'].film_id, 'left', dataframes['film'].film_id),
    ('film_category', 'category', dataframes['film_category'].category_id == dataframes['category'].category_id, 'left', dataframes['category'].category_id)], [
    ('film_actor', dataframes['film_category'].film_id == dataframes['film_actor'].film_id, 'left', dataframes['film_actor'].film_id),
    ('actor', dataframes['film_actor'].actor_id == dataframes['actor'].actor_id, 'left', dataframes['actor'].actor_id)])

result_df.show(vertical=True)