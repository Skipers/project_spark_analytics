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

dataframes = extract_postgres_table(
    "/home/vboxuser/Documents/GitHub/project_spark_analytics/postgresql-42.7.3.jar",
    ['film', 'category', 'film_category', 'film_actor', 'actor', 'rental', 'inventory', 'customer', 'address', 'city'],
    'jdbc',
    "jdbc:postgresql://localhost:5432/pagila",
    'postgres',
    '123456'
)

def joins_tables(dataframes, tables_to_join: list):
    if not tables_to_join:
        raise ValueError("Список таблиц для объединения не может быть пустым")

    # Начинаем с первого DataFrame
    result_df = dataframes[tables_to_join[0]]

    for table_name in tables_to_join[1:]:
        if table_name not in dataframes:
            raise ValueError(f"DataFrame для таблицы '{table_name}' не найден")

        df = dataframes[table_name]

        # Находим столбцы с 'id' в названии
        id_columns_result = [col for col in result_df.columns if 'id' in col.lower()]
        id_columns_df = [col for col in df.columns if 'id' in col.lower()]

        # Находим общие столбцы, содержащие 'id'
        common_id_columns = set(id_columns_result) & set(id_columns_df)
        
        if common_id_columns:
            join_columns = list(common_id_columns)
            # Объединяем таблицы
            result_df = result_df.join(df, on=join_columns, how='left')
        else:
            print(f"Нет общих столбцов с 'id' для объединения таблицы '{table_name}' с результатом")

    return result_df

tables_to_join = ['customer','rental','address','city','inventory','film','film_category','category']

result = joins_tables(dataframes, tables_to_join)



task_2 = result.groupBy('last_name').agg(F.count('rental_duration'))

task_4 = result.groupBy("first_name", "last_name").agg(F.count("film_id").alias("film_count")) \
    .orderBy(F.desc("film_count"))

task_5 = result.groupBy("city") \
    .agg(
        F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_count"),
        F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_count")
    ) \
    .orderBy(F.desc("inactive_count"))


task_6 = result \
    .filter(F.lower(F.col("city")).startswith("a")) \
    .groupBy("name") \
    .agg(F.sum("rental_duration").alias("total_hours")) \
    .orderBy(F.desc("total_hours")) \
    .limit(1)


task_7 = result \
    .filter(F.col("city").contains("-")) \
    .groupBy("name") \
    .agg(F.sum("rental_duration").alias("total_hours")) \
    .orderBy(F.desc("total_hours")) \
    .limit(1)

tasks_complit = [task_2,task_4,task_5,task_6,task_6,task_7]


for i in tasks_complit:
    i.show(vertical= True)