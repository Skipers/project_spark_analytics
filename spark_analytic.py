from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Analytics') \
    .getOrCreate()

# df = spark.read.binaryFiles('/home/vboxuser/Downloads/pagila-data-apt-jsonb.sql')
with open("/home/vboxuser/Downloads/pagila-data-apt-jsonb.sql", "r") as f:
    sql_query = f.read()


df = spark.sql(sql_query)


df.show()
