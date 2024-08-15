from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName('Analytics') \
    .config("spark.driver.extraClassPath", "/home/vboxuser/Documents/GitHub/project_spark_analytics/postgresql-42.7.3.jar") \
    .getOrCreate()


film = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "film") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

category = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "category") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

film_category = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "film_category") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

film_actor = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "film_actor") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

actor = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "actor") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

rental = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "rental") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()


inventory = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "inventory") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

customer = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "customer") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

address = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "address") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

city = spark.read \
    .format('jdbc') \
    .option('url', "jdbc:postgresql://localhost:5432/pagila") \
    .option("dbtable", "city") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()


table = film_category.join(film,film_category['film_id'] == film['film_id'],'left').drop(film['film_id']) \
    .join(category,film_category['category_id'] == category['category_id']).drop(category['category_id']) \
    
# table.show(vertical = True)

table_2 = film_actor.join(film,film_actor['film_id'] == film['film_id'],'left').drop(film['film_id']) \
    .join(actor,film_actor['actor_id'] == actor['actor_id'],'left').drop(actor['actor_id'])

# table_2.show(vertical = True)

task_2 = table_2.groupBy('last_name').agg(F.count('rental_duration'))

# task_2.show(vertical = True)

table_4 = inventory.join(film,inventory['film_id'] == film['film_id'],'left_anti')


# table_4.show(vertical = True)

# task_3 = table_4.filter(table_4.film_id NOT IN table_4 )

table_5 = film_actor.join(table,table['film_id'] == film_actor['film_id'],'left').drop(film_actor['film_id']) \
    .join(actor,actor['actor_id'] == film_actor['actor_id'],'left').drop(film_actor['actor_id'])

# table_5.show(vertical = True)
children_films_df = table_5.filter(F.col("name") == "Children")

task_5 = children_films_df.groupBy("first_name", "last_name").agg(F.count("film_id").alias("film_count")) \
    .orderBy(F.desc("film_count"))
# .limit(3)

# task_5.show(vertical = True)


table_6 = address.join(city,address['city_id'] == city['city_id'],'left').drop(city['city_id'])

table_6 = table_6.join(customer,customer['address_id'] == table_6['address_id'],'left')
# table_6.show(vertical = True)

city_counts_df = table_6.groupBy("city") \
    .agg(
        F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_count"),
        F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_count")
    ) \
    .orderBy(F.desc("inactive_count"))


# city_counts_df.show(vertical = True)


table_8 = address.join(city,city['city_id'] == address['city_id']).drop('city_id') \
    .join(customer,address['address_id'] == customer['address_id']).drop(customer['address_id'])

# table_8.show(vertical = True)

table_9 = customer.join(rental,rental['customer_id'] == customer['customer_id']).drop(customer['customer_id']) \
    .join(address,address['address_id'] == customer['address_id']).drop(customer['address_id']) 
# table_9.show(vertical = True)

table_10 = table_9.join(city,city['city_id'] == table_9['city_id']).drop(table_9['city_id'])
# table_10.show(vertical = True)

table_11  = inventory.join(table_10,inventory['inventory_id'] == table_10['inventory_id']).drop(table_10['inventory_id']) \
    .join(table,table['film_id'] == inventory['film_id']).drop(inventory['film_id'])

# table_11.show(vertical = True)

category_hours_in_a_cities = table_11 \
    .filter(F.col("city").startswith("a")) \
    .groupBy("name") \
    .agg(F.sum("rental_duration").alias("total_hours")) \
    .orderBy(F.desc("total_hours")) \
    .limit(1)

category_hours_in_a_cities.show(vertical = True)

category_hours_in_hyphen_cities = table_11 \
    .filter(F.col("city").contains("-")) \
    .groupBy("name") \
    .agg(F.sum("rental_duration").alias("total_hours")) \
    .orderBy(F.desc("total_hours")) \
    .limit(1)

category_hours_in_hyphen_cities.show(vertical = True)
