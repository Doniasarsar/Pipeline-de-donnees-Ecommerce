from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# Configuration de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkCassandra") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Définition des schémas pour chaque entité
schema_products = StructType([
    StructField("product_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("stock", IntegerType(), True)
])

schema_customers = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("loyalty_status", StringType(), True)
])

schema_orders = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("status", StringType(), True)
])

schema_transactions = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_date", TimestampType(), True),
    StructField("amount", FloatType(), True),
    StructField("status", StringType(), True)
])

# Fonction pour lire, transformer et écrire un topic spécifique
def process_stream(topic, schema, keyspace, table, transformations=None):
    # Lire les données depuis Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Transformation des données
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Appliquer des transformations supplémentaires si spécifié
    if transformations:
        df_parsed = transformations(df_parsed)

    # Écriture dans Cassandra
    query = df_parsed.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .outputMode("append") \
        .start()

    return query

# Transformation spécifique pour les produits
def transform_products(df):
    return df.withColumn("price_with_tax", col("price") * 1.2) \
             .filter(col("stock") > 0)  # Filtrer les produits avec stock

# Transformation spécifique pour les commandes
def transform_orders(df):
    return df.filter(col("status") != "Cancelled")  # Exclure les commandes annulées

# Traiter chaque topic Kafka
queries = []

queries.append(process_stream(
    topic="ecommerce-products",
    schema=schema_products,
    keyspace="ecommerce",
    table="products",
    transformations=transform_products
))

queries.append(process_stream(
    topic="ecommerce-customers",
    schema=schema_customers,
    keyspace="ecommerce",
    table="customers"
))

queries.append(process_stream(
    topic="ecommerce-orders",
    schema=schema_orders,
    keyspace="ecommerce",
    table="orders",
    transformations=transform_orders
))

queries.append(process_stream(
    topic="ecommerce-transactions",
    schema=schema_transactions,
    keyspace="ecommerce",
    table="transactions"
))

# Attendre la fin de toutes les requêtes
for query in queries:
    query.awaitTermination()
