from confluent_kafka import Consumer, KafkaException
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import json
import os
import uuid

# Configuration Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')  # Si Kafka est local
CASSANDRA_HOST = 'localhost'

# Configuration du consommateur Kafka
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'ecommerce_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True  # Activer la confirmation automatique
}

consumer = Consumer(consumer_conf)

# Connexion à Cassandra
cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()

# Créer un keyspace et des tables dans Cassandra
KEYSPACE = "ecommerce"

# Définition des tables
TABLE_PRODUCTS = "products"
TABLE_CUSTOMERS = "customers"
TABLE_ORDERS = "orders"
TABLE_TRANSACTIONS = "transactions"

# Créer le keyspace si nécessaire
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
""")
session.set_keyspace(KEYSPACE)

# Création des tables Cassandra
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_PRODUCTS} (
        product_id UUID PRIMARY KEY,
        title text,
        price float,
        category text,
        description text,
        stock int
    )
""")

session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_CUSTOMERS} (
        customer_id UUID PRIMARY KEY,
        name text,
        email text,
        signup_date timestamp,
        location text,
        loyalty_status text
    )
""")

session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_ORDERS} (
        order_id UUID PRIMARY KEY,
        customer_id UUID,
        product_id UUID,
        order_date timestamp,
        quantity int,
        status text
    )
""")

session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_TRANSACTIONS} (
        transaction_id UUID PRIMARY KEY,
        order_id UUID,
        payment_method text,
        payment_date timestamp,
        amount float,
        status text
    )
""")

# S'abonner aux topics Kafka
consumer.subscribe(['ecommerce-products', 'ecommerce-customers', 'ecommerce-orders', 'ecommerce-transactions'])

print("Consommateur prêt à recevoir des messages...")

# Fonction d'insertion dans Cassandra
def insert_into_cassandra(table, data):
    try:
        if table == TABLE_PRODUCTS:
            session.execute(f"""
                INSERT INTO {TABLE_PRODUCTS} (product_id, title, price, category, description, stock)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (uuid.UUID(data['product_id']), data['title'], data['price'], data['category'], data['description'], data['stock']))
        
        elif table == TABLE_CUSTOMERS:
            session.execute(f"""
                INSERT INTO {TABLE_CUSTOMERS} (customer_id, name, email, signup_date, location, loyalty_status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (uuid.UUID(data['customer_id']), data['name'], data['email'], data['signup_date'], data['location'], data['loyalty_status']))
        
        elif table == TABLE_ORDERS:
            session.execute(f"""
                INSERT INTO {TABLE_ORDERS} (order_id, customer_id, product_id, order_date, quantity, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (uuid.UUID(data['order_id']), uuid.UUID(data['customer_id']), uuid.UUID(data['product_id']),
                  data['order_date'], data['quantity'], data['status']))
        
        elif table == TABLE_TRANSACTIONS:
            session.execute(f"""
                INSERT INTO {TABLE_TRANSACTIONS} (transaction_id, order_id, payment_method, payment_date, amount, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (uuid.UUID(data['transaction_id']), uuid.UUID(data['order_id']), data['payment_method'],
                  data['payment_date'], data['amount'], data['status']))
    except Exception as e:
        print(f"Erreur lors de l'insertion dans {table} : {e}")
        print(f"Données : {data}")

# Consommation des messages Kafka
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur Kafka : {msg.error()}")
            continue
        
        # Identifier le topic et insérer dans la table correspondante
        topic = msg.topic()
        data = json.loads(msg.value().decode('utf-8'))

        if topic == 'ecommerce-products':
            insert_into_cassandra(TABLE_PRODUCTS, data)
        elif topic == 'ecommerce-customers':
            insert_into_cassandra(TABLE_CUSTOMERS, data)
        elif topic == 'ecommerce-orders':
            insert_into_cassandra(TABLE_ORDERS, data)
        elif topic == 'ecommerce-transactions':
            insert_into_cassandra(TABLE_TRANSACTIONS, data)

        print(f"Données insérées dans {topic} : {data}")

except KeyboardInterrupt:
    print("Arrêt du consommateur.")
finally:
    consumer.close()
    cluster.shutdown()
