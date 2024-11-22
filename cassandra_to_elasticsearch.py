import json
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError
import os
import time
from datetime import datetime

# Connexion à Cassandra
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KEYSPACE = "ecommerce"

# Connexion à Elasticsearch
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))

# Attendre que Cassandra soit prêt
print("En attente que Cassandra soit prêt...")
time.sleep(30)

# Connexion à Cassandra
try:
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    print("Connexion à Cassandra réussie.")
except Exception as e:
    print(f"Erreur de connexion à Cassandra : {e}")
    exit(1)

# Connexion à Elasticsearch
try:
    es = Elasticsearch(
        [{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}],
        request_timeout=30
    )
    if not es.ping():
        raise ConnectionError("Impossible de se connecter à Elasticsearch.")
    print("Connexion à Elasticsearch réussie.")
except ConnectionError as e:
    print(f"Erreur de connexion Elasticsearch : {e}")
    exit(1)

# Fonction générique pour synchroniser une table Cassandra avec un index Elasticsearch
def sync_table_to_elasticsearch(table_name, index_name, row_mapper):
    print(f"Synchronisation de la table {table_name} vers l'index {index_name}...")
    try:
        # Récupération des données de Cassandra
        query = SimpleStatement(f"SELECT * FROM {table_name}")
        rows = session.execute(query)

        for row in rows:
            try:
                # Mapper les colonnes de Cassandra aux champs d'Elasticsearch
                document = row_mapper(row)

                # Vérification de l'existence de l'index dans Elasticsearch
                if not es.indices.exists(index=index_name):
                    es.indices.create(index=index_name)

                # Utiliser le champ ID correct pour Elasticsearch
                es.index(index=index_name, id=document["id"], document=document)
                print(f"Document indexé dans {index_name} : {document}")

            except TransportError as e:
                print(f"Erreur lors de l'indexation du document dans {index_name} : {e}")
                continue

    except Exception as e:
        print(f"Erreur lors de la récupération des données depuis la table {table_name} : {e}")

# Mappers spécifiques pour chaque entité
def map_product_row(row):
    return {
        "id": str(row.product_id),  # Utilise `product_id` comme identifiant
        "title": row.title,
        "price": row.price,
        "category": row.category,
        "description": row.description,
        "stock": row.stock,
        "last_synced": datetime.now().isoformat()
    }

def map_customer_row(row):
    return {
        "id": str(row.customer_id),  # Utilise `customer_id` comme identifiant
        "name": row.name,
        "email": row.email,
        "signup_date": row.signup_date.isoformat() if row.signup_date else None,
        "location": row.location,
        "loyalty_status": row.loyalty_status,
        "last_synced": datetime.now().isoformat()
    }

def map_order_row(row):
    return {
        "id": str(row.order_id),  # Utilise `order_id` comme identifiant
        "customer_id": str(row.customer_id),
        "product_id": str(row.product_id),
        "order_date": row.order_date.isoformat() if row.order_date else None,
        "quantity": row.quantity,
        "status": row.status,
        "last_synced": datetime.now().isoformat()
    }

def map_transaction_row(row):
    return {
        "id": str(row.transaction_id),  # Utilise `transaction_id` comme identifiant
        "order_id": str(row.order_id),
        "payment_method": row.payment_method,
        "payment_date": row.payment_date.isoformat() if row.payment_date else None,
        "amount": row.amount,
        "status": row.status,
        "last_synced": datetime.now().isoformat()
    }

if __name__ == "__main__":
    print("Démarrage de la synchronisation des données de Cassandra vers Elasticsearch...")

    # Synchroniser les entités
    sync_table_to_elasticsearch(
        table_name="products",
        index_name="ecommerce_products",
        row_mapper=map_product_row
    )

    sync_table_to_elasticsearch(
        table_name="customers",
        index_name="ecommerce_customers",
        row_mapper=map_customer_row
    )

    sync_table_to_elasticsearch(
        table_name="orders",
        index_name="ecommerce_orders",
        row_mapper=map_order_row
    )

    sync_table_to_elasticsearch(
        table_name="transactions",
        index_name="ecommerce_transactions",
        row_mapper=map_transaction_row
    )

    print("Synchronisation terminée.")
