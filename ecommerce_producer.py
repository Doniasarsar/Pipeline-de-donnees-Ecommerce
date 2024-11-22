import json
from confluent_kafka import Producer
import random
from faker import Faker
import uuid
from datetime import datetime
import time

# Initialisation de Faker
fake = Faker()

# Configuration Kafka
KAFKA_BROKER = 'localhost:9092'

# Vérification de la connectivité Kafka
def check_kafka_connection():
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        producer.list_topics(timeout=5)
        print("Connexion à Kafka réussie.")
    except Exception as e:
        print(f"Erreur de connexion à Kafka : {e}")
        exit(1)

# Génération des Produits
def generate_products(n=50):
    categories = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports']
    return [
        {
            "product_id": str(uuid.uuid4()),  # Identifiant unique
            "title": fake.catch_phrase(),  # Nom du produit
            "price": round(random.uniform(5, 500), 2),  # Prix aléatoire
            "category": random.choice(categories),  # Catégorie
            "description": fake.text(max_nb_chars=200),  # Description
            "stock": random.randint(10, 200)  # Stock disponible
        }
        for _ in range(n)
    ]

# Génération des Clients
def generate_customers(n=20):
    return [
        {
            "customer_id": str(uuid.uuid4()),  # Identifiant unique
            "name": fake.name(),  # Nom complet
            "email": fake.email(),  # Email
            "signup_date": fake.date_between(start_date='-3y', end_date='today').isoformat(),  # Date d'inscription
            "location": fake.city(),  # Ville
            "loyalty_status": random.choice(['Bronze', 'Silver', 'Gold'])  # Statut fidélité
        }
        for _ in range(n)
    ]

# Génération des Commandes
def generate_orders(customers, products, n=100):
    return [
        {
            "order_id": str(uuid.uuid4()),  # Identifiant unique
            "customer_id": random.choice(customers)["customer_id"],  # Client associé
            "product_id": random.choice(products)["product_id"],  # Produit associé
            "order_date": fake.date_time_between(start_date='-1y', end_date='now').isoformat(),  # Date de commande
            "quantity": random.randint(1, 5),  # Quantité
            "status": random.choice(['Pending', 'Shipped', 'Cancelled'])  # Statut de la commande
        }
        for _ in range(n)
    ]

# Génération des Transactions
def generate_transactions(orders, n=50):
    transactions = []
    for order in random.sample(orders, n):  # Générer des transactions uniquement pour certaines commandes
        try:
            # Convertir la date de commande en datetime si nécessaire
            order_date = order["order_date"]
            if isinstance(order_date, str):
                order_date = datetime.fromisoformat(order_date.split('.')[0])  # Supprime les microsecondes

            transaction = {
                "transaction_id": str(uuid.uuid4()),  # Identifiant unique
                "order_id": order["order_id"],  # Commande associée
                "payment_method": random.choice(['Credit Card', 'PayPal', 'Bank Transfer']),  # Méthode de paiement
                "payment_date": fake.date_time_between(
                    start_date=order_date, end_date=datetime.now()
                ).strftime('%Y-%m-%dT%H:%M:%S'),  # Format ISO 8601 sans microsecondes
                "amount": round(random.uniform(20, 1000), 2),  # Montant aléatoire
                "status": random.choice(['Completed', 'Failed']),  # Statut de la transaction
            }
            transactions.append(transaction)
        except ValueError as e:
            print(f"Erreur lors de la génération de la transaction pour la commande {order['order_id']} avec la date {order['order_date']} : {e}")
            continue
    return transactions

# Callback Kafka pour confirmer l'envoi
def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur lors de l'envoi : {err}")
    else:
        print(f"Message envoyé : {msg.value().decode('utf-8')} au topic {msg.topic()}")

# Configuration du producteur Kafka
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Envoi des données à Kafka
def send_to_kafka(topic, data):
    for record in data:
        try:
            producer.produce(topic, value=json.dumps(record), callback=delivery_report)
        except Exception as e:
            print(f"Erreur d'envoi sur Kafka : {e}")
    producer.flush()

if __name__ == "__main__":
    # Vérifier la connexion à Kafka
    check_kafka_connection()

    # Générer les données
    products = generate_products(50)
    customers = generate_customers(20)
    orders = generate_orders(customers, products, 100)
    transactions = generate_transactions(orders, 50)

    # Envoyer les données générées à Kafka
    send_to_kafka('ecommerce-products', products)  # Envoyer les produits
    send_to_kafka('ecommerce-customers', customers)  # Envoyer les clients
    send_to_kafka('ecommerce-orders', orders)  # Envoyer les commandes
    send_to_kafka('ecommerce-transactions', transactions)  # Envoyer les transactions
