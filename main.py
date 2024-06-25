import faker
import psycopg2
from datetime import datetime
import random
from kafka import KafkaProducer
from time import sleep

topic_name='demo_testing'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
fake = faker.Faker()

def generate_transaction():
    user = fake.simple_profile()
    return {
        "transaction_id": fake.uuid4(),
        "user_id": user['username'],
        "timestamp": datetime.utcnow(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(['USD', 'IND', 'GBP']),
        "city": fake.city(),
        "country": fake.country(),
        "merchant_name": fake.company(),
        "payment_method": random.choice(["credit_card", "debit_card", "online_transaction", "UPI"]),
        "ip_address": fake.ipv4(),
        "voucher_code": random.choice(['', "Discount10", '']),
        "affiliated_id": fake.uuid4()
    }

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),   
            voucher_code VARCHAR(255),
            affiliated_id VARCHAR(255)
        )
        """
    )
    cursor.close()
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        database="sharath",
        user="postgres",
        password="Sharath@9224",
        port=5433
    )

    create_table(conn)
    
    transaction = generate_transaction()
    cur = conn.cursor()
    print(transaction)

    cur.execute(
        """
        INSERT INTO transactions(transcation_id, user_id, timestamp, amount, currency,
        city, country, merchant_name, payment_method, ip_address, voucher_code, affliated_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            transaction["transaction_id"],
            transaction["user_id"],
            transaction["timestamp"],
            transaction["amount"],
            transaction["currency"],
            transaction["city"],
            transaction["country"],
            transaction["merchant_name"],
            transaction["payment_method"],
            transaction["ip_address"],
            transaction["voucher_code"],
            transaction["affiliated_id"]
        )
    )

    cur.close()
    conn.commit()
    sleep(2)
    #conn.close()
