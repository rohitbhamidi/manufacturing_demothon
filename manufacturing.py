import random
import time
import json
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from secret import *
from constants import *

class SyntheticDataGenerator:
    def __init__(self):
        self.fake = Faker()
        self.product_ids = []
        self.machine_ids = []
        self.generate_unique_ids()

        self.kafka_config = KAFKA_CONFIG
        self.producer = Producer(self.kafka_config)

        self.mongo_conn_str = MONGO_CONNECTION_STRING
        self.mongo_client = MongoClient(self.mongo_conn_str, server_api=ServerApi('1'))
        self.db = self.mongo_client.manufacturing_demothon
        self.collection_new_orders = self.db.orders
        self.collection_completed_orders = self.db.completed_orders

        self.orders = []
        self.completed_orders = []

        # Validate connections
        self.validate_kafka_connection()
        self.validate_mongo_connection()

    def generate_unique_ids(self):
        self.product_ids = [self.fake.uuid4() for _ in range(2)]
        self.machine_ids = [self.fake.uuid4() for _ in range(2)]

    def generate_machine_status(self, machine_id, order_id):
        machine_status = random.choice([0, 1])
        machine_status_ts = datetime.now().isoformat()
        status = {
            'machine_id': machine_id,
            'machine_status': machine_status,
            'machine_status_ts': machine_status_ts,
            'order_id': order_id
        }
        self.send_to_kafka('new_orders', status)
        return status

    def create_order(self, order_id, product_id, quantity):
        start_time = datetime.now().isoformat()
        order = {
            'order_id': order_id,
            'product_id': product_id,
            'quantity': quantity,
            'start_time': start_time
        }
        self.orders.append(order)
        return order

    def generate_new_order(self):
        order_id = self.fake.uuid4()
        product_id = random.choice(self.product_ids)
        quantity = random.randint(1, 10)
        new_order = self.create_order(order_id, product_id, quantity)
        self.write_order_to_db(new_order)
        return new_order

    def complete_order(self, order_id):
        order = next((o for o in self.orders if o['order_id'] == order_id), None)
        if not order:
            raise ValueError("Order not found")

        end_time = datetime.now().isoformat()
        total_units = order['quantity']
        failed_units = 0

        for _ in range(total_units):
            machine_status = self.generate_machine_status(random.choice(self.machine_ids), order_id)
            if machine_status['machine_status'] == 0:
                failed_units += 1
            time.sleep(1)  # Simulate time passing

        successful_units = total_units - failed_units
        yield_rate = successful_units / total_units

        order_status = 1 if yield_rate == 1 else 0
        qa_comments = "All units completed successfully" if order_status == 1 else f"{failed_units} units failed during production"

        completed_order = {
            'order_id': order_id,
            'order_status': order_status,
            'end_time': end_time,
            'yield': yield_rate,
            'qa_comments': qa_comments
        }
        self.completed_orders.append(completed_order)
        self.orders.remove(order)
        self.write_completed_order_to_db(completed_order)
        return completed_order

    def write_order_to_db(self, order):
        try:
            self.collection_new_orders.insert_one(order)
            print("New order written to MongoDB successfully.")
        except Exception as e:
            print(f"Failed to write new order to MongoDB: {e}")

    def write_completed_order_to_db(self, completed_order):
        try:
            self.collection_completed_orders.insert_one(completed_order)
            print("Completed order written to MongoDB successfully.")
        except Exception as e:
            print(f"Failed to write completed order to MongoDB: {e}")

    def send_to_kafka(self, topic, data):
        try:
            json_data = json.dumps(data)
            self.producer.produce(topic, key=str(datetime.now()), value=json_data, callback=self.delivery_report)
            self.producer.poll(0)
        except KafkaException as e:
            print(f"Failed to send data to Kafka: {e}")

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def validate_kafka_connection(self):
        try:
            admin_client = AdminClient(self.kafka_config)
            topic_list = ['test_topic']
            metadata = admin_client.list_topics(timeout=10)

            for topic in topic_list:
                if topic not in metadata.topics:
                    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
                    admin_client.create_topics([new_topic])
                    print(f"Created topic: {topic}")

            # Now produce a test message
            self.producer.produce('test_topic', key='test', value='test_message', callback=self.delivery_report)
            self.producer.flush()
            print("Kafka connection validated successfully.")
        except Exception as e:
            print(f"Failed to validate Kafka connection: {e}")

    def validate_mongo_connection(self):
        try:
            # Perform a simple operation to check connection
            self.db.command('ping')
            print("MongoDB connection validated successfully.")
        except Exception as e:
            print(f"Failed to validate MongoDB connection: {e}")

    def run(self):
        while True:
            if random.random() < 0.1:  # 10% chance to generate a new order each iteration
                new_order = self.generate_new_order()
                print(f"New Order Generated: {new_order}")

            for order in list(self.orders):
                if random.random() < 0.1:  # 10% chance to complete each order per iteration
                    completed_order = self.complete_order(order['order_id'])
                    print(f"Order Completed: {completed_order}")

            time.sleep(1)  # Adjust the sleep time as necessary
