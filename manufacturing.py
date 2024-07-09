import pandas as pd
import numpy as np
from faker import Faker
from datetime import timedelta, datetime
import json
import time
import random
from collections import OrderedDict
from confluent_kafka import Producer

from constants import *
from secret import *

class SyntheticDataGenerator:
    def __init__(self):
        self.fake = Faker()
        self.product_ids = []
        self.machine_ids = []
        self.generate_unique_ids()

        self.kafka_config = KAFKA_CONFIG
        self.producer = Producer(self.kafka_config)
        
        self.orders = []
        self.completed_orders = []

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
        self.write_order_to_db(None, new_order)  # Replace None with actual db connection
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
        self.write_completed_order_to_db(None, completed_order)  # Replace None with actual db connection
        return completed_order

    def write_order_to_db(self, db_connection, order):
        # Simulated database write
        pass

    def write_completed_order_to_db(self, db_connection, completed_order):
        # Simulated database write
        pass

    def send_to_kafka(self, topic, data):
        try:
            json_data = json.dumps(data)
            self.producer.produce(topic, key=str(datetime.now()), value=json_data, callback=self.delivery_report)
            self.producer.poll(0)
        except Exception as e:
            print(f"Failed to send data to Kafka: {e}")
        # pass

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

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