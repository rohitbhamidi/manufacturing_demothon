import pandas as pd
import numpy as np
from faker import Faker
from datetime import timedelta, datetime
import json
import time
from collections import OrderedDict
from confluent_kafka import Producer
from constants import *

from secret import KAFKA_CONFIG

class SmallDataGenerator:
    """
    Generate synthetic data for testing a manufacturing process.

    This includes:
    - Real-time data: machine status, ingredient inventory, batch status, QA inspection results, final orders.
    - Static data: product catalog, machine catalog, ingredient catalog, inspector catalog, process catalog.
    """

    def __init__(self):
        """Initialize the data generator with unique IDs for various entities."""
        self.fake = Faker()
        self.product_ids = []
        self.machine_ids = []
        self.ingredient_ids = []
        self.inspector_ids = []
        self.process_ids = []
        self.generate_unique_ids()
        
        # Kafka Producer Configuration
        self.kafka_config = KAFKA_CONFIG
        self.producer = Producer(self.kafka_config)
        self.mongo_details = MONGO_DETAILS
    
    def generate_unique_ids(self):
        """Generate unique IDs for products, machines, ingredients, inspectors, and processes."""
        self.product_ids = [self.fake.uuid4() for _ in range(2)]
        self.machine_ids = [self.fake.uuid4() for _ in range(2)]
        self.ingredient_ids = [self.fake.uuid4() for _ in range(3)]
        self.inspector_ids = [self.fake.uuid4() for _ in range(2)]
        self.process_ids = [self.fake.uuid4() for _ in range(2)]
    
    def generate_static_data(self):
        """
        Generate static data for product, machine, ingredient, inspector, and process catalogs.
        
        Returns:
            tuple: Contains dictionaries for product, machine, ingredient, inspector, and process catalogs.
        """
        product_catalog = {
            "product_id": self.product_ids,
            "product_name": [self.fake.word() for _ in range(2)],
            "category": [self.fake.random_element(elements=('snack', 'beverage', 'candy', 'dairy')) for _ in range(2)],
            "price": [round(self.fake.random_number(digits=3) / 100, 2) for _ in range(2)],
            "ingredients_list": [self.fake.random_elements(elements=('flour', 'butter', 'sugar'), unique=True, length=2) for _ in range(2)]
        }
        machine_catalog = {
            "machine_id": self.machine_ids,
            "machine_name": [self.fake.word() for _ in range(2)],
            "location": [self.fake.random_int(min=1, max=100) for _ in range(2)],
            "capacity": [self.fake.random_int(min=1, max=100) for _ in range(2)]
        }
        ingredient_catalog = {
            "ingredient_id": self.ingredient_ids,
            "ingredient_name": ['flour', 'butter', 'sugar'],
            "supplier": [self.fake.company() for _ in range(3)],
            "unit_price": [round(self.fake.random_number(digits=2) / 100, 2) for _ in range(3)]
        }
        inspector_catalog = {
            "inspector_id": self.inspector_ids,
            "inspector_name": [self.fake.name() for _ in range(2)],
            "inspector_title": [self.fake.random_element(elements=('QA', 'QC', 'QA/QC')) for _ in range(2)],
            "reliability_score": [round(self.fake.random_number(digits=2) / 100, 2) for _ in range(2)]
        }
        process_catalog = {
            "process_id": self.process_ids,
            "process_name": [self.fake.word() for _ in range(2)],
            "process_json": [json.dumps({"step1": "do something", "step2": "do something else"}) for _ in range(2)]
        }
        return product_catalog, machine_catalog, ingredient_catalog, inspector_catalog, process_catalog

    def generate_real_time_data(self, orders, send_method='kafka'):
        """
        Generate real-time data for the manufacturing process.

        Args:
            orders (list): List of order counts for each product.

        Yields:
            dict: Current state of machine status, ingredient inventory, batch status, QA inspection results, and final orders.
        """
        ingredient_inventory = self._initialize_ingredient_inventory()
        batch_status = self._initialize_batch_status()
        product_order_counts = self._initialize_order_counts(orders)

        while any(count > 0 for count in product_order_counts.values()):
            machine_status = self._generate_machine_status()
            self._update_batch_status(machine_status, batch_status, ingredient_inventory, product_order_counts)
            inspection_results = self._generate_qa_inspection_notes(batch_status, product_order_counts)
            final_orders = self._generate_final_orders(product_order_counts)

            data = {
                "machine_status": machine_status,
                "ingredient_inventory": ingredient_inventory,
                "batch_status": batch_status,
                "inspection_results": inspection_results,
                "final_orders": final_orders
            }

            if send_method == 'kafka':
                self._send_to_kafka('real_time_data', data)
            else:
                self._write_to_mongo(self.mongo_details, data)

            yield data
            time.sleep(1)

    def _initialize_ingredient_inventory(self):
        """
        Initialize ingredient inventory with starting quantities.

        Returns:
            dict: Initial ingredient inventory.
        """
        return {id: {"ingredient_name": name, "quantity": 10} for id, name in zip(self.ingredient_ids, ['flour', 'butter', 'sugar'])}

    def _initialize_batch_status(self):
        """
        Initialize batch status for each product.

        Returns:
            dict: Initial batch status for each product.
        """
        return {id: {"timestamp": None, "batch_status": "waiting", "machine_stage": MACHINE_STAGE_WAITING} for id in self.product_ids}

    def _initialize_order_counts(self, orders):
        """
        Initialize order counts for each product.

        Args:
            orders (list): List of order counts for each product.

        Returns:
            dict: Initial order counts for each product.
        """
        return {product_id: count for product_id, count in zip(self.product_ids, orders)}

    def _generate_machine_status(self):
        """
        Generate the current status of each machine.

        Returns:
            dict: Current status of each machine.
        """
        return {id: {"timestamp": datetime.now(), "machine_status": self.fake.random_element(elements=OrderedDict([('running', MACHINE_RUNNING_PROB), ('stopped', MACHINE_STOPPED_PROB)]))} for id in self.machine_ids}

    def _update_batch_status(self, machine_status, batch_status, ingredient_inventory, product_order_counts):
        """
        Update batch status based on machine status and other conditions.

        Args:
            machine_status (dict): Current status of each machine.
            batch_status (dict): Current status of each batch.
            ingredient_inventory (dict): Current ingredient inventory.
            product_order_counts (dict): Current order counts for each product.
        """
        for batch_id, status in batch_status.items():
            if product_order_counts[batch_id] > 0:
                if status["batch_status"] == "waiting" or status["batch_status"] == "completed":
                    self._start_batch(status, ingredient_inventory)
                elif status["batch_status"] == "in_progress" and status["machine_stage"] == MACHINE_STAGE_IN_PROGRESS:
                    if datetime.now() - status["timestamp"] >= timedelta(seconds=30):
                        self._advance_batch_to_stage(status, MACHINE_STAGE_MACHINE_1)
                elif status["batch_status"] == "machine_1" and status["machine_stage"] == MACHINE_STAGE_MACHINE_1:
                    if datetime.now() - status["timestamp"] >= timedelta(seconds=30):
                        self._advance_batch_to_stage(status, MACHINE_STAGE_MACHINE_2)
                elif status["batch_status"] == "machine_2" and status["machine_stage"] == MACHINE_STAGE_MACHINE_2:
                    if datetime.now() - status["timestamp"] >= timedelta(seconds=30):
                        self._complete_batch(batch_id, status, product_order_counts)

                if any(machine_status[m_id]["machine_status"] == "stopped" for m_id in self.machine_ids):
                    self._fail_batch(batch_id, status, product_order_counts)

    def _start_batch(self, status, ingredient_inventory):
        """
        Start a new batch and update its status and ingredient inventory.

        Args:
            status (dict): Current status of the batch.
            ingredient_inventory (dict): Current ingredient inventory.
        """
        status["timestamp"] = datetime.now()
        status["batch_status"] = "in_progress"
        status["machine_stage"] = MACHINE_STAGE_IN_PROGRESS
        ingredient_inventory[self.ingredient_ids[0]]["quantity"] -= 1

    def _advance_batch_to_stage(self, status, stage):
        """
        Advance a batch to the specified stage.

        Args:
            status (dict): Current status of the batch.
            stage (int): Stage to advance the batch to.
        """
        status["timestamp"] = datetime.now()
        if stage == MACHINE_STAGE_MACHINE_1:
            status["batch_status"] = "machine_1"
        elif stage == MACHINE_STAGE_MACHINE_2:
            status["batch_status"] = "machine_2"
        status["machine_stage"] = stage

    def _complete_batch(self, batch_id, status, product_order_counts):
        """
        Complete a batch and update the order count.

        Args:
            batch_id (str): ID of the batch.
            status (dict): Current status of the batch.
            product_order_counts (dict): Current order counts for each product.
        """
        status["timestamp"] = datetime.now()
        status["batch_status"] = "completed"
        status["machine_stage"] = MACHINE_STAGE_COMPLETED
        product_order_counts[batch_id] -= 1

    def _fail_batch(self, batch_id, status, product_order_counts):
        """
        Fail a batch and update the order count.

        Args:
            batch_id (str): ID of the batch.
            status (dict): Current status of the batch.
            product_order_counts (dict): Current order counts for each product.
        """
        status["timestamp"] = datetime.now()
        status["batch_status"] = "failed"
        status["machine_stage"] = MACHINE_STAGE_WAITING
        product_order_counts[batch_id] -= 1

    def _generate_qa_inspection_notes(self, batch_status, product_order_counts):
        """
        Generate QA inspection notes for completed and failed batches.

        Args:
            batch_status (dict): Current status of each batch.
            product_order_counts (dict): Current order counts for each product.

        Returns:
            list: QA inspection results.
        """
        inspection_results = []
        for batch_id, status in batch_status.items():
            if status["batch_status"] == "completed":
                inspection_results.append({
                    "inspection_id": self.fake.uuid4(),
                    "timestamp": datetime.now(),
                    "batch_id": batch_id,
                    "inspection_result": "success",
                    "inspection_notes": "success - no issues",
                    "inspector_id": self.inspector_ids[0],
                    "embedding_vec": list(np.random.rand(5))
                })
            elif status["batch_status"] == "failed":
                inspection_results.append({
                    "inspection_id": self.fake.uuid4(),
                    "timestamp": datetime.now(),
                    "batch_id": batch_id,
                    "inspection_result": "failure",
                    "inspection_notes": f"failure due to machine stopping at stage {status['machine_stage']}",
                    "inspector_id": self.inspector_ids[0],
                    "embedding_vec": list(np.random.rand(5))
                })
                if product_order_counts[batch_id] > 0:
                    product_order_counts[batch_id] += 1
                    status["batch_status"] = "waiting"
        return inspection_results

    def _generate_final_orders(self, product_order_counts):
        """
        Generate final orders for products with remaining orders.

        Args:
            product_order_counts (dict): Current order counts for each product.

        Returns:
            list: Final orders.
        """
        final_orders = []
        for product_id, count in product_order_counts.items():
            if count > 0:
                final_orders.append({
                    "order_id": self.fake.uuid4(),
                    "timestamp": datetime.now(),
                    "product_id": product_id,
                    "quantity": 1,
                    "customer_id": self.fake.uuid4(),
                    "delivery_date": datetime.now() + timedelta(days=7)
                })
        return final_orders

    def _send_to_kafka(self, topic, data):
        """
        Send data to Kafka topic.

        Args:
            topic (str): Kafka topic to send data to.
            data (dict): Data to be sent.
        """
        try:
            json_data = json.dumps(data, default=self._json_serial)
            self.producer.produce(topic, key=str(datetime.now()), value=json_data, callback=self.delivery_report)
            self.producer.poll(0)  # Trigger delivery callbacks
        except Exception as e:
            print(f"Failed to send data to Kafka: {e}")

    def delivery_report(self, err, msg):
        """
        Delivery report callback.

        Args:
            err (KafkaError): The error (if any) that occurred.
            msg (Message): The message that was produced or failed.
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def _json_serial(self, obj):
        """
        JSON serializer for objects not serializable by default.

        Args:
            obj: Object to serialize.

        Returns:
            str: JSON serialized object.
        """
        if isinstance(obj, (datetime, timedelta)):
            return obj.isoformat()
        raise TypeError("Type not serializable")
