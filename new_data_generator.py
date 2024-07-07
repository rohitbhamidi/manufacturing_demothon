import pandas as pd
import numpy as np
from faker import Faker
from datetime import timedelta, datetime
import json
import time
from confluent_kafka import Producer
from collections import OrderedDict

class SmallDataGenerator:
    '''Generate small data for testing purposes. 

    The following are real-time data generated:
    - machine status: machine_id, timestamp, machine_status
    - ingredient inventory: ingredient_id, timestamp, ingredient_name, quantity
    - batch status: batch_id, timestamp, batch_status
    - QA inspection results: inspection_id, timestamp, batch_id, inspection_result, inspection_notes, inspector_id, embedding_vec
    - final orders: order_id, timestamp, product_id, quantity, customer_id, delivery_date

    The following are static data generated:
    - product catalog (what items are manufactured): product_id, product_name, category, price, ingredients_list
    - machine catalog (what machines are used): machine_id, machine_name, location, capacity
    - ingredient catalog (what ingredients are used): ingredient_id, ingredient_name, supplier, unit_price
    - inspector catalog (who inspects the products): inspector_id, inspector_name, inspector_title, reliability_score
    - process catalog (what processes are involved in making the products): process_id, process_name, process_json'''

    def __init__(self):
        self.fake = Faker()
        self.product_ids = []
        self.machine_ids = []
        self.ingredient_ids = []
        self.inspector_ids = []
        self.process_ids = []
        self.generate_unique_ids()
    
    def generate_unique_ids(self):
        self.product_ids = [self.fake.uuid4() for _ in range(2)]
        self.machine_ids = [self.fake.uuid4() for _ in range(2)]
        self.ingredient_ids = [self.fake.uuid4() for _ in range(3)]
        self.inspector_ids = [self.fake.uuid4() for _ in range(2)]
        self.process_ids = [self.fake.uuid4() for _ in range(2)]
    
    def generate_static_data(self):
        '''Generate static data for the following catalogs: product, machine, ingredient, inspector, process.'''

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

    def generate_machine_status(self, sleep_in_ms=1000, number_of_records=10):
        '''Generate machine status data in real-time.'''

        for i in range(number_of_records):
            machine_status = {id: {"timestamp": datetime.now(), "machine_status": self.fake.random_elements(elements=OrderedDict([('running', 0.99), ('stopped', 0.01)]), length=1, )} for id in self.machine_ids}
            yield machine_status
            time.sleep(sleep_in_ms / 1000)

    def generate_batch_status(self, sleep_in_ms=1000, number_of_records=10):
        '''Generate batch status data in real-time.'''

        machine_status_frequency = int(30000 / sleep_in_ms)  # Calculate the frequency of machine status output

        for i in range(number_of_records):
            batch_status = {id: {"timestamp": datetime.now(), "batch_status": self.fake.random_elements(elements=OrderedDict([('in_progress', 0.99), ('completed', 0.01)]), length=1, )} for id in self.product_ids}

            # Check if any machine is stopped
            machine_status = next(self.generate_machine_status(sleep_in_ms, 1))
            stopped_machines = [machine_id for machine_id, status in machine_status.items() if status['machine_status'][0] == 'stopped']

            # If any machine is stopped, assign batch status as failed_completed
            if stopped_machines:
                batch_status = {id: {"timestamp": datetime.now(), "batch_status": "failed_completed"} for id in self.product_ids}

            yield batch_status

            if i % machine_status_frequency == 0:  # Output machine status at the desired frequency
                yield machine_status

            time.sleep(30)  # Wait for 30 seconds for the next batch
