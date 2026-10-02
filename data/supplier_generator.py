import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

suppliers = ['SupplierA', 'SupplierB', 'SupplierC', 'SupplierD']
statuses = ['on_time', 'delayed', 'critical_delay', 'failed']

print("Starting supplier data generator...")

while True:
    event = {
        'timestamp': datetime.now().isoformat(),
        'supplier_id': random.choice(suppliers),
        'order_id': f'ORD-{random.randint(1000,9999)}',
        'expected_delivery_days': random.randint(2, 7),
        'actual_delivery_days': random.randint(1, 15),
        'status': random.choice(statuses),
        'quantity': random.randint(100, 1000),
        'region': random.choice(['North', 'South', 'East', 'West'])
    }
    producer.send('supplier-delays', event)
    print(f"Sent: {event['supplier_id']} - {event['status']}")
    time.sleep(2)