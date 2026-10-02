import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import logging

logging.basicConfig(filename='logs/anomalies.log', level=logging.INFO)

consumer = KafkaConsumer(
    'supplier-delays',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Anomaly Detector running...")

for msg in consumer:
    event = msg.value
    expected = event['expected_delivery_days']
    actual = event['actual_delivery_days']
    delay = actual - expected
    risk_score = round(min(delay / 10, 1.0), 2)

    is_anomaly = delay > 5 or event['status'] in ['critical_delay', 'failed']

    event['delay_days'] = delay
    event['risk_score'] = risk_score
    event['is_anomaly'] = is_anomaly

    if is_anomaly:
        print(f"ANOMALY DETECTED: {event['supplier_id']} | delay={delay} days | risk={risk_score}")
        logging.info(f"ANOMALY: {event}")
        producer.send('anomalies', event)
    else:
        print(f"OK: {event['supplier_id']} | delay={delay} days")