import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

logging.basicConfig(filename='logs/agent1.log', level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

consumer = KafkaConsumer(
    'supplier-delays',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🔍 Agent 1 — Anomaly Detection Agent RUNNING")
print("Listening to supplier-delays topic...\n")

for msg in consumer:
    event = msg.value
    expected = int(event.get('expected_delivery_days', 3))
    actual   = int(event.get('actual_delivery_days', 3))
    delay    = actual - expected
    status   = event.get('status','')
    risk     = round(min(max(delay/10, 0), 1.0), 2)

    is_anomaly = (
        delay > 5 or
        status in ['critical_delay','failed'] or
        risk > 0.6
    )

    event['delay_days'] = delay
    event['risk_score'] = risk
    event['is_anomaly'] = is_anomaly
    event['detected_at'] = datetime.now().isoformat()
    event['agent'] = 'Agent1_AnomalyDetection'

    if is_anomaly:
        print(f"🚨 ANOMALY: {event['supplier_id']} | delay={delay}d | risk={risk}")
        logging.info(f"ANOMALY DETECTED: {event}")
        producer.send('anomalies', event)
    else:
        print(f"✅ OK: {event['supplier_id']} | delay={delay}d")