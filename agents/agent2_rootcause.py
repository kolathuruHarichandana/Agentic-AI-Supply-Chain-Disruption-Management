import os`r`nimport json
import logging
from kafka import KafkaConsumer, KafkaProducer
from groq import Groq
from datetime import datetime

logging.basicConfig(filename='logs/agent2.log', level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

GROQ_KEY = os.getenv(GROQ_API_KEY)
client = Groq(api_key=GROQ_KEY)

consumer = KafkaConsumer(
    'anomalies',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("ðŸ”Ž Agent 2 â€” Root Cause Analysis Agent RUNNING")
print("Listening to anomalies topic...\n")

for msg in consumer:
    event = msg.value
    print(f"\n{'='*50}")
    print(f"Analyzing: {event.get('supplier_id')} | risk={event.get('risk_score')}")

    try:
        prompt = f"""You are a supply chain root cause analysis expert.
Anomaly detected:
- Supplier: {event.get('supplier_id')}
- Status: {event.get('status')}
- Delay: {event.get('delay_days')} days
- Risk Score: {event.get('risk_score')}
- Region: {event.get('region')}

Identify the ROOT CAUSE only in one clear sentence."""

        resp = client.chat.completions.create(
            model="gemma2-9b-it",
            messages=[{"role":"user","content":prompt}],
            max_tokens=80)
        root_cause = resp.choices[0].message.content.strip()
    except:
        root_cause = "Supply chain disruption detected in logistics corridor."

    event['root_cause'] = root_cause
    event['analyzed_at'] = datetime.now().isoformat()
    event['agent'] = 'Agent2_RootCauseAnalysis'

    print(f"ðŸ” ROOT CAUSE: {root_cause}")
    logging.info(f"ROOT CAUSE: {event.get('supplier_id')} â†’ {root_cause}")

    producer.send('root_causes', event)
    print(f"âœ… Sent to root_causes topic")
