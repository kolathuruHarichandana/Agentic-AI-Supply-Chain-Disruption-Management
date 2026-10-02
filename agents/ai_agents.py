import os`r`n

from groq import Groq
from kafka import KafkaConsumer, KafkaProducer
import json

# GROQ API KEY

GROQ_API_KEY = os.getenv("GROQ_API_KEY")


# Create Groq client
client = Groq(api_key=GROQ_API_KEY)

# Kafka Consumer
consumer = KafkaConsumer(
    'anomalies',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# AI Analysis Function
def analyze_anomaly(event):

    prompt = f"""
You are a supply chain expert AI agent.

Anomaly detected:
- Supplier: {event['supplier_id']}
- Status: {event['status']}
- Delay: {event['delay_days']} days
- Risk Score: {event['risk_score']}
- Region: {event['region']}

1. Root Cause: Most likely cause?
2. Mitigation: Give 2 specific actions.
3. Priority: High / Medium / Low
"""

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        max_tokens=300
    )

    return response.choices[0].message.content

# Start AI Agent
print("AI Agent running - waiting for anomalies...")

for msg in consumer:

    event = msg.value

    print(f"\n{'='*50}")
    print(f"ANOMALY: {event['supplier_id']} | risk={event['risk_score']}")

    analysis = analyze_anomaly(event)

    print(f"\nAI ANALYSIS:\n{analysis}")

    event['ai_analysis'] = analysis

    producer.send('decisions', event)
