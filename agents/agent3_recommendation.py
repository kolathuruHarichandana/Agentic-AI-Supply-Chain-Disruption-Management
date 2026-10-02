import os`r`nimport json
import logging
from kafka import KafkaConsumer, KafkaProducer
from groq import Groq
from datetime import datetime

logging.basicConfig(filename='logs/agent3.log', level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

GROQ_KEY = os.getenv(GROQ_API_KEY)
client = Groq(api_key=GROQ_KEY)

NEARBY_SUPPLIERS = {
    'North America': ['ALT-NA-001 NorthAm Logistics','ALT-NA-002 QuickShip America'],
    'Europe':        ['ALT-EU-001 EuroFreight GmbH', 'ALT-EU-002 FastLogix Europe'],
    'Asia Pacific':  ['ALT-AP-001 AsiaPac Express',  'ALT-AP-002 Pacific Trade Co'],
    'Middle East':   ['ALT-ME-001 Gulf Logistics LLC','ALT-ME-002 MidEast Supply'],
    'South Asia':    ['ALT-SA-001 IndoLogix Pvt Ltd', 'ALT-SA-002 SouthAsia Freight'],
    'North':         ['ALT-IN-001 Blue Dart India',   'ALT-IN-002 DTDC Express'],
    'South':         ['ALT-IN-003 Delhivery South',   'ALT-IN-004 Ecom Express'],
    'East':          ['ALT-IN-005 Shadowfax East',    'ALT-IN-006 XpressBees'],
    'West':          ['ALT-IN-007 Shiprocket West',   'ALT-IN-008 Dunzo Logistics'],
}

consumer = KafkaConsumer(
    'root_causes',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("ðŸ›¡ï¸  Agent 3 â€” Decision Recommendation Agent RUNNING")
print("Listening to root_causes topic...\n")

for msg in consumer:
    event = msg.value
    region = event.get('region','North America')
    print(f"\n{'='*50}")
    print(f"Recommending for: {event.get('supplier_id')} | {region}")

    try:
        prompt = f"""You are a supply chain decision expert.
Disruption details:
- Supplier: {event.get('supplier_id')}
- Root Cause: {event.get('root_cause')}
- Risk Score: {event.get('risk_score')}
- Region: {region}

Give ONE specific mitigation action in one sentence."""

        resp = client.chat.completions.create(
            model="gemma2-9b-it",
            messages=[{"role":"user","content":prompt}],
            max_tokens=80)
        mitigation = resp.choices[0].message.content.strip()

        risk = float(event.get('risk_score',0))
        if risk > 0.8:   priority = "Critical"
        elif risk > 0.6: priority = "High"
        elif risk > 0.4: priority = "Medium"
        else:            priority = "Low"

    except:
        mitigation = "Activate backup supplier and reroute shipment immediately."
        priority = "High"

    alternatives = NEARBY_SUPPLIERS.get(region, NEARBY_SUPPLIERS['North America'])

    event['mitigation']   = mitigation
    event['priority']     = priority
    event['alternatives'] = alternatives
    event['decided_at']   = datetime.now().isoformat()
    event['agent']        = 'Agent3_DecisionRecommendation'

    print(f"ðŸ›¡ï¸  MITIGATION : {mitigation}")
    print(f"âš¡ PRIORITY   : {priority}")
    print(f"ðŸ“ ALTERNATIVES: {alternatives}")

    logging.info(f"DECISION: {event.get('supplier_id')} â†’ {priority} | {mitigation}")
    producer.send('decisions', event)
    print(f"âœ… Sent to decisions topic â†’ Dashboard will update!")
