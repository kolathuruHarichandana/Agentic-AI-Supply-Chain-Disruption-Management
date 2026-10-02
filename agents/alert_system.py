import os`r`nimport json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from groq import Groq

logging.basicConfig(
    filename='logs/alerts.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

GROQ_KEY = os.getenv(GROQ_API_KEY)

# Nearby alternative suppliers database
NEARBY_SUPPLIERS = {
    'North America': [
        {'id': 'ALT-NA-001', 'name': 'NorthAm Logistics Co', 'distance': '45km', 'rating': 4.8, 'eta': '2 days'},
        {'id': 'ALT-NA-002', 'name': 'QuickShip America',    'distance': '78km', 'rating': 4.6, 'eta': '3 days'},
    ],
    'Europe': [
        {'id': 'ALT-EU-001', 'name': 'EuroFreight GmbH',     'distance': '32km', 'rating': 4.9, 'eta': '1 day'},
        {'id': 'ALT-EU-002', 'name': 'FastLogix Europe',     'distance': '91km', 'rating': 4.5, 'eta': '2 days'},
    ],
    'Asia Pacific': [
        {'id': 'ALT-AP-001', 'name': 'AsiaPac Express',      'distance': '56km', 'rating': 4.7, 'eta': '2 days'},
        {'id': 'ALT-AP-002', 'name': 'Pacific Trade Co',     'distance': '112km','rating': 4.4, 'eta': '4 days'},
    ],
    'Middle East': [
        {'id': 'ALT-ME-001', 'name': 'Gulf Logistics LLC',   'distance': '28km', 'rating': 4.6, 'eta': '1 day'},
        {'id': 'ALT-ME-002', 'name': 'MidEast Supply Chain', 'distance': '65km', 'rating': 4.3, 'eta': '3 days'},
    ],
    'South Asia': [
        {'id': 'ALT-SA-001', 'name': 'IndoLogix Pvt Ltd',    'distance': '41km', 'rating': 4.7, 'eta': '2 days'},
        {'id': 'ALT-SA-002', 'name': 'SouthAsia Freight',    'distance': '89km', 'rating': 4.5, 'eta': '3 days'},
    ],
}

def send_desktop_notification(title, message):
    try:
        from plyer import notification
        notification.notify(
            title=title,
            message=message,
            app_icon=None,
            timeout=8,
        )
        print(f"ðŸ”” NOTIFICATION SENT: {title}")
    except Exception as e:
        print(f"ðŸ”” ALERT: {title} â€” {message}")

def get_nearby_suppliers(region):
    suppliers = NEARBY_SUPPLIERS.get(region, NEARBY_SUPPLIERS['North America'])
    return suppliers

def get_ai_recommendation(event):
    try:
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""A shipment has FAILED in the supply chain:
- Supplier: {event['supplier_id']}
- Order: {event['order_id']}  
- Region: {event['region']}
- Delay: {event['delay_days']} days
- Quantity: {event['quantity']} units

Give a 2-line urgent action message for the supply chain manager."""
        resp = client.chat.completions.create(
            model="gemma2-9b-it",
            messages=[{"role":"user","content":prompt}],
            max_tokens=80)
        return resp.choices[0].message.content.strip()
    except:
        return "Immediate action required. Contact backup supplier and reroute shipment."

def process_failed_shipment(event):
    order_id = event.get('order_id','Unknown')
    supplier = event.get('supplier_id','Unknown')
    region   = event.get('region','Unknown')
    delay    = event.get('delay_days',0)
    qty      = event.get('quantity',0)

    print(f"\n{'='*60}")
    print(f"ðŸš¨ FAILED SHIPMENT DETECTED")
    print(f"{'='*60}")
    print(f"  Order ID  : {order_id}")
    print(f"  Supplier  : {supplier}")
    print(f"  Region    : {region}")
    print(f"  Delay     : {delay} days")
    print(f"  Quantity  : {qty} units")
    print(f"  Time      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Send desktop notification
    send_desktop_notification(
        f"ðŸš¨ Shipment Failed â€” {order_id}",
        f"Supplier: {supplier}\nRegion: {region}\nDelay: {delay} days\n{qty} units affected"
    )

    # Get AI recommendation
    print(f"\nðŸ¤– AI RECOMMENDATION:")
    ai_msg = get_ai_recommendation(event)
    print(f"  {ai_msg}")

    # Show nearby alternative suppliers
    alternatives = get_nearby_suppliers(region)
    print(f"\nðŸ“ NEARBY ALTERNATIVE SUPPLIERS ({region}):")
    print(f"  {'ID':<12} {'Name':<25} {'Distance':<10} {'Rating':<8} {'ETA'}")
    print(f"  {'-'*65}")
    for s in alternatives:
        print(f"  {s['id']:<12} {s['name']:<25} {s['distance']:<10} â­{s['rating']:<7} {s['eta']}")

    # Log everything
    logging.info(f"FAILED SHIPMENT: {order_id} | Supplier: {supplier} | Region: {region} | Delay: {delay}d")
    logging.info(f"AI RECOMMENDATION: {ai_msg}")
    logging.info(f"ALTERNATIVES SUGGESTED: {[s['id'] for s in alternatives]}")

    print(f"\nâœ… Alert logged and alternatives suggested")
    print(f"{'='*60}\n")

    return {
        'order_id': order_id,
        'supplier': supplier,
        'region': region,
        'ai_recommendation': ai_msg,
        'alternative_suppliers': alternatives,
        'timestamp': datetime.now().isoformat()
    }

# Main listener
print("ðŸš¨ Failed Shipment Alert System â€” RUNNING")
print("Listening for failed deliveries on Kafka...\n")

consumer = KafkaConsumer(
    'anomalies',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=3000,
    auto_offset_reset='earliest'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

processed = 0
for msg in consumer:
    event = msg.value
    status = event.get('status','')
    risk   = event.get('risk_score',0)

    if status in ['failed','critical_delay'] or risk > 0.8:
        result = process_failed_shipment(event)
        producer.send('failed_shipments', result)
        processed += 1

        if processed >= 3:
            print("Processed 3 failed shipments. Waiting for new ones...")
            break

print(f"\nTotal failed shipments processed: {processed}")
print("Check logs/alerts.log for full history")
