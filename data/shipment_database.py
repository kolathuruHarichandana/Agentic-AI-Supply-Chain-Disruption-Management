import csv
import json
import random
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ── Real-world shipment simulation ─────────────────────
SUPPLIERS = {
    'SUP-10234': {'name': 'Tata Logistics',      'region': 'South Asia',    'reliability': 0.85},
    'SUP-08765': {'name': 'DHL Express',          'region': 'Europe',        'reliability': 0.92},
    'SUP-04567': {'name': 'FedEx Asia Pacific',   'region': 'Asia Pacific',  'reliability': 0.88},
    'SUP-06789': {'name': 'Aramex Middle East',   'region': 'Middle East',   'reliability': 0.82},
    'SUP-03321': {'name': 'Blue Dart India',      'region': 'South Asia',    'reliability': 0.90},
    'SUP-11111': {'name': 'UPS North America',    'region': 'North America', 'reliability': 0.93},
    'SUP-22222': {'name': 'Maersk Shipping',      'region': 'Europe',        'reliability': 0.87},
}

PRODUCTS = [
    {'name': 'Electronic Components', 'category': 'Electronics', 'base_qty': 500},
    {'name': 'Raw Materials Steel',   'category': 'Industrial',  'base_qty': 2000},
    {'name': 'Pharmaceutical Drugs',  'category': 'Healthcare',  'base_qty': 300},
    {'name': 'Automotive Parts',      'category': 'Automotive',  'base_qty': 800},
    {'name': 'Textile Materials',     'category': 'Fashion',     'base_qty': 1500},
    {'name': 'Food Products',         'category': 'FMCG',        'base_qty': 1000},
    {'name': 'Chemical Compounds',    'category': 'Chemical',    'base_qty': 600},
]

WEATHER_RISKS = {
    'South Asia':    ['Heavy Monsoon','Cyclone Warning','Normal','Normal','Normal'],
    'Europe':        ['Snow Storm','Fog Alert','Normal','Normal','Normal'],
    'Asia Pacific':  ['Typhoon Warning','Heavy Rain','Normal','Normal','Normal'],
    'Middle East':   ['Sandstorm','Extreme Heat','Normal','Normal','Normal'],
    'North America': ['Hurricane Warning','Blizzard','Normal','Normal','Normal'],
}

DISRUPTION_EVENTS = [
    'Port congestion at major hub',
    'Customs clearance delay',
    'Vehicle breakdown',
    'Driver strike',
    'Warehouse fire',
    'System outage at logistics provider',
    'Road blockage due to accident',
    'Fuel shortage',
    'Normal operations',
    'Normal operations',
    'Normal operations',
    'Normal operations',
]

def generate_daily_shipments(date, num_shipments=20):
    shipments = []
    for i in range(num_shipments):
        supplier_id = random.choice(list(SUPPLIERS.keys()))
        supplier = SUPPLIERS[supplier_id]
        product = random.choice(PRODUCTS)
        reliability = supplier['reliability']
        weather = random.choice(WEATHER_RISKS[supplier['region']])
        disruption = random.choice(DISRUPTION_EVENTS)

        # Calculate realistic delays based on conditions
        base_days = random.randint(2, 7)
        if 'Warning' in weather or 'Storm' in weather or 'Cyclone' in weather:
            delay_factor = random.randint(3, 8)
        elif weather != 'Normal':
            delay_factor = random.randint(1, 4)
        elif disruption != 'Normal operations':
            delay_factor = random.randint(2, 6)
        else:
            delay_factor = 0 if random.random() < reliability else random.randint(1, 3)

        actual_days = base_days + delay_factor

        # Determine status
        if actual_days <= base_days:
            status = 'delivered'
            damage = 'none'
        elif actual_days <= base_days + 2:
            status = 'delayed'
            damage = 'none'
        elif actual_days <= base_days + 5:
            status = random.choice(['critical_delay', 'delayed'])
            damage = random.choice(['none', 'none', 'minor_damage'])
        else:
            status = random.choice(['failed', 'critical_delay'])
            damage = random.choice(['none', 'minor_damage', 'major_damage', 'total_loss'])

        qty = int(product['base_qty'] * random.uniform(0.5, 1.5))
        if damage == 'minor_damage':
            damaged_qty = int(qty * random.uniform(0.05, 0.15))
        elif damage == 'major_damage':
            damaged_qty = int(qty * random.uniform(0.3, 0.6))
        elif damage == 'total_loss':
            damaged_qty = qty
        else:
            damaged_qty = 0

        risk_score = round(min(delay_factor / 10 + (0.3 if damage != 'none' else 0), 1.0), 2)

        shipments.append({
            'date': date.strftime('%Y-%m-%d'),
            'order_id': f'ORD-{date.strftime("%Y%m%d")}-{i+1:03d}',
            'supplier_id': supplier_id,
            'supplier_name': supplier['name'],
            'product': product['name'],
            'category': product['category'],
            'region': supplier['region'],
            'expected_days': base_days,
            'actual_days': actual_days,
            'delay_days': delay_factor,
            'status': status,
            'quantity': qty,
            'damaged_quantity': damaged_qty,
            'damage_type': damage,
            'weather_condition': weather,
            'disruption_event': disruption,
            'risk_score': risk_score,
            'timestamp': datetime.combine(date, datetime.now().time()).isoformat()
        })
    return shipments

def save_daily_csv(shipments, date):
    filename = f"data/shipments_{date.strftime('%Y_%m_%d')}.csv"
    fieldnames = list(shipments[0].keys())
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(shipments)
    print(f"✅ Saved: {filename} ({len(shipments)} shipments)")
    return filename

def generate_summary(shipments, date):
    total       = len(shipments)
    delivered   = sum(1 for s in shipments if s['status'] == 'delivered')
    delayed     = sum(1 for s in shipments if s['status'] == 'delayed')
    critical    = sum(1 for s in shipments if s['status'] == 'critical_delay')
    failed      = sum(1 for s in shipments if s['status'] == 'failed')
    damaged     = sum(1 for s in shipments if s['damage_type'] != 'none')
    total_loss  = sum(1 for s in shipments if s['damage_type'] == 'total_loss')
    total_dmg_q = sum(s['damaged_quantity'] for s in shipments)
    high_risk   = sum(1 for s in shipments if s['risk_score'] > 0.7)

    print(f"\n{'='*55}")
    print(f"  DAILY SHIPMENT REPORT — {date.strftime('%B %d, %Y')}")
    print(f"{'='*55}")
    print(f"  Total Shipments   : {total}")
    print(f"  ✅ Delivered       : {delivered} ({round(delivered/total*100)}%)")
    print(f"  🟡 Delayed        : {delayed}")
    print(f"  🔴 Critical Delay : {critical}")
    print(f"  ❌ Failed         : {failed}")
    print(f"  📦 Damaged Orders : {damaged}")
    print(f"  💥 Total Loss     : {total_loss}")
    print(f"  📉 Damaged Units  : {total_dmg_q:,}")
    print(f"  ⚠️  High Risk      : {high_risk}")
    print(f"{'='*55}\n")

def stream_to_kafka(shipments):
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for s in shipments:
            producer.send('supplier-delays', s)
            time.sleep(0.5)
        print(f"✅ Streamed {len(shipments)} records to Kafka")
    except Exception as e:
        print(f"⚠️ Kafka not available: {e}")

def generate_week_history():
    print("Generating 7 days of historical shipment data...\n")
    today = datetime.now().date()
    for i in range(6, -1, -1):
        date = today - timedelta(days=i)
        shipments = generate_daily_shipments(date, num_shipments=20)
        save_daily_csv(shipments, date)
        generate_summary(shipments, date)
        time.sleep(0.2)

if __name__ == "__main__":
    print("🚚 SupplyAI — Shipment Database Generator")
    print("="*55)
    print("1. Generate 7 days history + stream today's data")
    print("="*55)

    # Generate historical data
    generate_week_history()

    # Stream today's data to Kafka
    today = datetime.now().date()
    today_shipments = generate_daily_shipments(today, num_shipments=20)
    print("\nStreaming today's shipments to Kafka...")
    stream_to_kafka(today_shipments)

    print("\n✅ Done! Check data/ folder for CSV files")
    print("Files created:")
    for f in os.listdir('data'):
        if f.startswith('shipments_'):
            print(f"  📄 data/{f}")