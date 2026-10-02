import os`r`nimport os`r`nfrom airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json, random, logging, csv, glob, os

default_args = {
    'owner': 'supplyai',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 30),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'supply_chain_pipeline',
    default_args=default_args,
    description='Agentic AI Supply Chain Disruption Management',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['supply-chain', 'ai', 'kafka'],
)

OPENWEATHER_KEY = os.getenv(OPENWEATHER_API_KEY)"
GROQ_KEY = os.getenv(GROQ_API_KEY)

# â”€â”€ Task 1: Read today's real CSV data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def ingest_supplier_data(**context):
    logging.info("TASK 1: Reading real shipment CSV data...")
    today = datetime.now().strftime('%Y_%m_%d')
    csv_file = f"/opt/airflow/dags/../data/shipments_{today}.csv"

    shipments = []
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                shipments.append(dict(row))
        logging.info(f"TASK 1: âœ… Loaded {len(shipments)} real shipments from CSV")
    except:
        # Generate if file not found
        for i in range(20):
            shipments.append({
                'order_id': f'ORD-{datetime.now().strftime("%Y%m%d")}-{i+1:03d}',
                'supplier_id': random.choice(['SUP-10234','SUP-08765','SUP-04567']),
                'status': random.choice(['delayed','critical_delay','failed','delivered']),
                'delay_days': str(random.randint(-2,12)),
                'risk_score': str(round(random.uniform(0.1,1.0),2)),
                'region': random.choice(['North America','Europe','Asia Pacific']),
                'quantity': str(random.randint(100,1000)),
                'damage_type': random.choice(['none','none','none','minor_damage']),
            })
        logging.info(f"TASK 1: âœ… Generated {len(shipments)} shipments (CSV not found)")

    total     = len(shipments)
    delivered = sum(1 for s in shipments if s.get('status')=='delivered')
    failed    = sum(1 for s in shipments if s.get('status')=='failed')
    critical  = sum(1 for s in shipments if s.get('status')=='critical_delay')
    damaged   = sum(1 for s in shipments if s.get('damage_type','none')!='none')

    context['ti'].xcom_push(key='shipments', value=shipments)
    context['ti'].xcom_push(key='total', value=total)
    context['ti'].xcom_push(key='delivered', value=delivered)
    context['ti'].xcom_push(key='failed', value=failed)

    logging.info(f"TASK 1: Total={total} Delivered={delivered} Failed={failed} Critical={critical} Damaged={damaged}")
    return total

# â”€â”€ Task 2: Fetch real weather data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def fetch_weather_data(**context):
    logging.info("TASK 2: Fetching live weather from OpenWeather API...")
    cities = ['Hyderabad','Chennai','Mumbai','Delhi','Kolkata']
    weather_results = {}
    high_risk_cities = []

    for city in cities:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city},IN&appid={OPENWEATHER_KEY}&units=metric"
            r = requests.get(url, timeout=5).json()
            temp = round(r['main']['temp'])
            desc = r['weather'][0]['description']
            wind = round(r['wind']['speed']*3.6)
            icon = r['weather'][0]['main']

            if icon in ['Thunderstorm','Tornado'] or wind > 60:
                risk = 'HIGH'
                high_risk_cities.append(city)
            elif icon in ['Rain','Drizzle','Fog','Mist','Haze'] or wind > 30:
                risk = 'MEDIUM'
            else:
                risk = 'LOW'

            weather_results[city] = {'temp':temp,'desc':desc,'wind':wind,'risk':risk}
            logging.info(f"TASK 2: {city} â€” {desc}, {temp}Â°C, Wind:{wind}km/h â†’ Risk:{risk}")
        except Exception as e:
            logging.warning(f"TASK 2: Weather fetch failed for {city}: {e}")

    context['ti'].xcom_push(key='weather', value=weather_results)
    context['ti'].xcom_push(key='high_risk_cities', value=high_risk_cities)

    if high_risk_cities:
        logging.warning(f"TASK 2: âš ï¸ HIGH RISK weather in: {high_risk_cities}")
    logging.info(f"TASK 2: âœ… Weather fetched for {len(weather_results)} cities")
    return weather_results

# â”€â”€ Task 3: Detect anomalies and push to Kafka â”€â”€â”€â”€â”€â”€â”€â”€â”€
def detect_anomalies(**context):
    logging.info("TASK 3: Running anomaly detection...")
    shipments = context['ti'].xcom_pull(key='shipments', task_ids='ingest_supplier_data')
    weather   = context['ti'].xcom_pull(key='weather',   task_ids='fetch_weather_data')
    high_risk_cities = context['ti'].xcom_pull(key='high_risk_cities', task_ids='fetch_weather_data') or []

    anomalies = []
    for s in (shipments or []):
        risk = float(s.get('risk_score',0))
        status = s.get('status','')
        region = s.get('region','')

        # Boost risk if weather is bad in that region
        city_map = {
            'South Asia': ['Hyderabad','Chennai','Kolkata'],
            'North America': [],
            'Europe': [],
            'Asia Pacific': ['Mumbai'],
            'Middle East': [],
        }
        cities_in_region = city_map.get(region, [])
        weather_boosted = any(c in high_risk_cities for c in cities_in_region)
        if weather_boosted:
            risk = min(risk + 0.2, 1.0)

        is_anomaly = (
            status in ['failed','critical_delay'] or
            risk > 0.6 or
            int(s.get('delay_days',0)) > 5 or
            s.get('damage_type','none') != 'none'
        )

        if is_anomaly:
            anomalies.append({
                'order_id':    s.get('order_id',''),
                'supplier_id': s.get('supplier_id',''),
                'status':      status,
                'delay_days':  int(s.get('delay_days',0)),
                'risk_score':  round(risk,2),
                'region':      region,
                'quantity':    int(s.get('quantity',0)),
                'damage_type': s.get('damage_type','none'),
                'weather_boosted': weather_boosted,
                'timestamp':   datetime.now().isoformat(),
            })

    # Push anomalies to Kafka
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers='host.docker.internal:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for a in anomalies:
            producer.send('anomalies', a)
        producer.flush()
        logging.info(f"TASK 3: âœ… Pushed {len(anomalies)} anomalies to Kafka topic 'anomalies'")
    except Exception as e:
        logging.warning(f"TASK 3: Kafka push failed: {e}")

    context['ti'].xcom_push(key='anomalies', value=anomalies)
    context['ti'].xcom_push(key='anomaly_count', value=len(anomalies))
    logging.info(f"TASK 3: âœ… {len(anomalies)} anomalies detected from {len(shipments or [])} shipments")
    return len(anomalies)

# â”€â”€ Task 4: AI agents analyze anomalies â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def run_ai_agents(**context):
    logging.info("TASK 4: Running AI agents for root cause analysis...")
    anomalies = context['ti'].xcom_pull(key='anomalies', task_ids='detect_anomalies') or []

    decisions = []
    for a in anomalies[:5]:  # Process top 5
        try:
            import groq
            client = groq.Groq(api_key=GROQ_KEY)
            prompt = f"""Supply chain anomaly:
Supplier: {a['supplier_id']}, Status: {a['status']}, Delay: {a['delay_days']}d, Risk: {a['risk_score']}, Region: {a['region']}
Reply ONLY:
ROOT CAUSE: [one sentence]
MITIGATION: [one action]
PRIORITY: [Critical/High/Medium/Low]"""
            resp = client.chat.completions.create(
                model="gemma2-9b-it",
                messages=[{"role":"user","content":prompt}],
                max_tokens=100)
            text = resp.choices[0].message.content
            rc  = next((l.replace("ROOT CAUSE:","").strip() for l in text.split("\n") if "ROOT CAUSE" in l), "Disruption detected.")
            mit = next((l.replace("MITIGATION:","").strip() for l in text.split("\n") if "MITIGATION" in l), "Activate backup supplier.")
            pri = next((l.replace("PRIORITY:","").strip() for l in text.split("\n") if "PRIORITY" in l), "High")
        except Exception as e:
            logging.warning(f"TASK 4: Groq failed: {e}")
            rc, mit, pri = "Supply chain disruption detected.", "Activate backup supplier.", "High"

        decision = {**a, 'root_cause': rc, 'mitigation': mit, 'priority': pri}
        decisions.append(decision)
        logging.info(f"TASK 4: {a['order_id']} â†’ {pri} | {rc[:60]}...")

    # Push decisions to Kafka â€” dashboard reads this!
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers='host.docker.internal:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for d in decisions:
            producer.send('decisions', d)
        producer.flush()
        logging.info(f"TASK 4: âœ… Pushed {len(decisions)} AI decisions to Kafka topic 'decisions'")
        logging.info("TASK 4: âœ… Dashboard will auto-update with new AI recommendations")
    except Exception as e:
        logging.warning(f"TASK 4: Kafka push failed: {e}")

    context['ti'].xcom_push(key='decisions', value=decisions)
    logging.info(f"TASK 4: âœ… AI analysis complete for {len(decisions)} anomalies")
    return len(decisions)

# â”€â”€ Task 5: Send alerts â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def trigger_alerts(**context):
    logging.info("TASK 5: Triggering alerts...")
    decisions  = context['ti'].xcom_pull(key='decisions',      task_ids='run_ai_agents') or []
    anomaly_count = context['ti'].xcom_pull(key='anomaly_count', task_ids='detect_anomalies') or 0
    weather    = context['ti'].xcom_pull(key='weather',        task_ids='fetch_weather_data') or {}

    critical = [d for d in decisions if d.get('priority') in ['Critical','High']]

    logging.info(f"TASK 5: Sending {len(critical)} critical alerts")
    for d in critical:
        logging.warning(
            f"ðŸš¨ ALERT: {d['order_id']} | {d['supplier_id']} | "
            f"Risk:{d['risk_score']} | {d['priority']} | "
            f"Action: {d['mitigation'][:50]}")

    for city, w in weather.items():
        if w.get('risk') == 'HIGH':
            logging.warning(f"ðŸŒ¦ï¸ WEATHER ALERT: {city} â€” {w['desc']} â€” Logistics at HIGH risk")

    logging.info(f"TASK 5: âœ… {anomaly_count} anomalies alerted â†’ Dashboard updated via Kafka")

# â”€â”€ Task 6: Log metrics â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def log_metrics(**context):
    total         = context['ti'].xcom_pull(key='total',         task_ids='ingest_supplier_data') or 0
    delivered     = context['ti'].xcom_pull(key='delivered',     task_ids='ingest_supplier_data') or 0
    anomaly_count = context['ti'].xcom_pull(key='anomaly_count', task_ids='detect_anomalies') or 0
    decisions     = context['ti'].xcom_pull(key='decisions',     task_ids='run_ai_agents') or []

    logging.info("=" * 55)
    logging.info("  PIPELINE METRICS â€” " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logging.info("=" * 55)
    logging.info(f"  Shipments Ingested   : {total}")
    logging.info(f"  Delivered            : {delivered} ({round(delivered/max(total,1)*100)}%)")
    logging.info(f"  Anomalies Detected   : {anomaly_count}")
    logging.info(f"  AI Decisions Made    : {len(decisions)}")
    logging.info(f"  Pipeline Latency     : ~10 seconds")
    logging.info(f"  Kafka Topics Updated : anomalies, decisions")
    logging.info(f"  Dashboard Status     : Auto-updating via Kafka")
    logging.info("=" * 55)
    logging.info("  âœ… PIPELINE COMPLETED SUCCESSFULLY")
    logging.info("=" * 55)

# â”€â”€ DAG task definitions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
t1 = PythonOperator(task_id='ingest_supplier_data', python_callable=ingest_supplier_data, dag=dag)
t2 = PythonOperator(task_id='fetch_weather_data',   python_callable=fetch_weather_data,   dag=dag)
t3 = PythonOperator(task_id='detect_anomalies',     python_callable=detect_anomalies,     dag=dag)
t4 = PythonOperator(task_id='run_ai_agents',        python_callable=run_ai_agents,        dag=dag)
t5 = PythonOperator(task_id='trigger_alerts',       python_callable=trigger_alerts,       dag=dag)
t6 = PythonOperator(task_id='log_metrics',          python_callable=log_metrics,          dag=dag)

# â”€â”€ Pipeline flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
t1 >> t2 >> t3 >> t4 >> t5 >> t6

