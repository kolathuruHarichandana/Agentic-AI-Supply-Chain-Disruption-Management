# 🚚 Agentic AI for Smart Supply Chain Disruption Management

## 📌 Project Overview

This project implements an Agentic AI-powered Supply Chain Management System that monitors shipment data in real time, detects anomalies, analyzes disruption causes, recommends mitigation strategies, and displays insights through an interactive dashboard.

The system combines Apache Airflow, Apache Kafka, AI Agents, Weather API, PostgreSQL, and Streamlit to automate the complete supply chain monitoring workflow.

---

# 🎯 Problem Statement

Modern supply chains experience disruptions due to:

- Supplier delays
- Weather conditions
- Damaged shipments
- Logistics failures
- Inventory shortages

Traditional monitoring systems are reactive and require manual intervention.

This project provides an AI-driven automated monitoring and decision support system.

---

# 🚀 Features

- Real-time shipment monitoring
- Kafka-based data streaming
- Airflow workflow orchestration
- AI-powered anomaly detection
- Root cause analysis
- Mitigation recommendations
- Weather API integration
- Interactive Streamlit Dashboard
- PostgreSQL database integration
- Automatic alert generation

---

# 🛠️ Technologies Used

- Python
- Apache Airflow
- Apache Kafka
- PostgreSQL
- Docker
- Streamlit
- OpenWeather API
- Groq LLM API
- Pandas
- JSON

---

# 📁 Project Structure

```
supply-chain-ai/
│
├── agents/
│   ├── agent1_anomaly.py
│   ├── agent2_rootcause.py
│   ├── agent3_recommendation.py
│   ├── ai_agents.py
│   └── alert_system.py
│
├── dashboard/
│   └── app.py
│
├── data/
│   ├── shipment_database.py
│   ├── supplier_generator.py
│   └── shipments_*.csv
│
├── orchestration/
│   └── supply_chain_dag.py
│
├── processing/
│   └── anomaly_detector.py
│
├── logs/
│
├── docker-compose.yml
│
└── README.md
```

---

# ⚙️ System Architecture

```
Shipment Generator
        │
        ▼
Apache Kafka
        │
        ▼
Apache Airflow DAG
        │
 ┌──────┼────────┐
 │      │        │
 ▼      ▼        ▼
Agent1 Agent2 Agent3
 │
 ▼
Alert System
 │
 ▼
PostgreSQL
 │
 ▼
Streamlit Dashboard
```

---

# ▶️ How to Run

## 1. Start Docker Containers

```bash
docker compose up -d
```

---

## 2. Generate Shipment Data

```bash
python data/shipment_database.py
```

---

## 3. Start Dashboard

```bash
streamlit run dashboard/app.py
```

Dashboard URL

```
http://localhost:8501
```

---

## 4. Open Airflow

```
http://localhost:8080
```

Trigger the DAG:

```
supply_chain_pipeline
```

---

# 📊 Dashboard Modules

- Shipment Summary
- Risk Analysis
- Weather Monitoring
- Damage Analysis
- Regional Disruptions
- Live Shipment Monitor
- AI Recommendations

---

# 🤖 AI Agents

### Agent 1

Detects shipment anomalies.

### Agent 2

Analyzes disruption causes.

### Agent 3

Generates mitigation recommendations.

---

# 🌦 Weather Integration

The project integrates the OpenWeather API to monitor weather conditions that may impact logistics and shipment routes.

---

# 📈 Workflow

1. Generate shipment data
2. Stream records using Kafka
3. Airflow triggers the pipeline
4. AI agents process shipment events
5. Alerts are generated
6. Results are stored
7. Dashboard visualizes the insights

---

# 📌 Results

- Real-time shipment monitoring
- Automated anomaly detection
- AI-based decision support
- Live dashboard visualization
- Improved disruption management

---

# 📚 Future Enhancements

- GPS tracking integration
- Predictive demand forecasting
- IoT sensor integration
- Multi-cloud deployment
- Mobile application support

---

# 👨‍💻 Author

**K. Harichandana**

B.Tech Computer Science Engineering

Amrita Vishwa Vidyapeetham

---

# 📄 License

This project was developed as part of the **TCS iON Agentic AI for Data Engineering Capstone Project** for educational purposes.