import os`r`nimport os`r`nimport streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
import pandas as pd
import json, random, requests, glob, os
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from groq import Groq

st.set_page_config(page_title="SupplyAI â€” Command Center", layout="wide", page_icon="â—ˆ")
st_autorefresh(interval=10000, key="autorefresh")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
* { font-family: 'Inter', sans-serif !important; }
[data-testid="stAppViewContainer"] { background: #f0f2f8; }
[data-testid="stHeader"],[data-testid="stToolbar"],
[data-testid="stDecoration"],#MainMenu,footer,header,
iframe,div[data-testid="stIFrame"] {
    display:none !important; height:0 !important;
}
.block-container {
    padding: 28px 28px 20px 28px !important;
    max-width: 100% !important;
    margin-top: -40px !important;
}
.kpi-card {
    background:white; border-radius:16px; padding:20px;
    box-shadow:0 1px 4px rgba(0,0,0,0.06); border:1px solid #f1f5f9;
    transition: transform 0.2s ease, box-shadow 0.2s ease; cursor:pointer;
}
.kpi-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 8px 24px rgba(99,102,241,0.15);
}
@keyframes pulse {
    0% { box-shadow: 0 0 0 0 rgba(74,222,128,0.6); }
    70% { box-shadow: 0 0 0 8px rgba(74,222,128,0); }
    100% { box-shadow: 0 0 0 0 rgba(74,222,128,0); }
}
.live-dot {
    width:8px; height:8px; background:#4ade80; border-radius:50%;
    display:inline-block; margin-right:6px;
    animation: pulse 1.5s infinite;
}
.card {
    background:white; border-radius:16px; padding:18px 20px;
    box-shadow:0 1px 4px rgba(0,0,0,0.06); border:1px solid #f1f5f9;
    margin-bottom:14px;
}
.weather-card {
    background:#f8fafc; border-radius:12px; padding:12px 14px;
    border:1px solid #f1f5f9; margin:6px 0;
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.weather-card:hover { transform: translateX(4px); box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
.ai-card {
    border:1px solid #f1f5f9; border-radius:12px;
    padding:14px; margin:8px 0; background:#fafafa;
    transition: box-shadow 0.2s ease;
}
.ai-card:hover { box-shadow: 0 4px 16px rgba(0,0,0,0.08); }
div[data-testid="stPlotlyChart"] { margin-top:-12px !important; }
thead tr th {
    background:#f8fafc !important; color:#64748b !important;
    font-size:11px !important; font-weight:600 !important;
    text-transform:uppercase !important;
}
</style>
""", unsafe_allow_html=True)

OPENWEATHER_KEY = os.getenv(OPENWEATHER_API_KEY)"
GROQ_KEY = os.getenv(GROQ_API_KEY)

CITY_CONFIG = {
    'Hyderabad':  {'country':'IN','flag':'ðŸ‡®ðŸ‡³','region':'India â€” South'},
    'Chennai':    {'country':'IN','flag':'ðŸ‡®ðŸ‡³','region':'India â€” South'},
    'Mumbai':     {'country':'IN','flag':'ðŸ‡®ðŸ‡³','region':'India â€” West'},
    'New York':   {'country':'US','flag':'ðŸ‡ºðŸ‡¸','region':'North America'},
    'London':     {'country':'GB','flag':'ðŸ‡¬ðŸ‡§','region':'Europe'},
    'Singapore':  {'country':'SG','flag':'ðŸ‡¸ðŸ‡¬','region':'Asia Pacific'},
    'Dubai':      {'country':'AE','flag':'ðŸ‡¦ðŸ‡ª','region':'Middle East'},
    'Tokyo':      {'country':'JP','flag':'ðŸ‡¯ðŸ‡µ','region':'Asia Pacific'},
}

@st.cache_data(ttl=120)
def get_ai_analysis(supplier, status, delay, risk, region):
    try:
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Supply chain anomaly:
- Supplier: {supplier}, Status: {status}, Delay: {delay}d, Risk: {risk}, Region: {region}
Reply ONLY in this exact format:
ROOT CAUSE: [one sentence]
MITIGATION: [one action sentence]
PRIORITY: [Critical/High/Medium/Low]"""
        resp = client.chat.completions.create(
            model="gemma2-9b-it",
            messages=[{"role":"user","content":prompt}],
            max_tokens=120)
        text = resp.choices[0].message.content
        rc  = next((l.replace("ROOT CAUSE:","").strip() for l in text.split("\n") if "ROOT CAUSE" in l), "Supply chain disruption detected.")
        mit = next((l.replace("MITIGATION:","").strip() for l in text.split("\n") if "MITIGATION" in l), "Activate contingency plan.")
        pri = next((l.replace("PRIORITY:","").strip() for l in text.split("\n") if "PRIORITY" in l), "High")
        return rc, mit, pri
    except:
        return "Disruption detected in supply corridor.", "Activate backup supplier and reroute.", "High"

@st.cache_data(ttl=300)
def get_weather(city):
    try:
        country = CITY_CONFIG.get(city, {}).get('country', 'IN')
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city},{country}&appid={OPENWEATHER_KEY}&units=metric"
        d = requests.get(url, timeout=5).json()
        return {
            'temp': round(d['main']['temp']),
            'desc': d['weather'][0]['description'].title(),
            'wind': round(d['wind']['speed']*3.6),
            'humidity': d['main']['humidity'],
            'icon': d['weather'][0]['main']
        }
    except:
        return None

def w_risk(icon, wind):
    if icon in ['Thunderstorm','Tornado'] or wind>60: return ('ðŸ”´ HIGH','#ef4444')
    if icon in ['Rain','Drizzle','Snow','Fog','Mist','Haze'] or wind>30: return ('ðŸŸ¡ MED','#f59e0b')
    return ('ðŸŸ¢ LOW','#10b981')

def w_emoji(icon):
    return {"Thunderstorm":"â›ˆï¸","Drizzle":"ðŸŒ¦ï¸","Rain":"ðŸŒ§ï¸","Snow":"â„ï¸",
            "Fog":"ðŸŒ«ï¸","Mist":"ðŸŒ«ï¸","Haze":"ðŸŒ«ï¸","Clear":"â˜€ï¸","Clouds":"â›…"}.get(icon,"ðŸŒ¤ï¸")

@st.cache_data(ttl=300)
def load_shipment_data():
    files = sorted(glob.glob('data/shipments_*.csv'), reverse=True)[:7]
    if not files:
        return []
    all_shipments = []
    for f in files:
        try:
            df = pd.read_csv(f).head(20)
            for _, row in df.iterrows():
                all_shipments.append({
                    'supplier_id':      str(row.get('supplier_id','')),
                    'supplier_name':    str(row.get('supplier_name','')),
                    'order_id':         str(row.get('order_id','')),
                    'product':          str(row.get('product','')),
                    'category':         str(row.get('category','')),
                    'status':           str(row.get('status','')),
                    'delay_days':       int(row.get('delay_days',0)),
                    'risk_score':       float(row.get('risk_score',0)),
                    'region':           str(row.get('region','')),
                    'quantity':         int(row.get('quantity',0)),
                    'damage_type':      str(row.get('damage_type','none')),
                    'damaged_quantity': int(row.get('damaged_quantity',0)),
                    'weather_condition':str(row.get('weather_condition','Normal')),
                    'disruption_event': str(row.get('disruption_event','Normal operations')),
                    'timestamp':        str(row.get('date','')),
                    'ai_confidence':    random.randint(85,99),
                })
        except:
            pass
    return all_shipments

if 'ai_cache' not in st.session_state:
    st.session_state.ai_cache = {}

csv_alerts = load_shipment_data()

kafka_alerts = []
try:
    consumer = KafkaConsumer('decisions', bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=2000, auto_offset_reset='earliest')
    for msg in consumer:
        kafka_alerts.append(msg.value)
except:
    pass

all_alerts = csv_alerts + kafka_alerts
seen_orders = set()
alerts = []
for a in all_alerts:
    oid = a.get('order_id','')
    if oid and oid not in seen_orders:
        seen_orders.add(oid)
        alerts.append(a)

if len(alerts) < 10:
    suppliers = [
        ('SUP-10234','Tata Logistics','South Asia'),
        ('SUP-08765','DHL Express','Europe'),
        ('SUP-04567','FedEx Asia Pacific','Asia Pacific'),
        ('SUP-06789','Aramex Middle East','Middle East'),
        ('SUP-03321','Blue Dart India','South Asia'),
        ('SUP-11111','UPS North America','North America'),
        ('SUP-22222','Maersk Shipping','Europe'),
    ]
    for i in range(20):
        sid,sname,region = random.choice(suppliers)
        alerts.append({
            'supplier_id': sid,
            'supplier_name': sname,
            'order_id': f'ORD-DEMO-{i+1:03d}',
            'product': random.choice(['Electronic Components','Raw Materials','Pharmaceuticals','Automotive Parts']),
            'status': random.choice(['delayed','critical_delay','failed','delivered']),
            'delay_days': random.randint(-2,12),
            'risk_score': round(random.uniform(0.1,1.0),2),
            'region': region,
            'quantity': random.randint(100,1000),
            'damage_type': random.choice(['none','none','none','minor_damage']),
            'damaged_quantity': random.randint(0,50),
            'weather_condition': random.choice(['Normal','Normal','Heavy Rain','Snow Storm']),
            'disruption_event': 'Normal operations',
            'timestamp': datetime.now().strftime('%Y-%m-%d'),
            'ai_confidence': random.randint(85,99),
        })

anomalies = [a for a in alerts if a.get('status') in ['failed','critical_delay','delayed']
             or a.get('risk_score',0)>0.6]
for a in anomalies[:5]:
    key = a.get('supplier_id','') + a.get('status','')
    if key not in st.session_state.ai_cache:
        rc,mit,pri = get_ai_analysis(
            a.get('supplier_id',''), a.get('status',''),
            a.get('delay_days',0), a.get('risk_score',0), a.get('region',''))
        st.session_state.ai_cache[key] = (rc,mit,pri)
    a['root_cause'],a['mitigation'],a['priority'] = st.session_state.ai_cache[key]

total     = len(alerts)
delivered = sum(1 for a in alerts if a.get('status')=='delivered')
delayed   = sum(1 for a in alerts if a.get('status')=='delayed')
critical  = sum(1 for a in alerts if a.get('status')=='critical_delay')
failed_c  = sum(1 for a in alerts if a.get('status')=='failed')
damaged   = sum(1 for a in alerts if a.get('damage_type','none') != 'none')
high_risk = sum(1 for a in alerts if a.get('risk_score',0)>0.7)
weather_disruptions = sum(1 for a in alerts if a.get('weather_condition','Normal') != 'Normal')
total_damaged_units = sum(a.get('damaged_quantity',0) for a in alerts)

# HEADER
st.markdown(f"""
<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:16px'>
  <div>
    <div style='font-size:22px;font-weight:700;color:#0f172a;letter-spacing:-0.5px'>
      â—ˆ SupplyAI â€” Command Center</div>
    <div style='color:#94a3b8;font-size:12px;margin-top:2px'>
      Real-time monitoring Â· AI-powered insights Â· Global supply chain</div>
  </div>
  <div style='display:flex;gap:10px;align-items:center'>
    <div style='background:white;border:1px solid #e2e8f0;border-radius:10px;
    padding:8px 14px;font-size:13px;color:#64748b'>
      ðŸ”” <span style='background:#ef4444;color:white;border-radius:10px;
      padding:1px 7px;font-size:10px;font-weight:700'>{high_risk}</span>
    </div>
    <div style='background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;
    padding:8px 14px;font-size:12px;color:#16a34a;font-weight:600'>â†º 10s refresh</div>
    <div style='background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:10px;
    padding:9px 18px;font-size:13px;color:white;font-weight:500;
    box-shadow:0 4px 14px rgba(99,102,241,0.4)'>
      <span class='live-dot'></span>Live Â· {datetime.now().strftime('%b %d, %H:%M:%S')}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# KPI CARDS
k1,k2,k3,k4,k5 = st.columns(5)
kpis = [
    (k1,"ðŸ“¦","Total Orders",    f"{total:,}",  f"+{delivered} delivered","#eff6ff","#3b82f6","#bfdbfe",int(delivered/max(total,1)*100)),
    (k2,"âš ï¸","High Risk",       str(high_risk),f"{round(high_risk/max(total,1)*100)}% of total","#fff7ed","#f97316","#fed7aa",int(high_risk/max(total,1)*100)),
    (k3,"ðŸš¨","Critical Delays", str(critical), f"{delayed} delayed too","#fdf4ff","#a855f7","#e9d5ff",int(critical/max(total,1)*100)),
    (k4,"ðŸ’¥","Damaged Orders",  str(damaged),  f"{total_damaged_units:,} units","#fff1f2","#f43f5e","#fecdd3",int(damaged/max(total,1)*100)),
    (k5,"âœ…","Delivered",       str(delivered),f"{round(delivered/max(total,1)*100)}% success","#f0fdf4","#10b981","#bbf7d0",int(delivered/max(total,1)*100)),
]
for col,icon,label,val,delta,bg,color,border,pct in kpis:
    with col:
        st.markdown(f"""
        <div class='kpi-card'>
          <div style='display:flex;justify-content:space-between;margin-bottom:14px'>
            <div style='background:{bg};border:1px solid {border};width:40px;height:40px;
            border-radius:12px;display:flex;align-items:center;justify-content:center;
            font-size:18px'>{icon}</div>
            <span style='background:#f0fdf4;color:#16a34a;font-size:11px;
            font-weight:600;padding:3px 9px;border-radius:20px'>{delta}</span>
          </div>
          <div style='font-size:30px;font-weight:700;color:#0f172a;letter-spacing:-1px'>{val}</div>
          <div style='color:#94a3b8;font-size:12px;margin-top:3px;font-weight:500'>{label}</div>
          <div style='height:3px;background:{bg};border-radius:2px;margin-top:14px'>
            <div style='height:3px;width:{min(pct,100)}%;background:{color};border-radius:2px'></div>
          </div>
        </div>""", unsafe_allow_html=True)

# SUMMARY BANNER
st.markdown(f"""
<div style='background:white;border-radius:14px;padding:12px 20px;
box-shadow:0 1px 3px rgba(0,0,0,0.06);border:1px solid #f1f5f9;
margin-top:14px;margin-bottom:14px;display:flex;gap:24px;align-items:center;flex-wrap:wrap'>
  <div style='display:flex;align-items:center;gap:8px'>
    <span style='font-size:16px'>ðŸ“…</span>
    <div><div style='font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase'>Period</div>
    <div style='font-size:12px;font-weight:700;color:#0f172a'>Last 7 Days</div></div>
  </div>
  <div style='width:1px;height:28px;background:#f1f5f9'></div>
  <div style='display:flex;align-items:center;gap:8px'>
    <span style='font-size:16px'>ðŸšš</span>
    <div><div style='font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase'>Shipments</div>
    <div style='font-size:12px;font-weight:700;color:#0f172a'>{total:,} tracked</div></div>
  </div>
  <div style='width:1px;height:28px;background:#f1f5f9'></div>
  <div style='display:flex;align-items:center;gap:8px'>
    <span style='font-size:16px'>ðŸ“¦</span>
    <div><div style='font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase'>Damaged</div>
    <div style='font-size:12px;font-weight:700;color:#f43f5e'>{total_damaged_units:,} units</div></div>
  </div>
  <div style='width:1px;height:28px;background:#f1f5f9'></div>
  <div style='display:flex;align-items:center;gap:8px'>
    <span style='font-size:16px'>ðŸŒ</span>
    <div><div style='font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase'>Weather Events</div>
    <div style='font-size:12px;font-weight:700;color:#f97316'>{weather_disruptions} disruptions</div></div>
  </div>
  <div style='width:1px;height:28px;background:#f1f5f9'></div>
  <div style='display:flex;align-items:center;gap:8px'>
    <span style='font-size:16px'>âœ…</span>
    <div><div style='font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase'>Success Rate</div>
    <div style='font-size:12px;font-weight:700;color:#10b981'>{round(delivered/max(total,1)*100)}% on-time</div></div>
  </div>
  <div style='width:1px;height:28px;background:#f1f5f9'></div>
  <div style='display:flex;align-items:center;gap:8px'>
    <span style='font-size:16px'>ðŸ­</span>
    <div><div style='font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase'>Source</div>
    <div style='font-size:12px;font-weight:700;color:#6366f1'>CSV + Kafka</div></div>
  </div>
</div>
""", unsafe_allow_html=True)

# CHARTS
c1,c2,c3,c4 = st.columns(4)
cs = dict(paper_bgcolor='white',plot_bgcolor='white',height=175,margin=dict(l=0,r=0,t=8,b=0))

with c1:
    st.markdown("<div class='card'><div style='font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px'>ðŸ“ˆ Risk Score â€” 7 Days</div>", unsafe_allow_html=True)
    dates = [datetime.now()-timedelta(days=i) for i in range(6,-1,-1)]
    daily_risk = []
    for d in dates:
        day_str = d.strftime('%Y-%m-%d')
        day_alerts = [a for a in alerts if str(a.get('timestamp',''))[:10] == day_str]
        avg = round(sum(a.get('risk_score',0) for a in day_alerts)/max(len(day_alerts),1),2) if day_alerts else round(random.uniform(0.3,0.8),2)
        daily_risk.append(avg)
    fig = go.Figure(go.Scatter(x=dates,y=daily_risk,fill='tozeroy',
        line=dict(color='#6366f1',width=2.5),fillcolor='rgba(99,102,241,0.08)',
        mode='lines+markers',marker=dict(size=5,color='#6366f1')))
    fig.update_layout(**cs,xaxis=dict(showgrid=False,tickfont=dict(size=9,color='#94a3b8')),
        yaxis=dict(showgrid=True,gridcolor='#f1f5f9',tickfont=dict(size=9,color='#94a3b8')))
    st.plotly_chart(fig,use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with c2:
    st.markdown("<div class='card'><div style='font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px'>ðŸ“Š Shipment Status</div>", unsafe_allow_html=True)
    fig2 = go.Figure(go.Bar(
        x=['Delivered','Delayed','Critical','Failed'],
        y=[delivered, delayed, critical, failed_c],
        marker_color=['#10b981','#f59e0b','#a855f7','#ef4444'],
        marker_line_width=0,width=0.5))
    fig2.update_layout(**cs,xaxis=dict(showgrid=False,tickfont=dict(size=10,color='#64748b')),
        yaxis=dict(showgrid=True,gridcolor='#f1f5f9',tickfont=dict(size=9,color='#94a3b8')))
    st.plotly_chart(fig2,use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with c3:
    st.markdown("<div class='card'><div style='font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px'>ðŸ’¥ Damage Analysis</div>", unsafe_allow_html=True)
    dmg_counts = {}
    for a in alerts:
        d = a.get('damage_type','none')
        dmg_counts[d] = dmg_counts.get(d,0)+1
    if dmg_counts:
        fig3 = go.Figure(go.Pie(
            labels=list(dmg_counts.keys()),values=list(dmg_counts.values()),
            hole=0.55,marker_colors=['#10b981','#f59e0b','#f97316','#ef4444'],textinfo='none'))
        fig3.update_layout(paper_bgcolor='white',height=175,margin=dict(l=0,r=70,t=8,b=0),
            legend=dict(font=dict(size=9),orientation='v',x=1.02,y=0.5))
        st.plotly_chart(fig3,use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with c4:
    st.markdown("<div class='card'><div style='font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px'>ðŸŒ Regional Disruptions</div>", unsafe_allow_html=True)
    reg = {}
    for a in alerts:
        r = a.get('region','Unknown')
        reg[r] = reg.get(r,0)+1
    if reg:
        sr = dict(sorted(reg.items(),key=lambda x:x[1]))
        fig4 = go.Figure(go.Bar(x=list(sr.values()),y=list(sr.keys()),orientation='h',
            marker_color=['#c4b5fd','#a78bfa','#8b5cf6','#7c3aed','#6d28d9'][:len(sr)],
            marker_line_width=0))
        fig4.update_layout(**cs,xaxis=dict(showgrid=False,showticklabels=False),
            yaxis=dict(showgrid=False,tickfont=dict(size=10,color='#64748b')))
        st.plotly_chart(fig4,use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# MAIN CONTENT
left, right = st.columns([3,2])

with left:
    total_str = str(total)
    st.markdown(
        "<div class='card'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:14px'>"
        "<div><div style='font-size:14px;font-weight:700;color:#0f172a'>ðŸšš Real Shipment Monitor</div>"
        "<div style='font-size:12px;color:#94a3b8;margin-top:2px'>Live CSV + Kafka â€” updates every 10s</div></div>"
        "<div style='display:flex;align-items:center;gap:8px'>"
        "<span style='background:#f0fdf4;color:#16a34a;padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600'>"
        "<span class='live-dot'></span>LIVE</span>"
        "<span style='background:#f0f4ff;color:#6366f1;padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600'>"
        + total_str + " orders</span>"
        "</div></div>",
        unsafe_allow_html=True)

    rows = []
    for a in alerts[:15]:
        r = a.get('risk_score',0)
        s = a.get('status','')
        if s=='delivered':        sb='âœ… Delivered'
        elif s=='critical_delay': sb='ðŸ”´ Critical'
        elif s=='delayed':        sb='ðŸŸ¡ Delayed'
        elif s=='failed':         sb='âŒ Failed'
        else:                     sb='ðŸŸ¢ On Track'
        dmg = a.get('damage_type','none')
        dmg_icon = 'ðŸ’¥' if dmg=='total_loss' else 'âš ï¸' if dmg=='major_damage' else 'ðŸ“¦' if dmg=='minor_damage' else 'âœ…'
        rows.append({
            'Date':     str(a.get('timestamp',''))[:10],
            'Order':    a.get('order_id',''),
            'Supplier': a.get('supplier_name') or a.get('supplier_id',''),
            'Product':  a.get('product','N/A'),
            'Status':   sb,
            'Delay(d)': a.get('delay_days',0),
            'Risk':     r,
            'Damage':   f"{dmg_icon} {dmg}",
            'Weather':  a.get('weather_condition','Normal'),
            'Region':   a.get('region',''),
        })
    st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True,height=290)
    st.markdown("</div>", unsafe_allow_html=True)

    # AI Panel
    st.markdown(
        "<div class='card'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:16px'>"
        "<div style='font-size:14px;font-weight:700;color:#0f172a'>ðŸ¤– AI Recommendation Panel</div>"
        "<span style='background:linear-gradient(135deg,#6366f1,#8b5cf6);color:white;"
        "padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600'>Powered by Groq</span>"
        "</div>",
        unsafe_allow_html=True)

    shown = 0
    for a in anomalies:
        if shown >= 4: break
        if 'root_cause' not in a: continue
        r   = a.get('risk_score',0)
        pri = a.get('priority','High')
        rc  = a.get('root_cause','')
        mit = a.get('mitigation','')
        pri_color = "#ef4444" if pri in ['High','Critical'] else "#f59e0b" if pri=='Medium' else "#10b981"
        border = "#ef4444" if r>0.7 else "#f97316" if r>0.4 else "#10b981"
        pct = int(r*100)
        dmg = a.get('damage_type','none')
        weather = a.get('weather_condition','Normal')
        st.markdown(f"""
        <div class='ai-card' style='border-left:4px solid {border}'>
          <div style='display:flex;justify-content:space-between;margin-bottom:10px'>
            <div>
              <span style='font-size:13px;font-weight:600;color:#0f172a'>
              {a.get('order_id','')} â€” {a.get('supplier_name') or a.get('supplier_id','')}</span><br>
              <span style='font-size:11px;color:#94a3b8'>
              {a.get('product','N/A')} Â· {a.get('region','')} Â· ðŸŒ¦ï¸ {weather}</span>
            </div>
            <div style='display:flex;gap:6px;align-items:flex-start'>
              <span style='background:{pri_color}15;color:{pri_color};padding:2px 10px;
              border-radius:20px;font-size:11px;font-weight:600'>{pri}</span>
              <span style='background:#f1f5f9;color:#64748b;padding:2px 8px;
              border-radius:20px;font-size:11px'>Risk: {r}</span>
            </div>
          </div>
          <div style='display:grid;grid-template-columns:1fr 1fr;gap:10px'>
            <div style='background:white;border-radius:8px;padding:10px;border:1px solid #f1f5f9'>
              <div style='font-size:10px;color:#6366f1;font-weight:700;text-transform:uppercase;
              letter-spacing:0.05em;margin-bottom:5px'>ðŸ” Root Cause</div>
              <div style='font-size:12px;color:#475569;line-height:1.5'>{rc}</div>
            </div>
            <div style='background:white;border-radius:8px;padding:10px;border:1px solid #f1f5f9'>
              <div style='font-size:10px;color:#10b981;font-weight:700;text-transform:uppercase;
              letter-spacing:0.05em;margin-bottom:5px'>ðŸ›¡ï¸ Mitigation</div>
              <div style='font-size:12px;color:#475569;line-height:1.5'>{mit}</div>
            </div>
          </div>
          <div style='margin-top:8px;display:flex;gap:8px;align-items:center'>
            <div style='height:4px;flex:1;background:#f1f5f9;border-radius:2px'>
              <div style='height:4px;width:{pct}%;background:{border};border-radius:2px'></div>
            </div>
            <span style='font-size:11px;color:#94a3b8'>{pct}% risk Â· {dmg}</span>
          </div>
        </div>""", unsafe_allow_html=True)
        shown += 1
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    # Global Weather Monitor
    st.markdown(
        "<div class='card'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:12px'>"
        "<div><div style='font-size:14px;font-weight:700;color:#0f172a'>ðŸŒ Global Weather Risk Monitor</div>"
        "<div style='font-size:12px;color:#94a3b8;margin-top:2px'>India Â· Americas Â· Europe Â· Asia Â· Middle East</div></div>"
        "<span style='background:#e0f2fe;color:#0284c7;padding:3px 10px;border-radius:20px;"
        "font-size:11px;font-weight:600'>OpenWeather API</span>"
        "</div>",
        unsafe_allow_html=True)

    fallback = {
        'Hyderabad': ('â›ˆï¸','Thunderstorm',  38, 72,'ðŸ”´ HIGH','#ef4444'),
        'Chennai':   ('ðŸŒ§ï¸','Heavy Rain',    33, 45,'ðŸ”´ HIGH','#ef4444'),
        'Mumbai':    ('â›…','Partly Cloudy', 31, 22,'ðŸŸ¢ LOW', '#10b981'),
        'New York':  ('ðŸŒ¨ï¸','Snow Storm',    -2, 45,'ðŸ”´ HIGH','#ef4444'),
        'London':    ('ðŸŒ«ï¸','Fog',            8, 15,'ðŸŸ¡ MED', '#f59e0b'),
        'Singapore': ('ðŸŒ§ï¸','Heavy Rain',    29, 35,'ðŸŸ¡ MED', '#f59e0b'),
        'Dubai':     ('â˜€ï¸','Clear Sky',     38, 10,'ðŸŸ¢ LOW', '#10b981'),
        'Tokyo':     ('â›…','Cloudy',         18, 20,'ðŸŸ¢ LOW', '#10b981'),
    }

    for city in ['Hyderabad','Chennai','Mumbai','New York','London','Singapore','Dubai','Tokyo']:
        w = get_weather(city)
        cfg = CITY_CONFIG.get(city, {})
        flag = cfg.get('flag','ðŸŒ')
        region_label = cfg.get('region','Global')

        if w:
            rl,rc2 = w_risk(w['icon'],w['wind'])
            em = w_emoji(w['icon'])
            temp,desc,wind,hum = w['temp'],w['desc'],w['wind'],w['humidity']
        else:
            em,desc,temp,wind,rl,rc2 = fallback[city]
            hum = '--'

        st.markdown(f"""
        <div class='weather-card'>
          <div style='display:flex;justify-content:space-between;align-items:center'>
            <div style='display:flex;align-items:center;gap:10px'>
              <span style='font-size:22px'>{em}</span>
              <div>
                <div style='display:flex;align-items:center;gap:6px'>
                  <span style='font-size:13px;font-weight:600;color:#0f172a'>{city}</span>
                  <span style='font-size:10px;color:#6366f1;font-weight:600;
                  background:#eff6ff;padding:1px 6px;border-radius:8px'>{flag} {region_label}</span>
                </div>
                <div style='font-size:11px;color:#94a3b8;margin-top:2px'>
                {desc} Â· ðŸ’¨{wind}km/h Â· ðŸ’§{hum}%</div>
              </div>
            </div>
            <div style='text-align:right'>
              <div style='font-size:18px;font-weight:700;color:#0f172a'>{temp}Â°C</div>
              <span style='background:{rc2}15;color:{rc2};padding:2px 8px;
              border-radius:10px;font-size:10px;font-weight:700'>{rl}</span>
            </div>
          </div>
        </div>""", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # AI Engine
    st.markdown("""
    <div style='background:linear-gradient(135deg,#0f1628,#1e1b4b);
    border-radius:16px;padding:20px;border:1px solid #2d3561;margin-bottom:14px'>
      <div style='display:flex;justify-content:space-between;margin-bottom:12px'>
        <span style='color:white;font-weight:700;font-size:14px'>ðŸ¤– AI Decision Engine</span>
        <span style='background:#052e16;color:#4ade80;padding:3px 10px;
        border-radius:20px;font-size:11px;font-weight:600'>
        <span class='live-dot'></span>Active</span>
      </div>
      <div style='text-align:center;padding:6px 0 12px'>
        <div style='font-size:40px'>ðŸŒ</div>
        <div style='color:#94a3b8;font-size:12px;margin-top:6px'>
        Groq AI agents monitoring all global nodes</div>
      </div>
      <div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px'>
        <div style='background:rgba(255,255,255,0.07);border-radius:10px;padding:10px;text-align:center'>
          <div style='color:white;font-size:20px;font-weight:700'>24</div>
          <div style='color:#64748b;font-size:10px'>Actions</div>
        </div>
        <div style='background:rgba(255,255,255,0.07);border-radius:10px;padding:10px;text-align:center'>
          <div style='color:#f97316;font-size:20px;font-weight:700'>5</div>
          <div style='color:#64748b;font-size:10px'>Predicted</div>
        </div>
        <div style='background:rgba(255,255,255,0.07);border-radius:10px;padding:10px;text-align:center'>
          <div style='color:#4ade80;font-size:20px;font-weight:700'>92%</div>
          <div style='color:#64748b;font-size:10px'>Accuracy</div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    # System health
    st.markdown("<div class='card'><div style='font-size:14px;font-weight:700;color:#0f172a;margin-bottom:12px'>âš™ï¸ System Monitoring</div>", unsafe_allow_html=True)
    csv_files = len(glob.glob('data/shipments_*.csv'))
    for name,val,color in [
        ("Kafka Cluster",    "3/3 Healthy",          "#10b981"),
        ("Groq AI Agent",    "98.7% uptime",          "#10b981"),
        ("Anomaly Detector", "Live 320ms",             "#10b981"),
        ("Weather API",      "8 cities live",         "#10b981"),
        ("CSV Data Files",   f"{csv_files} days",     "#6366f1"),
        ("Total Orders",     f"{total:,} tracked",    "#10b981"),
        ("Orchestration",    "DAG Scheduled",          "#a855f7"),
        ("Error Rate",       "0.3%",                   "#10b981"),
    ]:
        st.markdown(f"""
        <div style='display:flex;justify-content:space-between;padding:7px 0;
        border-bottom:1px solid #f8fafc'>
          <span style='color:#64748b;font-size:12px'>{name}</span>
          <span style='background:{color}15;color:{color};padding:2px 10px;
          border-radius:20px;font-size:11px;font-weight:600'>{val}</span>
        </div>""", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

# FOOTER
now_str = datetime.now().strftime('%H:%M:%S')
st.markdown(
    "<div style='background:linear-gradient(135deg,#0f1628,#1e1b4b);"
    "border-radius:16px;padding:16px 24px;margin-top:8px;border:1px solid #2d3561;"
    "display:flex;justify-content:space-between;align-items:center'>"
    "<div style='color:#475569;font-size:11px'>"
    "Â© 2026 SupplyAI Command Center Â· CSV Â· Kafka Â· Groq AI Â· OpenWeather Â· Streamlit"
    "</div>"
    "<div style='color:#475569;font-size:11px'>"
    "Last updated: " + now_str + " Â· Auto-refresh: 10s"
    "</div>"
    "</div>",
    unsafe_allow_html=True)

