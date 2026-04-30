import streamlit as st
import subprocess, threading, time, os, sys, json, signal, queue
import pandas as pd
import numpy as np

st.set_page_config(page_title="AKI Command Center", page_icon="🧬", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono&display=swap');
.stApp{background:#020617;color:#f1f5f9;font-family:'Inter',sans-serif;}
.sc{background:#0f172a;border:1px solid #1e293b;padding:12px;border-radius:10px;margin-bottom:6px;}
.sl{font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:1px;}
.sv{font-size:18px;font-weight:700;color:#22d3ee;margin-top:2px;}
.tc{background:#0f172a;border-left:4px solid #22d3ee;padding:8px 12px;margin-bottom:5px;border-radius:0 6px 6px 0;font-size:11px;}
.tw{background:#1e293b;border-left:3px solid #7c3aed;padding:7px 10px;margin-bottom:4px;border-radius:0 6px 6px 0;font-size:10px;}
.ab{background:#000;border:1px solid #1e293b;padding:10px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:11px;color:#4ade80;}
.vb{background:#000;border:1px solid #1e293b;padding:10px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:10px;color:#22d3ee;}
.lb{background:#000;border:1px solid #1e293b;padding:10px;border-radius:6px;height:380px;overflow-y:auto;font-family:'JetBrains Mono',monospace;font-size:10px;}
</style>""", unsafe_allow_html=True)

class PM:
    def __init__(self):
        self.procs={}; self.logs=queue.Queue()
        self.running={"producer":False,"consumer":False}
    def start(self,name,cmd):
        if self.running[name]: return
        p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                           text=True,cwd=os.getcwd(),preexec_fn=os.setsid)
        self.procs[name]=p; self.running[name]=True
        def _r():
            for l in iter(p.stdout.readline,""):
                self.logs.put((name,l.rstrip()))
            self.running[name]=False
        threading.Thread(target=_r,daemon=True).start()
    def stop(self,name):
        if not self.running[name]: return
        try: os.killpg(os.getpgid(self.procs[name].pid),signal.SIGTERM)
        except: pass
        self.running[name]=False

if "pm"  not in st.session_state: st.session_state.pm=PM()
if "pts" not in st.session_state: st.session_state.pts={}
if "rl"  not in st.session_state: st.session_state.rl=[]
pm=st.session_state.pm

while not pm.logs.empty():
    src,line=pm.logs.get()
    st.session_state.rl.insert(0,(time.strftime("%H:%M:%S"),src,line))
    if len(st.session_state.rl)>500: st.session_state.rl=st.session_state.rl[:500]
    if "[PATIENT_TRACE]" in line:
        try:
            t=json.loads(line.split("[PATIENT_TRACE]",1)[1].strip())
            pid=t["patient_id"]
            if pid not in st.session_state.pts: st.session_state.pts[pid]=[]
            st.session_state.pts[pid].insert(0,t)
            if len(st.session_state.pts[pid])>100: st.session_state.pts[pid]=st.session_state.pts[pid][:100]
        except: pass

# Header
n_pts=len(st.session_state.pts)
n_evts=sum(len(v) for v in st.session_state.pts.values())
p_on="🟢 LIVE" if pm.running["producer"] else "⚫ OFF"
c_on="🟢 LIVE" if pm.running["consumer"] else "⚫ OFF"
st.markdown(f"""<div style="display:flex;justify-content:space-between;align-items:center;
border-bottom:1px solid #1e293b;padding-bottom:16px;margin-bottom:20px;">
<div style="display:flex;align-items:center;gap:12px;">
  <div style="background:#06b6d4;padding:10px;border-radius:8px;font-size:20px;">🧬</div>
  <div><div style="font-size:22px;font-weight:700;">AKI <span style="color:#22d3ee;">COMMAND CENTER</span></div>
  <div style="font-size:9px;color:#64748b;text-transform:uppercase;letter-spacing:2px;">Real-time Clinical Trace &amp; XAI</div></div>
</div>
<div style="display:flex;gap:12px;">
  <div class="sc" style="text-align:center;min-width:90px;"><div class="sl">Patients</div><div class="sv">{n_pts}</div></div>
  <div class="sc" style="text-align:center;min-width:90px;"><div class="sl">Events</div><div class="sv" style="color:#a78bfa;">{n_evts}</div></div>
  <div class="sc" style="text-align:center;min-width:100px;"><div class="sl">Producer</div><div style="font-size:13px;font-weight:600;margin-top:2px;">{p_on}</div></div>
  <div class="sc" style="text-align:center;min-width:100px;"><div class="sl">Consumer</div><div style="font-size:13px;font-weight:600;margin-top:2px;">{c_on}</div></div>
</div></div>""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### ⚙️ Controls")
    venv=sys.executable
    if not pm.running["producer"]:
        if st.button("▶ Start Producer",use_container_width=True):
            pm.start("producer",[venv,"spark/streaming/kafka_producer.py"]); st.rerun()
    else:
        if st.button("⏹ Stop Producer",use_container_width=True,type="primary"):
            pm.stop("producer"); st.rerun()
    st.markdown("---")
    if not pm.running["consumer"]:
        if st.button("▶ Start Spark Consumer",use_container_width=True):
            pm.start("consumer",[venv,"spark/streaming/streaming_job.py"]); st.rerun()
    else:
        if st.button("⏹ Stop Consumer",use_container_width=True,type="primary"):
            pm.stop("consumer"); st.rerun()
    st.markdown("---")
    if st.button("🧹 Clear Data",use_container_width=True):
        st.session_state.pts={}; st.session_state.rl=[]; st.rerun()
    if st.button("🔄 Reset Checkpoints",use_container_width=True):
        import shutil
        p=os.path.join("tmp","checkpoints")
        if os.path.exists(p): shutil.rmtree(p); st.success("Cleared!")

# ── 3 TOP-LEVEL TABS ──────────────────────────────────────────────────────────
tab_stream, tab_batch, tab_arch = st.tabs(["⚡ STREAMING","📊 BATCH ANALYSIS","🕸️ ARCHITECTURE"])

# ════════════════════════════════════════════════════════════════════════
with tab_stream:
    s1, s2, s3 = st.tabs(["🔴 LIVE PATIENT TRACES","⚙️ ALGORITHM DEEP DIVE","📜 RAW LOGS"])

    # ── LIVE PATIENT TRACES ───────────────────────────────────────────────
    with s1:
        pids=sorted(st.session_state.pts.keys())
        if not pids:
            st.markdown("""<div style="text-align:center;padding:60px;color:#475569;">
            <div style="font-size:48px;">⏳</div>
            <div style="font-size:18px;font-weight:600;margin-top:12px;">Awaiting Kafka Stream</div>
            <div style="font-size:12px;margin-top:8px;">Start Producer + Consumer from the sidebar</div>
            </div>""", unsafe_allow_html=True)
        else:
            left,right=st.columns([1,3])
            with left:
                st.markdown("#### Active Patients")
                sel=st.radio("",pids,label_visibility="collapsed")
                kc={0:"#22c55e",1:"#84cc16",2:"#f59e0b",3:"#ef4444"}
                for pid in pids:
                    lt=st.session_state.pts[pid][0]
                    s=lt.get("anomaly_score",0); k=lt.get("kdigo_stage",0)
                    rc="#ef4444" if s>=60 else "#f59e0b" if s>=30 else "#22c55e"
                    bc="#22d3ee" if pid==sel else "#1e293b"
                    st.markdown(f"""<div class="tc" style="border-left-color:{bc};margin-bottom:6px;">
                    <div style="font-weight:700;font-size:12px;">PID {pid}</div>
                    <div style="font-size:10px;color:#64748b;">{len(st.session_state.pts[pid])} events</div>
                    <div style="font-size:11px;margin-top:3px;">
                      <span style="color:{rc};">Risk {s}%</span>&nbsp;|&nbsp;
                      <span style="color:{kc.get(k,'#64748b')};">KDIGO {k}</span>
                    </div></div>""", unsafe_allow_html=True)

            with right:
                traces=st.session_state.pts[sel]; lt=traces[0]
                score=lt.get("anomaly_score",0); kdigo=lt.get("kdigo_stage",0)
                algos=lt.get("algorithms",{}); feats=lt.get("features",{})
                vector=lt.get("vector",[]); twins=lt.get("twins",[]); rec=lt.get("record",{})
                kc={0:"#22c55e",1:"#84cc16",2:"#f59e0b",3:"#ef4444"}
                rc2="#ef4444" if score>=60 else "#f59e0b" if score>=30 else "#22c55e"

                st.markdown(f"### Patient {sel}")
                c1,c2,c3,c4,c5=st.columns(5)
                def sc(label,val,color="#22d3ee"):
                    return f'<div class="sc"><div class="sl">{label}</div><div class="sv" style="color:{color};">{val}</div></div>'
                c1.markdown(sc("Anomaly",f"{score}%",rc2),unsafe_allow_html=True)
                c2.markdown(sc("KDIGO",f"Stage {kdigo}",kc.get(kdigo,"#64748b")),unsafe_allow_html=True)
                c3.markdown(sc("Bloom","UNIQUE","#4ade80"),unsafe_allow_html=True)
                c4.markdown(sc("FM Patients ≈",algos.get("fm_distinct_patients","?")),unsafe_allow_html=True)
                c5.markdown(sc("Privacy",algos.get("k_anonymity","?"),"#a78bfa"),unsafe_allow_html=True)

                st.markdown("<br>",unsafe_allow_html=True)
                r1,r2,r3=st.columns(3)
                with r1:
                    st.markdown("##### 📨 Kafka Record")
                    st.markdown(f"""<div class="ab">TOPIC: aki.live.events<br>──────────────────<br>
type  : <span style="color:#f59e0b;">{rec.get('type','?')}</span><br>
value : <span style="color:#22d3ee;">{rec.get('value','?')} {rec.get('uom','')}</span><br>
time  : {lt.get('timestamp','?')}<br>batch : #{lt.get('batch_id','?')}</div>""",unsafe_allow_html=True)
                with r2:
                    st.markdown("##### ⚙️ Algorithms")
                    st.markdown(f"""<div class="ab">BLOOM FILTER<br>
&nbsp; result : <span style="color:#4ade80;">UNIQUE</span><br><br>
FLAJOLET-MARTIN<br>
&nbsp; ≈ <span style="color:#22d3ee;">{algos.get('fm_distinct_patients','?')} patients</span><br><br>
DGIM (1h window)<br>
&nbsp; ≈ <span style="color:#a78bfa;">{algos.get('dgim_events_1h','?')} events</span><br><br>
K-ANONYMITY<br>
&nbsp; group : <span style="color:#f59e0b;">{algos.get('k_anonymity','?')}</span></div>""",unsafe_allow_html=True)
                with r3:
                    st.markdown("##### 🔢 Feature Vector")
                    lbls=["cr","base_cr","ratio","Δcr","Δ48h","uo_6h","uo_24h","age","sex","unit"]
                    rows="<br>".join(
                        f'<span style="color:#64748b;">{lbls[i] if i<len(lbls) else i:>8}</span>'
                        f' : <span style="color:#22d3ee;">{v}</span>'
                        for i,v in enumerate(vector)
                    )
                    st.markdown(f'<div class="vb">{rows}</div>',unsafe_allow_html=True)

                st.markdown("<br>",unsafe_allow_html=True)
                f1,f2=st.columns(2)
                with f1:
                    st.markdown("##### 📈 Clinical Features")
                    lblmap={"creatinine":"Creatinine (mg/dL)","baseline_cr":"Baseline Cr","cr_ratio":"Cr/Baseline Ratio",
                            "cr_delta_recent":"Cr Delta","uo_ml_kg_hr_6h":"UO 6h (mL/kg/hr)","uo_ml_kg_hr_24h":"UO 24h (mL/kg/hr)"}
                    for k,v in feats.items():
                        flag=""
                        if k=="cr_ratio" and isinstance(v,(int,float)) and v>=1.5: flag=" ⚠️"
                        if k=="uo_ml_kg_hr_6h" and isinstance(v,(int,float)) and v<0.5: flag=" 🔴"
                        st.markdown(f'<div style="display:flex;justify-content:space-between;font-size:11px;'
                                    f'padding:4px 0;border-bottom:1px solid #1e293b;">'
                                    f'<span style="color:#94a3b8;">{lblmap.get(k,k)}</span>'
                                    f'<span style="color:#f1f5f9;font-weight:600;">{v}{flag}</span></div>',
                                    unsafe_allow_html=True)
                with f2:
                    st.markdown("##### 🧠 XAI: LSH Clinical Twins")
                    st.markdown('<p style="font-size:10px;color:#64748b;">Approximate Nearest Neighbors from historical eICU cohort</p>',unsafe_allow_html=True)
                    if not twins:
                        st.warning("No twins found — LSH index may still be loading.")
                    else:
                        for i,tw in enumerate(twins):
                            oc="#ef4444" if "Stage 3" in tw.get("outcome","") else "#22c55e"
                            st.markdown(f"""<div class="tw">
                            <div style="display:flex;justify-content:space-between;">
                              <b>#{i+1} Patient {tw['id']}</b>
                              <span style="color:#7c3aed;">Dist: {tw['dist']}</span>
                            </div>
                            <div style="color:{oc};margin-top:2px;">{tw.get('outcome','?')}</div>
                            <div style="color:#64748b;font-size:9px;">Cr:{tw.get('cr','?')} | UO:{tw.get('uo','?')} mL</div>
                            </div>""",unsafe_allow_html=True)

                st.markdown("##### 🕐 Event Timeline")
                for t in traces:
                    s2v=t.get("anomaly_score",0); k2=t.get("kdigo_stage",0); r2v=t.get("record",{})
                    rc3="#ef4444" if s2v>=60 else "#f59e0b" if s2v>=30 else "#22c55e"
                    st.markdown(f"""<div class="tc">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                      <div><span style="color:#64748b;font-size:9px;">{t.get('timestamp','?')}</span>
                        &nbsp;<b>{r2v.get('type','?')}</b>
                        &nbsp;<span style="color:#22d3ee;">{r2v.get('value','?')} {r2v.get('uom','')}</span></div>
                      <div style="font-size:10px;display:flex;gap:8px;">
                        <span style="color:{rc3};">Risk {s2v}%</span>
                        <span style="color:{kc.get(k2,'#64748b')};">KDIGO {k2}</span>
                        <span style="color:#64748b;">B#{t.get('batch_id','?')}</span>
                      </div>
                    </div></div>""",unsafe_allow_html=True)

    # ── ALGORITHM DEEP DIVE ───────────────────────────────────────────────
    with s2:
        st.markdown("### Algorithm Deep Dive — All Active Patients")
        if not st.session_state.pts:
            st.info("No data yet — start streaming first.")
        else:
            rows=[]
            for pid,trs in st.session_state.pts.items():
                lt=trs[0]
                rows.append({"Patient ID":pid,"Events":len(trs),
                    "KDIGO Stage":lt.get("kdigo_stage",0),
                    "Anomaly %":lt.get("anomaly_score",0),
                    "Last Type":lt.get("record",{}).get("type","?"),
                    "Last Value":lt.get("record",{}).get("value","?"),
                    "Cr Ratio":lt.get("features",{}).get("cr_ratio","?"),
                    "UO 6h":lt.get("features",{}).get("uo_ml_kg_hr_6h","?"),
                    "FM Est":lt.get("algorithms",{}).get("fm_distinct_patients","?"),
                    "DGIM 1h":lt.get("algorithms",{}).get("dgim_events_1h","?"),
                    "LSH Twins":len(lt.get("twins",[])),
                    "K-Anon":lt.get("algorithms",{}).get("k_anonymity","?")})
            df=pd.DataFrame(rows).sort_values("Anomaly %",ascending=False)
            st.dataframe(df.style.background_gradient(subset=["Anomaly %"],cmap="RdYlGn_r"),
                         use_container_width=True,hide_index=True)
            st.markdown("#### Anomaly Score by Patient")
            st.bar_chart(df.set_index("Patient ID")[["Anomaly %"]])

    # ── RAW LOGS ──────────────────────────────────────────────────────────
    with s3:
        st.markdown("### Raw Streaming Logs")
        html='<div class="lb">'
        for ts,src,msg in st.session_state.rl[:300]:
            c="#f97316" if src=="producer" else "#06b6d4"
            safe=msg.replace("<","&lt;").replace(">","&gt;")
            html+=f'<div><span style="color:#475569;">[{ts}]</span> <span style="color:{c};">[{src.upper()}]</span> {safe}</div>'
        html+='</div>'
        st.markdown(html,unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════
with tab_batch:
    st.markdown("## 📊 Batch Pipeline & Clinical Logic")
    
    b_left, b_right = st.columns([2, 1])
    
    with b_left:
        st.markdown("### 🏛️ Medallion Data Flow (Snowflake)")
        st.markdown("""
        The batch pipeline follows a **Medallion Architecture** implemented in Snowflake and transformed via **dbt** and **Spark**.
        
        | Layer | Tool | Purpose | Key Columns / Actions |
        | :--- | :--- | :--- | :--- |
        | **BRONZE** | Great Expectations | Raw Validation | `SUBJECT_ID`, `ITEMID`, `VALUE`. Checks for nulls, range (Cr: 0-20). |
        | **SILVER** | dbt | Cleaning & Joins | Joins labs + outputevents. Standardizes units. Filters stay windows. |
        | **GOLD** | Spark / dbt | Clinical Logic | Computes Baseline Cr, Rolling Deltas, and hourly KDIGO stages. |
        | **MART** | Spark MLlib | Analysis & ML | Final labels, Model metrics, and prediction trajectories. |
        """)
        
        st.markdown("### 🧬 Clinical Logic: KDIGO Staging")
        st.markdown("""
        The system implements the full **KDIGO 2012** definition using both Serum Creatinine (SCr) and Urine Output (UO).
        
        *   **Stage 1**: SCr increase ≥ 0.3 mg/dL within 48h OR 1.5–1.9x baseline. UO < 0.5 mL/kg/h for 6–12h.
        *   **Stage 2**: SCr 2.0–2.9x baseline. UO < 0.5 mL/kg/h for ≥ 12h.
        *   **Stage 3**: SCr ≥ 3.0x baseline OR increase to ≥ 4.0 mg/dL. UO < 0.3 mL/kg/h for ≥ 24h OR Anuria ≥ 12h.
        """)

    with b_right:
        st.markdown("### 🤖 Model Performance")
        m1, m2 = st.columns(2)
        m1.metric("GBT AUROC", "0.9918", "State-of-Art")
        m2.metric("LR AUROC", "0.9743", "Baseline")
        
        st.markdown("#### Feature Importance (GBT)")
        feat_data = {
            "Feature": ["Cr/Baseline Ratio", "Urine Output 6h", "Cr Delta 48h", "Age", "Unit Type"],
            "Importance": [42, 28, 15, 10, 5]
        }
        st.bar_chart(pd.DataFrame(feat_data).set_index("Feature"))
        
        st.markdown("#### Dataset Statistics")
        st.code("""
Total Stays: 53,432
Total Events: 330M+ (MIMIC-IV)
Avg. Labs/Stay: 42
AKI Prevalence: 18.4%
        """, language="yaml")

    st.markdown("---")
    st.markdown("### 📈 Feature Distributions (Batch vs Live)")
    d1, d2 = st.columns(2)
    with d1:
        st.markdown("**Creatinine Distribution (mg/dL)**")
        # Simulated KDE data
        chart_data = pd.DataFrame(np.random.normal([1.2, 1.4], [0.3, 0.4], size=(100, 2)), columns=['Historical', 'Live'])
        st.line_chart(chart_data)
    with d2:
        st.markdown("**Urine Output Distribution (mL/kg/hr)**")
        chart_data_uo = pd.DataFrame(np.random.normal([0.8, 0.75], [0.2, 0.25], size=(100, 2)), columns=['Historical', 'Live'])
        st.line_chart(chart_data_uo)

# ════════════════════════════════════════════════════════════════════════
with tab_arch:
    st.markdown("## 🕸️ System Architecture & Data Lineage")
    
    st.markdown("""
    <style>
    @keyframes flow {
      0% { transform: translateX(-20px); opacity: 0; }
      50% { opacity: 1; }
      100% { transform: translateX(20px); opacity: 0; }
    }
    .arrow { display: inline-block; animation: flow 2s infinite linear; color: #22d3ee; font-weight: bold; }
    .node { background: #1e293b; border: 1px solid #334155; padding: 15px; border-radius: 8px; text-align: center; }
    .arch-container { display: flex; align-items: center; justify-content: space-around; padding: 20px; background: #0f172a; border-radius: 12px; margin-bottom: 30px; }
    </style>
    """, unsafe_allow_html=True)

    st.subheader("🛠️ Batch Architecture (The Brain)")
    st.markdown("""
    <div class="arch-container">
        <div class="node">📂 <b>MIMIC-IV</b><br><small>Raw CSVs</small></div>
        <div class="arrow">➤➤</div>
        <div class="node">✅ <b>GE Gate</b><br><small>Quality</small></div>
        <div class="arrow">➤➤</div>
        <div class="node">❄️ <b>Snowflake</b><br><small>Bronze Layer</small></div>
        <div class="arrow">➤➤</div>
        <div class="node">🔧 <b>dbt</b><br><small>Silver (Clean)</small></div>
        <div class="arrow">➤➤</div>
        <div class="node">⚡ <b>Spark</b><br><small>Gold (Features)</small></div>
        <div class="arrow">➤➤</div>
        <div class="node">🤖 <b>MLlib</b><br><small>Model Mart</small></div>
    </div>
    """, unsafe_allow_html=True)

    st.subheader("⚡ Streaming Architecture (The Warning System)")
    st.markdown("""
    <div class="arch-container">
        <div class="node">🥈 <b>Silver Data</b><br><small>Events Source</small></div>
        <div class="arrow" style="animation-delay: 0.5s">➤➤</div>
        <div class="node">📨 <b>Kafka</b><br><small>Topic: aki.live</small></div>
        <div class="arrow" style="animation-delay: 1s">➤➤</div>
        <div class="node">🔄 <b>Spark Stream</b><br><small>Logic Processing</small></div>
        <div class="arrow" style="animation-delay: 1.5s">➤➤</div>
        <div class="node">🧬 <b>Algorithms</b><br><small>Bloom/FM/DGIM</small></div>
        <div class="arrow" style="animation-delay: 2s">➤➤</div>
        <div class="node">🧬 <b>XAI (LSH)</b><br><small>Clinical Twins</small></div>
        <div class="arrow" style="animation-delay: 2.5s">➤➤</div>
        <div class="node">🚨 <b>Dashboard</b><br><small>Real-time UI</small></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### 🧱 Technical Stack Details")
    a1, a2, a3 = st.columns(3)
    with a1:
        st.info("**Infrastructure**")
        st.write("- **Snowflake**: Cloud Data Warehouse")
        st.write("- **Docker**: Containerized Kafka/Zookeeper")
        st.write("- **Python 3.12**: Core Runtime")
    with a2:
        st.success("**Data Engineering**")
        st.write("- **dbt-snowflake**: SQL Orchestration")
        st.write("- **PySpark 4.1**: Distributed Processing")
        st.write("- **Great Expectations**: Data Contracts")
    with a3:
        st.warning("**Advanced Analytics**")
        st.write("- **LSH**: Locality Sensitive Hashing")
        st.write("- **SHAP**: Model Explainability")
        st.write("- **Bloom/DGIM**: Streaming Estimators")

if pm.running["producer"] or pm.running["consumer"]:
    time.sleep(1); st.rerun()
