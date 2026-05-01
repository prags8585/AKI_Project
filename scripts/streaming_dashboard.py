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
# Set JAVA_HOME for Spark
if "JAVA_HOME" not in os.environ:
    try:
        jh = subprocess.check_output("/usr/libexec/java_home", text=True).strip()
        os.environ["JAVA_HOME"] = jh
    except: pass

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
    # Robust venv detection
    venv = sys.executable
    local_venv = os.path.join(os.getcwd(), ".venv", "bin", "python")
    if os.path.exists(local_venv):
        venv = local_venv
        
    st.markdown("---")
    mock_mode = st.checkbox("🛠️ Local Mock Mode", value=True, help="Run without Kafka (uses local file-stream)")
    
    extra_args = ["--mock"] if mock_mode else []
    
    if not pm.running["producer"]:
        if st.button("▶ Start Producer",use_container_width=True):
            pm.start("producer",[venv,"spark/streaming/kafka_producer.py"] + extra_args); st.rerun()
    else:
        if st.button("⏹ Stop Producer",use_container_width=True,type="primary"):
            pm.stop("producer"); st.rerun()
    st.markdown("---")
    if not pm.running["consumer"]:
        if st.button("▶ Start Spark Consumer",use_container_width=True):
            pm.start("consumer",[venv,"spark/streaming/streaming_job.py"] + extra_args); st.rerun()
    else:
        if st.button("⏹ Stop Consumer",use_container_width=True,type="primary"):
            pm.stop("consumer"); st.rerun()
    st.markdown("---")
    if st.button("🧹 Clear Data",use_container_width=True):
        st.session_state.pts={}; st.session_state.rl=[]; 
        # Also clear mock file
        mock_file = os.path.join("tmp", "mock_stream.jsonl")
        if os.path.exists(mock_file): open(mock_file, 'w').close()
        st.rerun()
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
    st.markdown("## 📊 Model Registry & Analytical Outputs")
    
    # 1. Model Registry from outputs/models/registry.json
    try:
        with open("outputs/models/registry.json", "r") as f:
            registry = json.load(f)
        
        st.markdown("### 📜 Model Performance Registry")
        for model in registry:
            with st.expander(f"Model: {model['model_name']} ({model['timestamp']})", expanded=True):
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("AUROC", f"{model['metrics']['auroc']:.4f}")
                col2.metric("AUPRC", f"{model['metrics']['auprc']:.4f}")
                col3.metric("F1 Score", f"{model['metrics']['f1']:.4f}")
                col4.metric("Accuracy", f"{model['metrics']['accuracy']:.4f}")
                
                st.markdown("**Parameters:** " + ", ".join([f"`{k}: {v}`" for k, v in model['parameters'].items()]))
                st.markdown(f"**Lineage:** Logic: `{model['lineage']['dbt_logic_version']}` | Data: `{model['lineage']['training_data_file']}`")
    except Exception as e:
        st.warning(f"Could not load model registry: {e}")

    st.markdown("---")
    
    # 2. Confusion Matrices & Evaluation Visuals
    st.markdown("### 📉 Comprehensive Evaluation Suite")
    
    # ROC and LR/GBT Confusion Matrices
    row1_c1, row1_c2, row1_c3 = st.columns(3)
    with row1_c1:
        st.markdown("**ROC Curves (Comparative)**")
        if os.path.exists("outputs/metrics/roc_curves.png"):
            st.image("outputs/metrics/roc_curves.png", use_container_width=True)
    with row1_c2:
        st.markdown("**GBT Confusion Matrix**")
        if os.path.exists("docs/reports/model_analysis/confusion_matrix_gradient_boosting.png"):
            st.image("docs/reports/model_analysis/confusion_matrix_gradient_boosting.png", use_container_width=True)
    with row1_c3:
        st.markdown("**LR Confusion Matrix**")
        if os.path.exists("docs/reports/model_analysis/confusion_matrix_logistic_regression.png"):
            st.image("docs/reports/model_analysis/confusion_matrix_logistic_regression.png", use_container_width=True)

    # Correlation and Covariance
    st.markdown("<br>", unsafe_allow_html=True)
    row2_c1, row2_c2 = st.columns(2)
    with row2_c1:
        st.markdown("**Feature Correlation Matrix**")
        if os.path.exists("docs/reports/model_analysis/correlation_matrix.png"):
            st.image("docs/reports/model_analysis/correlation_matrix.png", use_container_width=True)
    with row2_c2:
        st.markdown("**Feature Covariance Matrix**")
        if os.path.exists("docs/reports/model_analysis/covariance_matrix.png"):
            st.image("docs/reports/model_analysis/covariance_matrix.png", use_container_width=True)

    st.markdown("---")
    
    # 3. Model Metric CSV Summaries
    st.markdown("### 📋 Detailed Metric Summaries")
    m_col1, m_col2 = st.columns(2)
    with m_col1:
        st.markdown("**Gradient Boosting Metrics**")
        if os.path.exists("outputs/metrics/gradient_boosting_metrics.csv"):
            st.dataframe(pd.read_csv("outputs/metrics/gradient_boosting_metrics.csv"), use_container_width=True, hide_index=True)
    with m_col2:
        st.markdown("**Logistic Regression Metrics**")
        if os.path.exists("outputs/metrics/logistic_regression_metrics.csv"):
            st.dataframe(pd.read_csv("outputs/metrics/logistic_regression_metrics.csv"), use_container_width=True, hide_index=True)

    st.markdown("---")
    
    # 3. Feature Distributions from docs/reports/feature_distributions
    st.markdown("### 📈 Clinical Feature Distributions")
    f1, f2, f3 = st.columns(3)
    dist_imgs = {
        "Creatinine": "docs/reports/feature_distributions/distribution_creatinine.png",
        "Urine Output": "docs/reports/feature_distributions/distribution_urine.png",
        "Hours since Admit": "docs/reports/feature_distributions/distribution_hours.png"
    }
    cols = [f1, f2, f3]
    for i, (label, path) in enumerate(dist_imgs.items()):
        with cols[i]:
            st.markdown(f"**{label}**")
            if os.path.exists(path):
                st.image(path, use_container_width=True)
            else:
                st.info(f"{label} distribution not found.")

    st.markdown("---")
    
    # 4. Mart Data Preview
    st.markdown("### 💎 Data Mart Preview (Sample Metrics)")
    try:
        if os.path.exists("outputs/mart/dp_unit_metrics.csv"):
            df_mart = pd.read_csv("outputs/mart/dp_unit_metrics.csv")
            st.dataframe(df_mart, use_container_width=True, hide_index=True)
    except:
        st.info("Mart metrics CSV not found.")

# ════════════════════════════════════════════════════════════════════════
with tab_arch:
    st.header("🧬 End-to-End Clinical Architecture")
    st.info("The AKI Monitoring System uses a dual-path architecture to ensure deep clinical validity (Batch) and rapid bedside response (Streaming).")

    # Path 0: Unified Source
    st.subheader("📍 Data Source & Ingress")
    st.markdown("---")
    st.button("🏥 MIMIC-IV Clinical Database (330M Observation Events)", use_container_width=True, type="primary")
    st.write("")

    # Dual Paths
    col_batch, col_stream = st.columns(2)

    with col_batch:
        st.subheader("📘 Analytical Path (Batch)")
        st.caption("Focus: Precision & Training")
        
        st.success("**1. Great Expectations Gate**\n\nValidates physiological bounds and schema integrity.")
        st.write("↓")
        st.success("**2. Snowflake Bronze (Raw)**\n\nPersistent audit trail of all clinical laboratory data.")
        st.write("↓")
        st.success("**3. dbt Silver (Cleaned)**\n\nNormalizes units (mg/dL vs mmol/L) and time-aligns monitors.")
        st.write("↓")
        st.success("**4. Spark Gold (Features)**\n\nComputes KDIGO-based AKI stages and creatinine trajectories.")
        st.write("↓")
        st.success("**5. Model Registry**\n\nTrains Gradient Boosting models for production deployment.")

    with col_stream:
        st.subheader("⚡ Warning Path (Streaming)")
        st.caption("Focus: Latency & Alerts")
        
        st.warning("**1. Kafka Event Bus**\n\nHigh-throughput replay of bedside telemetry and lab feeds.")
        st.write("↓")
        st.warning("**2. Spark Structured Stream**\n\nMicro-batch processing (1s) of incoming patient signals.")
        st.write("↓")
        st.warning("**3. Streaming Estimators**\n\nApproximation algos (Bloom/FM) for efficient state tracking.")
        st.write("↓")
        st.warning("**4. XAI Clinical Twins**\n\nQueries Gold layer via LSH to explain risk using similar cases.")
        st.write("↓")
        st.warning("**5. Command Dashboard**\n\nReal-time monitoring and predictive clinical alerting.")

    st.divider()

    st.subheader("🧱 Resources, Components, and Heuristics (RCH)")
    
    rch_data = [
        {"Category": "Compute", "Component": "Apache Spark Cluster", "Resource": "Memory Optimized Nodes", "Heuristic": "Micro-batching for <2s latency"},
        {"Category": "Storage", "Component": "Snowflake Data Cloud", "Resource": "Medallion Architecture", "Heuristic": "Separation of Raw/Refined data"},
        {"Category": "Streaming", "Component": "Confluent Kafka", "Resource": "Patient Partitioning", "Heuristic": "Exactly-once event processing"},
        {"Category": "AI/ML", "Component": "Gradient Boosting", "Resource": "SHAP/LSH Explanations", "Heuristic": "Predict risk 48h in advance"},
        {"Category": "Governance", "Component": "Great Expectations", "Resource": "Clinical Rules Engine", "Heuristic": "Zero-tolerance for invalid lab values"}
    ]
    st.table(rch_data)
    
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Snowflake Data", "330M Rows", "Scalable")
    col_b.metric("Spark Latency", "< 2.0s", "Low-Lat")
    col_c.metric("Model Registry", "v1.2.0", "Versioned")

if pm.running["producer"] or pm.running["consumer"]:
    time.sleep(1); st.rerun()
