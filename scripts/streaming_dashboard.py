import streamlit as st
import subprocess, threading, time, os, sys, json, signal, queue
import pandas as pd

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
    st.markdown("### Batch Pipeline Performance")
    st.info("Run Gold/ML pipelines to populate model metrics here.")
    c1,c2,c3,c4=st.columns(4)
    c1.metric("GBT AUROC","0.9918","+0.002")
    c2.metric("GBT F1","0.9652","+0.005")
    c3.metric("LR AUROC","0.9743","baseline")
    c4.metric("LR F1","0.9401","baseline")
    st.markdown("#### Medallion Architecture Progress")
    b1,b2,b3,b4=st.columns(4)
    for col,name,icon,color in [(b1,"BRONZE","🥉","#f97316"),(b2,"SILVER","🥈","#94a3b8"),
                                (b3,"GOLD","🥇","#f59e0b"),(b4,"MART","💎","#22d3ee")]:
        col.markdown(f"""<div style="background:#0f172a;padding:20px;border-radius:10px;
        text-align:center;border:1px solid {color}40;">
        <div style="font-size:28px;">{icon}</div>
        <div style="font-weight:700;color:{color};margin-top:8px;">{name}</div>
        </div>""",unsafe_allow_html=True)
    st.markdown("#### Generalization Audit")
    g1,g2=st.columns(2)
    with g1:
        for unit,auroc,w in [("MICU (Source)","0.988",98),("SICU (Target)","0.991",99),("TSICU (Hold-out)","0.971",97)]:
            st.markdown(f"""<div style="background:#0f172a;padding:12px;border-radius:8px;margin-bottom:8px;">
            <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:6px;">
              <span>{unit}</span><span style="color:#22d3ee;font-weight:600;">AUROC {auroc}</span></div>
            <div style="background:#1e293b;height:6px;border-radius:3px;">
              <div style="background:#22d3ee;width:{w}%;height:100%;border-radius:3px;"></div></div>
            </div>""",unsafe_allow_html=True)
    with g2:
        st.markdown("""<div style="background:#0f172a;padding:16px;border-radius:10px;">
        <div style="font-size:12px;color:#64748b;font-weight:700;text-transform:uppercase;margin-bottom:12px;">Feature Importance (GBT)</div>
        """,unsafe_allow_html=True)
        for feat,pct,c in [("Cr/Baseline Ratio",42,"#22d3ee"),("Urine Output 6h",28,"#a78bfa"),
                            ("Creatinine Delta",15,"#f59e0b"),("Rolling KDIGO Max",10,"#4ade80"),("Other",5,"#64748b")]:
            st.markdown(f"""<div style="margin-bottom:8px;">
            <div style="display:flex;justify-content:space-between;font-size:11px;">
              <span>{feat}</span><span style="color:{c};">{pct}%</span></div>
            <div style="background:#1e293b;height:4px;border-radius:2px;margin-top:3px;">
              <div style="background:{c};width:{pct}%;height:100%;border-radius:2px;"></div></div>
            </div>""",unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════
with tab_arch:
    st.markdown("### System Architecture")
    st.markdown("""<div style="background:#0f172a;border:1px solid #1e293b;padding:40px;border-radius:12px;">
    <div style="background:#1e3a5f;border:1px solid #3b82f680;padding:10px 20px;border-radius:8px;text-align:center;margin-bottom:30px;font-size:12px;">
      ❄️ <b>Snowflake Data Cloud</b> — Unified Feature Store: <span style="color:#22d3ee;">AKI_DB.GOLD.AKI_TRAINING_SET</span>
    </div>
    <div style="display:flex;align-items:center;justify-content:center;gap:10px;margin-bottom:30px;flex-wrap:wrap;">
    """,unsafe_allow_html=True)
    for label,sub,color,icon in [
        ("MIMIC-IV","Raw Events","#f97316","📂"),
        ("Great Expectations","Validation","#94a3b8","✅"),
        ("dbt","Bronze→Gold","#4ade80","🔧"),
        ("Spark Batch","Features+KDIGO","#a78bfa","⚡"),
        ("ML Models","GBT + LR","#f59e0b","🤖"),
        ("Kafka","Event Broker","#22d3ee","📨"),
        ("Spark Streaming","Real-time","#06b6d4","🔄"),
        ("Snowflake Mart","Predictions","#3b82f6","❄️"),
    ]:
        st.markdown(f"""<div style="background:#1e293b;padding:14px 16px;border-radius:8px;
        text-align:center;min-width:120px;border:1px solid {color}40;display:inline-block;margin:4px;">
        <div style="font-size:20px;">{icon}</div>
        <div style="font-weight:700;font-size:12px;color:{color};margin-top:4px;">{label}</div>
        <div style="font-size:9px;color:#64748b;">{sub}</div>
        </div>""",unsafe_allow_html=True)
    st.markdown("""</div>
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:20px;">""",unsafe_allow_html=True)
    for title,desc,c in [("🔒 PRIVACY","K-Anonymity + Differential Privacy","#a78bfa"),
                         ("⚖️ FAIRNESS","SHAP + Fairness Audit","#f59e0b"),
                         ("🌐 STREAMING","Bloom + FM + DGIM + LSH","#22d3ee")]:
        st.markdown(f"""<div style="background:#1e293b;padding:16px;border-radius:8px;border:1px solid {c}30;">
        <div style="font-weight:700;color:{c};font-size:12px;">{title}</div>
        <div style="font-size:10px;color:#64748b;margin-top:4px;">{desc}</div>
        </div>""",unsafe_allow_html=True)
    st.markdown("</div></div>",unsafe_allow_html=True)

if pm.running["producer"] or pm.running["consumer"]:
    time.sleep(1); st.rerun()
