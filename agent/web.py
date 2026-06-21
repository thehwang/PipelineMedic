"""PipelineMedic Web UI + API.

A small dashboard for the human-in-the-loop demo:
  - a background loop scans Airflow every PM_SCAN_INTERVAL seconds (default 600),
  - failures are diagnosed and shown as incident cards,
  - one click approves a gated clear+rerun (transient only), or escalates,
  - an optional "Run agent" button runs the full Qwen reasoning loop.

Run: ./.venv/bin/python scripts/serve.py   (or: uvicorn agent.web:app)
"""

from __future__ import annotations

import asyncio
import contextlib

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from . import scanner
from .loop import run_agent

app = FastAPI(title="PipelineMedic")


class TaskRef(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    try_number: int = 1


class AutoFix(BaseModel):
    auto_fix: bool


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/api/state")
def api_state() -> JSONResponse:
    return JSONResponse(scanner.get_state())


@app.post("/api/scan")
async def api_scan() -> JSONResponse:
    state = await run_in_threadpool(scanner.scan_once)
    return JSONResponse(state)


@app.post("/api/approve")
async def api_approve(ref: TaskRef) -> JSONResponse:
    inc = await run_in_threadpool(
        scanner.approve, ref.dag_id, ref.run_id, ref.task_id, ref.try_number
    )
    return JSONResponse(inc)


@app.post("/api/escalate")
async def api_escalate(ref: TaskRef) -> JSONResponse:
    inc = await run_in_threadpool(
        scanner.escalate, ref.dag_id, ref.run_id, ref.task_id, ref.try_number
    )
    return JSONResponse(inc)


@app.post("/api/agent")
async def api_agent(ref: TaskRef) -> JSONResponse:
    result = await run_in_threadpool(
        run_agent,
        {
            "dag_id": ref.dag_id,
            "run_id": ref.run_id,
            "task_id": ref.task_id,
            "try_number": ref.try_number,
        },
    )
    return JSONResponse({"final": result.get("final", ""), "steps": result.get("steps")})


@app.post("/api/settings")
def api_settings(s: AutoFix) -> JSONResponse:
    scanner.set_auto_fix(s.auto_fix)
    return JSONResponse(scanner.get_state())


async def _scan_loop() -> None:
    await asyncio.sleep(1)
    while True:
        with contextlib.suppress(Exception):
            await run_in_threadpool(scanner.scan_once)
        await asyncio.sleep(scanner.get_interval())


@app.on_event("startup")
async def _startup() -> None:
    app.state.scan_task = asyncio.create_task(_scan_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    task = getattr(app.state, "scan_task", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>PipelineMedic</title>
<style>
  :root{
    --bg:#0b0f17; --panel:#141b27; --panel2:#1b2433; --line:#243044;
    --txt:#e6edf6; --muted:#8a98ad; --accent:#4f8cff;
    --green:#36d399; --amber:#fbbd23; --red:#f87272; --orange:#ff9f43; --gray:#5b6675;
  }
  *{box-sizing:border-box}
  body{margin:0;font:14px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
       background:var(--bg);color:var(--txt)}
  header{display:flex;align-items:center;gap:16px;padding:16px 24px;
         border-bottom:1px solid var(--line);background:var(--panel);position:sticky;top:0;z-index:5}
  header h1{font-size:18px;margin:0;letter-spacing:.3px}
  header .dot{width:9px;height:9px;border-radius:50%;background:var(--green);
              box-shadow:0 0 10px var(--green);display:inline-block;margin-right:8px}
  .sub{color:var(--muted);font-size:12px}
  .spacer{flex:1}
  .ctrl{display:flex;align-items:center;gap:10px}
  button{font:inherit;border:1px solid var(--line);background:var(--panel2);color:var(--txt);
         padding:7px 12px;border-radius:8px;cursor:pointer;transition:.15s}
  button:hover{border-color:var(--accent)}
  button.primary{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}
  button.warn{background:#3a2330;border-color:#5b2f3f;color:#ffd0d0}
  button:disabled{opacity:.45;cursor:not-allowed}
  .switch{display:flex;align-items:center;gap:8px;color:var(--muted)}
  .toggle{width:42px;height:24px;border-radius:14px;background:var(--line);position:relative;cursor:pointer;transition:.15s}
  .toggle.on{background:var(--green)}
  .toggle:after{content:"";position:absolute;top:3px;left:3px;width:18px;height:18px;border-radius:50%;
                background:#fff;transition:.15s}
  .toggle.on:after{left:21px}
  main{display:grid;grid-template-columns:1fr 320px;gap:18px;padding:20px 24px;max-width:1280px;margin:0 auto}
  .cards{display:flex;flex-direction:column;gap:14px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px 18px}
  .card.fixable{border-left:3px solid var(--green)}
  .card.human{border-left:3px solid var(--red)}
  .card.done{opacity:.6}
  .row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .title{font-weight:600;font-size:15px}
  .badge{font-size:11px;padding:2px 9px;border-radius:20px;font-weight:600;white-space:nowrap}
  .b-cat{background:#1f2a3d;color:#bcd2ff}
  .b-needs_review{background:#3a3115;color:var(--amber)}
  .b-needs_human{background:#3a1f1f;color:var(--red)}
  .b-recovered{background:#143028;color:var(--green)}
  .b-rerun_failed{background:#3a2a15;color:var(--orange)}
  .b-escalated{background:#26303d;color:var(--gray)}
  .desc{color:var(--muted);margin:8px 0 4px}
  .ids{color:#6f7d92;font-size:12px;font-family:ui-monospace,Menlo,monospace}
  details{margin-top:10px}
  summary{cursor:pointer;color:var(--muted);font-size:12px}
  pre{white-space:pre-wrap;background:#0a0e15;border:1px solid var(--line);border-radius:8px;
      padding:10px;color:#c9d4e3;font-size:11.5px;max-height:220px;overflow:auto;margin:8px 0 0}
  .actions{display:flex;gap:8px;margin-top:14px;flex-wrap:wrap}
  aside{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px;height:fit-content;position:sticky;top:84px}
  aside h3{margin:0 0 10px;font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
  .feed{display:flex;flex-direction:column;gap:7px;max-height:70vh;overflow:auto}
  .feed div{font-size:12.5px;color:#aab6c8}
  .feed .t{color:#566173;font-family:ui-monospace,Menlo,monospace;margin-right:6px}
  .empty{color:var(--muted);text-align:center;padding:40px}
  .agentout{margin-top:10px;border:1px dashed var(--line);border-radius:8px;padding:10px;background:#0e1420;color:#cfe0ff;font-size:12.5px;white-space:pre-wrap}
</style>
</head>
<body>
<header>
  <h1><span class="dot"></span>PipelineMedic</h1>
  <span class="sub" id="meta">loading…</span>
  <div class="spacer"></div>
  <div class="switch">auto-fix
    <div class="toggle" id="toggle" onclick="toggleAuto()"></div>
  </div>
  <button class="primary" onclick="scanNow()" id="scanBtn">Scan now</button>
</header>

<main>
  <div class="cards" id="cards"><div class="empty">Scanning…</div></div>
  <aside>
    <h3>Activity</h3>
    <div class="feed" id="feed"></div>
  </aside>
</main>

<script>
let STATE = null;

async function getState(){ const r = await fetch('/api/state'); STATE = await r.json(); render(); }
async function scanNow(){
  const b=document.getElementById('scanBtn'); b.disabled=true; b.textContent='Scanning…';
  await fetch('/api/scan',{method:'POST'}); await getState();
  b.disabled=false; b.textContent='Scan now';
}
async function toggleAuto(){
  await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({auto_fix: !STATE.auto_fix})}); await getState();
}
function ref(i){ return {dag_id:i.dag_id, run_id:i.run_id, task_id:i.task_id, try_number:i.try_number}; }
async function approve(i, btn){
  btn.disabled=true; btn.textContent='Rerunning…';
  await fetch('/api/approve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(ref(i))});
  await getState();
}
async function escalate(i){
  await fetch('/api/escalate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(ref(i))});
  await getState();
}
async function runAgent(i, btn, id){
  btn.disabled=true; btn.textContent='Thinking…';
  const out=document.getElementById(id); out.style.display='block'; out.textContent='Qwen is investigating…';
  const r=await fetch('/api/agent',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(ref(i))});
  const d=await r.json(); out.textContent='🧠 '+(d.final||'(no output)');
  btn.disabled=false; btn.textContent='Run agent (LLM)';
  getState();
}
function esc(s){ return (s||'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }

function render(){
  if(!STATE) return;
  const t=document.getElementById('toggle'); t.className='toggle'+(STATE.auto_fix?' on':'');
  const open = STATE.incidents.filter(i=>!['recovered','escalated'].includes(i.status)).length;
  document.getElementById('meta').textContent =
    `last scan ${STATE.last_scan||'—'} · every ${Math.round(STATE.scan_interval/60)} min · ${open} open / ${STATE.incidents.length} total`;

  const wrap=document.getElementById('cards');
  if(!STATE.incidents.length){ wrap.innerHTML='<div class="empty">No failures detected. 🎉</div>'; }
  else wrap.innerHTML = STATE.incidents.map((i,idx)=>{
    const done=['recovered','escalated'].includes(i.status);
    const cls=i.auto_fixable?'fixable':'human';
    const canRerun = i.auto_fixable && ['needs_review','rerun_failed'].includes(i.status);
    const outId='agent_'+idx;
    let actions='';
    if(canRerun) actions+=`<button class="primary" onclick='approve(${JSON.stringify(ref(i))},this)'>Approve &amp; Rerun</button>`;
    if(!i.auto_fixable && i.status==='needs_human') actions+=`<button class="warn" onclick='escalate(${JSON.stringify(ref(i))})'>Escalate to human</button>`;
    actions+=`<button onclick='runAgent(${JSON.stringify(ref(i))},this,"${outId}")'>Run agent (LLM)</button>`;
    return `<div class="card ${cls} ${done?'done':''}">
      <div class="row">
        <span class="title">${esc(i.dag_id)}.${esc(i.task_id)}</span>
        <span class="badge b-cat">${esc(i.category)}</span>
        <span class="badge b-${i.status}">${esc(i.status.replace(/_/g,' '))}</span>
        ${i.auto_fixable?'<span class="badge b-recovered">auto-fixable</span>':'<span class="badge b-needs_human">needs human</span>'}
      </div>
      <div class="desc">${esc(i.summary)}</div>
      <div class="ids">run ${esc(i.run_id)}</div>
      <details><summary>evidence</summary><pre>${esc(i.evidence)}</pre></details>
      <div class="actions">${actions}</div>
      <div class="agentout" id="${outId}" style="display:none"></div>
    </div>`;
  }).join('');

  document.getElementById('feed').innerHTML =
    (STATE.events||[]).map(e=>`<div><span class="t">${esc(e.ts)}</span>${esc(e.msg)}</div>`).join('')
    || '<div class="empty">no activity yet</div>';
}

getState();
setInterval(getState, 4000);
</script>
</body>
</html>
"""
