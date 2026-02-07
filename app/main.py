import os
import logging
import threading
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from rich.logging import RichHandler

from app.auth import basic_auth
from core.db_factory import make_db
from core.config_loader import load_config, build_tasks
from core.pipeline import Pipeline
from core.export import export_xlsx

from providers.yandex.link_collector import collect_task_links
from providers.yandex.info_collector import parse_org_page

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, markup=False)],
)
log = logging.getLogger("parser_maps_pro")

app = FastAPI()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", "./runtime")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

EXPORT_XLSX = os.getenv("EXPORT_XLSX", "./output/results.xlsx")
EXPORT_SHEET = os.getenv("EXPORT_SHEET", "Results")

HEADLESS = os.getenv("HEADLESS", "0").strip() in ("1", "true", "True", "YES", "yes")
SAVE_DEBUG = os.getenv("SAVE_DEBUG", "1").strip() in ("1", "true", "True", "YES", "yes")
DOMAIN_PREF = os.getenv("Y_DOMAIN_PREF", "auto").strip() or "auto"
MAPSCAN_ALWAYS = os.getenv("MAPSCAN_ALWAYS", "1").strip() in ("1", "true", "True", "YES", "yes")

db = make_db()
db.init()

_state = {"cfg_path": "", "excludes": [], "pipeline": None}

UI_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Parser Maps Pro</title>
  <style>
    body{font-family:Arial;margin:18px}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
    .card{border:1px solid #ddd;border-radius:10px;padding:12px;margin:10px 0}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #ddd;padding:6px;font-size:12px;vertical-align:top}
    th{background:#f6f6f6}
    .badge{padding:2px 8px;border-radius:12px;border:1px solid #ccc;font-size:12px}
    .btn{padding:8px 12px;border:1px solid #111;background:#111;color:#fff;border-radius:8px;cursor:pointer}
    .btn2{padding:8px 12px;border:1px solid #111;background:#fff;color:#111;border-radius:8px;cursor:pointer}
    small{color:#666}
  </style>
</head>
<body>
  <h2>Parser Maps Pro</h2>

  <div class="card">
    <div class="row">
      <input type="file" id="cfg">
      <button class="btn" onclick="upload()">Обновить config.xlsx</button>
      <button class="btn2" onclick="start()">Старт</button>
      <button class="btn2" onclick="stop()">Стоп</button>
      <button class="btn2" onclick="exportXlsx()">Экспорт в .xlsx</button>
      <button class="btn2" onclick="clearTasks()">Очистить задачи</button>
      <button class="btn2" onclick="requeueAll()">Вернуть всё</button>
    </div>

    <div style="margin-top:8px">
      <span class="badge" id="st">tasks</span>
      <span class="badge" id="links">links</span>
      <span class="badge" id="orgs">orgs</span>
      <span class="badge" id="pending">pending_orgs</span>
      <span class="badge" id="run">running</span>
    </div>

    <div style="margin-top:8px">
      <small>Если статус WAITCAPTCHA: реши капчу в открытом Chrome-профиле или подожди, затем нажми Retry.</small>
    </div>
  </div>

  <div class="card">
    <div class="row">
      <label>Шаблон:</label>
      <select id="tpl"></select>
      <button class="btn2" onclick="exportXlsx()">Импорт</button>
    </div>
  </div>

  <div class="card">
    <div class="row">
      <b>Задачи</b>
      <button class="btn2" onclick="refresh()">Обновить</button>
    </div>
    <div style="margin-top:10px;max-height:520px;overflow:auto">
      <table id="t"></table>
    </div>
  </div>

<script>
async function api(path, opt){
  const r = await fetch(path, opt);
  if(!r.ok) throw new Error(await r.text());
  return await r.json();
}

async function upload(){
  const f = document.getElementById('cfg').files[0];
  if(!f){ alert('Выбери config.xlsx'); return; }
  const fd = new FormData();
  fd.append('file', f);
  const j = await api('/api/upload_config', {method:'POST', body: fd});
  alert('ОК: задач=' + j.tasks_added);
  await refresh();
}

async function start(){
  await api('/api/start', {method:'POST'});
  await refresh();
}

async function stop(){
  await api('/api/stop', {method:'POST'});
  await refresh();
}

async function retry(id){
  await api('/api/task_retry?id=' + id, {method:'POST'});
  await refresh();
}

async function requeue(id){
  await api('/api/task_requeue?id=' + id, {method:'POST'});
  await refresh();
}

async function requeueAll(){
  await api('/api/tasks_requeue_all', {method:'POST'});
  await refresh();
}

async function delTask(id){
  if(!confirm('Удалить задачу id=' + id + '?')) return;
  await api('/api/task_delete?id=' + id, {method:'POST'});
  await refresh();
}

async function clearTasks(){
  if(!confirm('Очистить все задачи и результаты?')) return;
  await api('/api/tasks_clear', {method:'POST'});
  await refresh();
}

async function exportXlsx(){
  const tpl = document.getElementById('tpl').value || 1;
  const j = await api('/api/export?template_id=' + tpl, {method:'POST'});
  window.location = '/api/download?path=' + encodeURIComponent(j.xlsx);
}

async function refresh(){
  const s = await api('/api/status');
  document.getElementById('st').innerText = 'tasks ' + JSON.stringify(s.stats.tasks);
  document.getElementById('links').innerText = 'links ' + s.stats.total_links;
  document.getElementById('orgs').innerText = 'orgs ' + s.stats.total_orgs;
  document.getElementById('pending').innerText = 'pending_orgs ' + s.stats.pending_orgs;
  document.getElementById('run').innerText = 'running ' + (s.running ? 'YES' : 'NO');

  const t = await api('/api/tasks?limit=200');
  const rows = t.tasks;

  const el = document.getElementById('t');
  let html = '<tr><th>ID</th><th>Регион</th><th>Город</th><th>Режим</th><th>Запрос</th><th>Категория</th><th>Статус</th><th>Попытки</th><th>Ошибки</th><th>Обновлён</th><th>Действие</th></tr>';
  for(const r of rows){
    const err = (r.last_error || '').toString();
    const errShort = err.length > 220 ? err.slice(0,220) + '...' : err;

    let act = '';
    act += '<button class="btn2" onclick="requeue(' + r.id + ')">В работу</button> ';
    if(r.status === 'WAITCAPTCHA' || r.status === 'ERROR'){
      act += '<button class="btn2" onclick="retry(' + r.id + ')">Retry</button> ';
    }
    act += '<button class="btn2" onclick="delTask(' + r.id + ')">Удалить</button>';

    html += '<tr>'
      + '<td>' + r.id + '</td>'
      + '<td>' + (r.region || '') + '</td>'
      + '<td>' + (r.city || '') + '</td>'
      + '<td>' + (r.mode || '') + '</td>'
      + '<td>' + (r.query_ru || '') + '</td>'
      + '<td>' + (r.category_path || '') + '</td>'
      + '<td>' + (r.status || '') + '</td>'
      + '<td>' + (r.attempts || 0) + '</td>'
      + '<td>' + errShort.replaceAll('<','&lt;') + '</td>'
      + '<td>' + (r.updated_at || '') + '</td>'
      + '<td>' + act + '</td>'
      + '</tr>';
  }
  el.innerHTML = html;

  const tp = await api('/api/templates');
  const sel = document.getElementById('tpl');
  if(sel.options.length === 0){
    for(const it of tp.templates){
      const o = document.createElement('option');
      o.value = it.id;
      o.textContent = it.name;
      sel.appendChild(o);
    }
  }
}

refresh();
setInterval(refresh, 7000);
</script>

</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(basic_auth)])
def home():
    return UI_HTML


@app.post("/api/upload_config", dependencies=[Depends(basic_auth)])
async def upload_config(file: UploadFile = File(...)):
    if not (file.filename or "").lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Нужен файл .xlsx")

    cfg_path = DATA_DIR / "config.xlsx"
    cfg_path.write_bytes(await file.read())

    cfg = load_config(str(cfg_path))
    _state["cfg_path"] = str(cfg_path)
    _state["excludes"] = cfg.get("excludes", [])

    tasks = build_tasks(cfg, domain_pref=DOMAIN_PREF, mapscan_always=MAPSCAN_ALWAYS)
    added = db.add_tasks(tasks)

    return JSONResponse({"ok": True, "saved": str(cfg_path), "tasks_added": added, "excludes": len(_state["excludes"])})


def _get_pipeline_running() -> bool:
    p = _state.get("pipeline")
    if not p:
        return False
    if hasattr(p, "is_alive") and p.is_alive():
        return True
    _state["pipeline"] = None
    return False


@app.post("/api/start", dependencies=[Depends(basic_auth)])
def api_start():
    if _get_pipeline_running():
        return JSONResponse({"ok": True, "running": True, "stats": db.stats()})

    debug_dir = str((RUNTIME_DIR / "debug").resolve())
    os.makedirs(debug_dir, exist_ok=True)
    excludes = _state.get("excludes", [])

    def links_fn(driver, task_row: dict) -> int:
        urls = collect_task_links(
            driver=driver,
            task=task_row,
            excludes=excludes,
            debug_dir=debug_dir,
            save_debug=SAVE_DEBUG,
            mapscan=("MAPSCAN" in (task_row.get("mode") or "")),
        )
        return db.insert_links(task_row, urls or [], source_mode=task_row.get("mode", "unknown"))

    def info_fn(driver, link_row: dict) -> dict:
        url = link_row.get("url") or ""
        return parse_org_page(driver, url=url, debug_dir=debug_dir, save_debug=SAVE_DEBUG)

    p = Pipeline(
        db=db,
        headless=HEADLESS,
        runtime_dir=str(RUNTIME_DIR),
        logger=log,
        links_fn=links_fn,
        info_fn=info_fn,
    )
    p.start()
    _state["pipeline"] = p

    return JSONResponse({"ok": True, "running": True, "stats": db.stats()})


@app.post("/api/stop", dependencies=[Depends(basic_auth)])
def api_stop():
    p = _state.get("pipeline")
    if p:
        if hasattr(p, "stop_async"):
            p.stop_async()
        else:
            threading.Thread(target=p.stop, daemon=True).start()
        _state["pipeline"] = None
    return JSONResponse({"ok": True, "running": False, "stats": db.stats()})


@app.get("/api/status", dependencies=[Depends(basic_auth)])
def api_status():
    return JSONResponse({"ok": True, "running": _get_pipeline_running(), "stats": db.stats()})


@app.get("/api/tasks", dependencies=[Depends(basic_auth)])
def api_tasks(limit: int = 200):
    return JSONResponse({"ok": True, "tasks": db.list_tasks(limit=limit)})


@app.post("/api/task_retry", dependencies=[Depends(basic_auth)])
def api_task_retry(id: int):
    db.retry_task(id)
    return JSONResponse({"ok": True})


@app.post("/api/task_requeue", dependencies=[Depends(basic_auth)])
def api_task_requeue(id: int):
    db.requeue_task(id, reset_attempts=True, clear_links=True, clear_sources=True)
    return JSONResponse({"ok": True})


@app.post("/api/tasks_requeue_all", dependencies=[Depends(basic_auth)])
def api_tasks_requeue_all():
    db.requeue_all_tasks()
    return JSONResponse({"ok": True})


@app.post("/api/task_delete", dependencies=[Depends(basic_auth)])
def api_task_delete(id: int):
    db.delete_task(id)
    return JSONResponse({"ok": True})


@app.post("/api/tasks_clear", dependencies=[Depends(basic_auth)])
def api_tasks_clear():
    db.clear_tasks()
    return JSONResponse({"ok": True})


@app.get("/api/templates", dependencies=[Depends(basic_auth)])
def api_templates():
    return JSONResponse({"ok": True, "templates": db.templates()})


@app.post("/api/export", dependencies=[Depends(basic_auth)])
def api_export(template_id: int = 1):
    sql_text = db.get_template_sql(template_id)
    if not sql_text:
        raise HTTPException(status_code=404, detail="Template not found")

    out = export_xlsx(db, EXPORT_XLSX, EXPORT_SHEET, sql_text)
    return JSONResponse({"ok": True, "xlsx": str(Path(out).resolve())})


@app.get("/api/download", dependencies=[Depends(basic_auth)])
def api_download(path: str):
    p = Path(path).resolve()
    if not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(p), filename=p.name)
