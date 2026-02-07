from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List


SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  manager TEXT,
  region TEXT,
  city TEXT,
  mode TEXT,
  query_key TEXT,
  query_ru TEXT,
  category_path TEXT,
  domain_pref TEXT,
  status TEXT DEFAULT 'PENDING',
  attempts INTEGER DEFAULT 0,
  last_error TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id INTEGER,
  org_id TEXT,
  url TEXT,
  source_mode TEXT,
  city TEXT,
  region TEXT,
  manager TEXT,
  query_ru TEXT,
  category_path TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(org_id) ON CONFLICT IGNORE
);

CREATE TABLE IF NOT EXISTS orgs (
  org_id TEXT PRIMARY KEY,
  name TEXT,
  address TEXT,
  website TEXT,
  ypage TEXT,
  phone TEXT,
  social TEXT,
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS org_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id TEXT,
  task_id INTEGER,
  manager TEXT,
  region TEXT,
  city TEXT,
  mode TEXT,
  query_ru TEXT,
  category_path TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS export_templates(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE,
  sql_text TEXT
);
"""

DEFAULT_TEMPLATES: list[tuple[str, str]] = [
    (
        "XLSX: полный (менеджер/регион/город/запрос/категория + колонки)",
        """
SELECT
  s.manager AS manager,
  s.region AS region,
  s.city AS city,
  COALESCE(s.query_ru, '') AS request,
  COALESCE(s.category_path, '') AS category,
  o.name AS name,
  o.address AS address,
  o.website AS website,
  o.ypage AS ypage,
  o.phone AS phone,
  o.social AS social
FROM org_sources s
JOIN orgs o ON o.org_id = s.org_id
ORDER BY s.id ASC
""".strip(),
    ),
]


class SQLiteDB:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        con.row_factory = sqlite3.Row
        return con

    def init(self) -> None:
        con = self.connect()
        try:
            con.executescript(SCHEMA)
            cur = con.cursor()
            for name, sql_text in DEFAULT_TEMPLATES:
                cur.execute(
                    "INSERT OR IGNORE INTO export_templates(name, sql_text) VALUES(?,?)",
                    (name, sql_text),
                )
            con.commit()
        finally:
            con.close()

    def add_tasks(self, tasks: List[Dict[str, Any]]) -> int:
        if not tasks:
            return 0
        con = self.connect()
        try:
            cur = con.cursor()
            for t in tasks:
                cur.execute(
                    """
INSERT INTO tasks(manager, region, city, mode, query_key, query_ru, category_path, domain_pref, status)
VALUES(?,?,?,?,?,?,?,?, 'PENDING')
""".strip(),
                    (
                        t.get("manager", ""),
                        t.get("region", ""),
                        t.get("city", ""),
                        t.get("mode", ""),
                        t.get("query_key", ""),
                        t.get("query_ru", ""),
                        t.get("category_path", ""),
                        t.get("domain_pref", "auto"),
                    ),
                )
            con.commit()
            return len(tasks)
        finally:
            con.close()

    def pick_next_task_for_links(self, max_attempts: int = 30):
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
SELECT *
FROM tasks
WHERE (mode LIKE '%LINKS%' OR mode LIKE '%MAPSCAN%')
  AND status IN ('PENDING','RETRY')
  AND attempts < ?
ORDER BY id ASC
LIMIT 1
""".strip(),
                (max_attempts,),
            )
            row = cur.fetchone()
            if not row:
                return None

            cur.execute(
                "UPDATE tasks SET status='RUNNING', attempts=attempts+1, updated_at=datetime('now') WHERE id=?",
                (row["id"],),
            )
            con.commit()

            cur.execute("SELECT * FROM tasks WHERE id=?", (row["id"],))
            r2 = cur.fetchone()
            return dict(r2) if r2 else None
        finally:
            con.close()

    def set_task_links_done(self, task_id: int, inserted: int = 0) -> None:
        con = self.connect()
        try:
            con.execute(
                "UPDATE tasks SET status='DONE', last_error=?, updated_at=datetime('now') WHERE id=?",
                (f"inserted_links={int(inserted or 0)}", task_id),
            )
            con.commit()
        finally:
            con.close()

    def set_task_waitcaptcha(self, task_id: int, err: str, worker: str) -> None:
        msg = f"{worker}: {err}" if worker else (err or "")
        con = self.connect()
        try:
            con.execute(
                "UPDATE tasks SET status='WAITCAPTCHA', last_error=?, updated_at=datetime('now') WHERE id=?",
                ((msg or "")[:2000], task_id),
            )
            con.commit()
        finally:
            con.close()

    def set_task_error(self, task_id: int, err: str, worker: str) -> None:
        msg = f"{worker}: {err}" if worker else (err or "")
        con = self.connect()
        try:
            con.execute(
                "UPDATE tasks SET status='ERROR', last_error=?, updated_at=datetime('now') WHERE id=?",
                ((msg or "")[:2000], task_id),
            )
            con.commit()
        finally:
            con.close()

    def retry_task(self, task_id: int) -> None:
        con = self.connect()
        try:
            con.execute(
                "UPDATE tasks SET status='RETRY', last_error=NULL, updated_at=datetime('now') WHERE id=?",
                (task_id,),
            )
            con.commit()
        finally:
            con.close()

    def requeue_all_tasks(self) -> None:
        con = self.connect()
        try:
            con.execute(
                """
UPDATE tasks
SET status='RETRY',
    attempts=0,
    last_error=NULL,
    updated_at=datetime('now')
WHERE status != 'RUNNING'
""".strip()
            )
            con.commit()
        finally:
            con.close()

    def delete_task(self, task_id: int) -> None:
        con = self.connect()
        try:
            con.execute("DELETE FROM org_sources WHERE task_id=?", (task_id,))
            con.execute("DELETE FROM links WHERE task_id=?", (task_id,))
            con.execute("DELETE FROM tasks WHERE id=?", (task_id,))
            con.commit()
        finally:
            con.close()

    def clear_tasks(self) -> None:
        con = self.connect()
        try:
            con.execute("DELETE FROM org_sources")
            con.execute("DELETE FROM orgs")
            con.execute("DELETE FROM links")
            con.execute("DELETE FROM tasks")
            con.commit()
        finally:
            con.close()

    def insert_links(self, task_row: dict, urls: List[str], source_mode: str) -> int:
        from core.utils import org_id_from_url

        if not urls:
            return 0

        con = self.connect()
        try:
            cur = con.cursor()
            inserted = 0
            for u in urls:
                oid = org_id_from_url(u)
                if not oid:
                    continue
                cur.execute(
                    """
INSERT OR IGNORE INTO links(task_id, org_id, url, source_mode, city, region, manager, query_ru, category_path)
VALUES(?,?,?,?,?,?,?,?,?)
""".strip(),
                    (
                        task_row["id"],
                        oid,
                        u,
                        source_mode,
                        task_row.get("city", ""),
                        task_row.get("region", ""),
                        task_row.get("manager", ""),
                        task_row.get("query_ru", ""),
                        task_row.get("category_path", ""),
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
            con.commit()
            return inserted
        finally:
            con.close()

    def fetch_next_link(self):
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
SELECT l.*
FROM links l
LEFT JOIN orgs o ON o.org_id = l.org_id
WHERE o.org_id IS NULL
ORDER BY l.id ASC
LIMIT 1
""".strip()
            )
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            con.close()

    def upsert_org(self, org: Dict[str, str]) -> None:
        con = self.connect()
        try:
            con.execute(
                """
INSERT INTO orgs(org_id, name, address, website, ypage, phone, social, updated_at)
VALUES(?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(org_id) DO UPDATE SET
  name=excluded.name,
  address=excluded.address,
  website=excluded.website,
  ypage=excluded.ypage,
  phone=excluded.phone,
  social=excluded.social,
  updated_at=datetime('now')
""".strip(),
                (
                    org.get("org_id", ""),
                    org.get("name", ""),
                    org.get("address", ""),
                    org.get("website", ""),
                    org.get("ypage", ""),
                    org.get("phone", ""),
                    org.get("social", ""),
                ),
            )
            con.commit()
        finally:
            con.close()

    def add_source(self, org_id: str, link_row: dict, mode: str) -> None:
        con = self.connect()
        try:
            con.execute(
                """
INSERT INTO org_sources(org_id, task_id, manager, region, city, mode, query_ru, category_path)
VALUES(?,?,?,?,?,?,?,?)
""".strip(),
                (
                    org_id,
                    link_row.get("task_id"),
                    link_row.get("manager", ""),
                    link_row.get("region", ""),
                    link_row.get("city", ""),
                    mode,
                    link_row.get("query_ru", ""),
                    link_row.get("category_path", ""),
                ),
            )
            con.commit()
        finally:
            con.close()

    def stats(self) -> Dict[str, Any]:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute("SELECT status, COUNT(*) c FROM tasks GROUP BY status")
            task = {r["status"]: r["c"] for r in cur.fetchall()}
            cur.execute("SELECT COUNT(*) c FROM links")
            total_links = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) c FROM orgs")
            total_orgs = cur.fetchone()[0]
            cur.execute(
                """
SELECT COUNT(*) c
FROM links l
LEFT JOIN orgs o ON o.org_id = l.org_id
WHERE o.org_id IS NULL
""".strip()
            )
            pending_orgs = cur.fetchone()[0]
            return {
                "tasks": task,
                "total_links": total_links,
                "total_orgs": total_orgs,
                "pending_orgs": pending_orgs,
            }
        finally:
            con.close()

    def list_tasks(self, limit: int = 200):
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
SELECT id, manager, region, city, mode, query_ru, category_path, status, attempts, last_error, updated_at
FROM tasks
ORDER BY id DESC
LIMIT ?
""".strip(),
                (int(limit or 200),),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            con.close()

    def templates(self):
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute("SELECT id, name FROM export_templates ORDER BY id ASC")
            return [dict(r) for r in cur.fetchall()]
        finally:
            con.close()

    def get_template_sql(self, template_id: int) -> str:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute("SELECT sql_text FROM export_templates WHERE id=?", (template_id,))
            r = cur.fetchone()
            return (r[0] if r else "").strip()
        finally:
            con.close()
