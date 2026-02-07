from __future__ import annotations

from typing import Any, Dict, List

import psycopg2
import psycopg2.extras

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  k TEXT PRIMARY KEY,
  v TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
  id BIGSERIAL PRIMARY KEY,
  manager TEXT,
  region TEXT,
  city TEXT,
  mode TEXT,
  query_key TEXT,
  query_ru TEXT,
  category_path TEXT,
  domain_pref TEXT,
  status TEXT DEFAULT 'PENDING',
  attempts INT DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS links (
  id BIGSERIAL PRIMARY KEY,
  task_id BIGINT,
  org_id TEXT UNIQUE,
  url TEXT,
  source_mode TEXT,
  city TEXT,
  region TEXT,
  manager TEXT,
  query_ru TEXT,
  category_path TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orgs (
  org_id TEXT PRIMARY KEY,
  name TEXT,
  address TEXT,
  website TEXT,
  ypage TEXT,
  phone TEXT,
  social TEXT,
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS org_sources (
  id BIGSERIAL PRIMARY KEY,
  org_id TEXT,
  task_id BIGINT,
  manager TEXT,
  region TEXT,
  city TEXT,
  mode TEXT,
  query_ru TEXT,
  category_path TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS export_templates(
  id BIGSERIAL PRIMARY KEY,
  name TEXT UNIQUE,
  sql_text TEXT
);
"""

DEFAULT_TEMPLATES = [
    (
        "XLSX: full (manager/region/city/request/category + columns)",
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
    (
        "B24: minimal contacts",
        """
SELECT
  o.name AS name,
  o.phone AS phone,
  o.website AS website,
  o.address AS address,
  o.ypage AS ypage
FROM orgs o
ORDER BY o.updated_at DESC
""".strip(),
    ),
]


class PostgresDB:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def connect(self):
        return psycopg2.connect(self.dsn)

    def init(self) -> None:
        con = self.connect()
        try:
            con.autocommit = True
            cur = con.cursor()
            cur.execute(SCHEMA)
            for name, sql_text in DEFAULT_TEMPLATES:
                cur.execute(
                    "INSERT INTO export_templates(name, sql_text) VALUES(%s,%s) ON CONFLICT(name) DO NOTHING",
                    (name, sql_text),
                )
        finally:
            con.close()

    def add_tasks(self, tasks: List[Dict[str, Any]]) -> int:
        con = self.connect()
        try:
            cur = con.cursor()
            for t in tasks:
                cur.execute(
                    """
INSERT INTO tasks(manager, region, city, mode, query_key, query_ru, category_path, domain_pref, status)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'PENDING')
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

    def pick_next_task_for_links(self, max_attempts: int = 15):
        con = self.connect()
        try:
            cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
SELECT *
FROM tasks
WHERE mode LIKE 'LINKS%%'
  AND status IN ('PENDING','RETRY')
  AND attempts < %s
ORDER BY id ASC
LIMIT 1
FOR UPDATE SKIP LOCKED
""".strip(),
                (max_attempts,),
            )
            row = cur.fetchone()
            if not row:
                con.rollback()
                return None

            cur2 = con.cursor()
            cur2.execute(
                "UPDATE tasks SET status='RUNNING', attempts=attempts+1, updated_at=NOW() WHERE id=%s",
                (row["id"],),
            )
            con.commit()
            return dict(row)
        finally:
            con.close()

    def set_task_links_done(self, task_id: int, inserted: int = 0) -> None:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute(
                "UPDATE tasks SET status='DONE', last_error=%s, updated_at=NOW() WHERE id=%s",
                (f"inserted_links={inserted}", task_id),
            )
            con.commit()
        finally:
            con.close()

    def set_task_waitcaptcha(self, task_id: int, err: str, worker: str = "") -> None:
        msg = (f"[{worker}] " if worker else "") + (err or "")
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute(
                "UPDATE tasks SET status='WAITCAPTCHA', last_error=%s, updated_at=NOW() WHERE id=%s",
                (msg[:2000], task_id),
            )
            con.commit()
        finally:
            con.close()

    def set_task_error(self, task_id: int, err: str, worker: str = "") -> None:
        msg = (f"[{worker}] " if worker else "") + (err or "")
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute(
                "UPDATE tasks SET status='ERROR', last_error=%s, updated_at=NOW() WHERE id=%s",
                (msg[:2000], task_id),
            )
            con.commit()
        finally:
            con.close()

    def retry_task(self, task_id: int) -> None:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute(
                "UPDATE tasks SET status='RETRY', attempts=0, last_error=NULL, updated_at=NOW() WHERE id=%s",
                (task_id,),
            )
            con.commit()
        finally:
            con.close()

    def requeue_all_tasks(self) -> None:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute("DELETE FROM org_sources")
            cur.execute("DELETE FROM links")
            cur.execute("UPDATE tasks SET status='RETRY', attempts=0, last_error=NULL, updated_at=NOW()")
            con.commit()
        finally:
            con.close()

    def delete_task(self, task_id: int) -> None:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute("DELETE FROM org_sources WHERE task_id=%s", (task_id,))
            cur.execute("DELETE FROM links WHERE task_id=%s", (task_id,))
            cur.execute("DELETE FROM tasks WHERE id=%s", (task_id,))
            con.commit()
        finally:
            con.close()

    def clear_tasks(self) -> None:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute("TRUNCATE TABLE org_sources RESTART IDENTITY")
            cur.execute("TRUNCATE TABLE orgs RESTART IDENTITY")
            cur.execute("TRUNCATE TABLE links RESTART IDENTITY")
            cur.execute("TRUNCATE TABLE tasks RESTART IDENTITY")
            con.commit()
        finally:
            con.close()

    def insert_links(self, task_row: dict, urls: List[str], source_mode: str) -> int:
        from core.utils import org_id_from_url

        con = self.connect()
        try:
            cur = con.cursor()
            inserted = 0
            for u in (urls or []):
                oid = org_id_from_url(u)
                if not oid:
                    continue
                cur.execute(
                    """
INSERT INTO links(task_id, org_id, url, source_mode, city, region, manager, query_ru, category_path)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(org_id) DO NOTHING
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
            cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
            cur = con.cursor()
            cur.execute(
                """
INSERT INTO orgs(org_id, name, address, website, ypage, phone, social, updated_at)
VALUES(%s,%s,%s,%s,%s,%s,%s,NOW())
ON CONFLICT(org_id) DO UPDATE SET
  name=EXCLUDED.name,
  address=EXCLUDED.address,
  website=EXCLUDED.website,
  ypage=EXCLUDED.ypage,
  phone=EXCLUDED.phone,
  social=EXCLUDED.social,
  updated_at=NOW()
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
            cur = con.cursor()
            cur.execute(
                """
INSERT INTO org_sources(org_id, task_id, manager, region, city, mode, query_ru, category_path)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
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
            cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT status, COUNT(*) c FROM tasks GROUP BY status")
            task = {r["status"]: r["c"] for r in cur.fetchall()}
            cur.execute("SELECT COUNT(*) c FROM links")
            total_links = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) c FROM orgs")
            total_orgs = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) c FROM links l LEFT JOIN orgs o ON o.org_id=l.org_id WHERE o.org_id IS NULL")
            pending_orgs = cur.fetchone()["c"]
            return {"tasks": task, "total_links": total_links, "total_orgs": total_orgs, "pending_orgs": pending_orgs}
        finally:
            con.close()

    def list_tasks(self, limit: int = 200):
        con = self.connect()
        try:
            cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
SELECT id, manager, region, city, mode, query_ru, category_path, status, attempts, last_error, updated_at
FROM tasks
ORDER BY id DESC
LIMIT %s
""".strip(),
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            con.close()

    def templates(self):
        con = self.connect()
        try:
            cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, name FROM export_templates ORDER BY id ASC")
            return [dict(r) for r in cur.fetchall()]
        finally:
            con.close()

    def get_template_sql(self, template_id: int) -> str:
        con = self.connect()
        try:
            cur = con.cursor()
            cur.execute("SELECT sql_text FROM export_templates WHERE id=%s", (template_id,))
            r = cur.fetchone()
            return (r[0] if r else "").strip()
        finally:
            con.close()
