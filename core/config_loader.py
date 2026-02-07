from __future__ import annotations

import pandas as pd


def _read_sheet(path: str, name: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=name)


def load_config(path: str) -> dict:
    cities_df = _read_sheet(path, "Cities")
    req_df = _read_sheet(path, "Requests")
    cat_df = _read_sheet(path, "Categories")
    exc_df = _read_sheet(path, "Excludes")

    cities = []
    for _, r in cities_df.iterrows():
        manager = str(r.get("manager", "")).strip()
        region = str(r.get("region", "")).strip()
        city = str(r.get("city", "")).strip()
        if city:
            cities.append({"manager": manager, "region": region, "city": city})

    requests = []
    for _, r in req_df.iterrows():
        q = str(r.get("query_ru", "")).strip()
        if q:
            requests.append({"query_ru": q})

    categories = []
    for _, r in cat_df.iterrows():
        p = str(r.get("category_path", "")).strip()
        en = str(r.get("enabled", "1")).strip()
        enabled = en in ("1", "true", "True", "TRUE", "yes", "YES")
        if p:
            categories.append({"category_path": p, "enabled": enabled})

    excludes = []
    for _, r in exc_df.iterrows():
        t = str(r.get("text", "")).strip()
        if t:
            excludes.append(t)

    return {
        "cities": cities,
        "requests": requests,
        "categories": categories,
        "excludes": excludes,
    }


def build_tasks(cfg: dict, domain_pref: str = "auto", mapscan_always: bool = True) -> list[dict]:
    tasks: list[dict] = []

    cities = cfg.get("cities", []) or []
    reqs = cfg.get("requests", []) or []
    cats = cfg.get("categories", []) or []

    for c in cities:
        for r in reqs:
            base_mode = "LINKS_SEARCH"
            mode = "LINKS_MAPSCAN_SEARCH" if mapscan_always else base_mode
            q = (r.get("query_ru", "") or "").strip()
            if not q:
                continue
            tasks.append(
                {
                    "manager": c.get("manager", ""),
                    "region": c.get("region", ""),
                    "city": c.get("city", ""),
                    "mode": mode,
                    "query_key": "A",
                    "query_ru": q,
                    "category_path": "",
                    "domain_pref": domain_pref,
                }
            )

    for c in cities:
        for cat in cats:
            if not cat.get("enabled", True):
                continue
            p = (cat.get("category_path", "") or "").strip().strip("/")
            if not p:
                continue
            base_mode = "LINKS_CATEGORY"
            mode = "LINKS_MAPSCAN_CATEGORY" if mapscan_always else base_mode
            tasks.append(
                {
                    "manager": c.get("manager", ""),
                    "region": c.get("region", ""),
                    "city": c.get("city", ""),
                    "mode": mode,
                    "query_key": "B",
                    "query_ru": "",
                    "category_path": p,
                    "domain_pref": domain_pref,
                }
            )

    return tasks
