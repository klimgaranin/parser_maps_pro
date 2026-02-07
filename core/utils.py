import re


def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def strip_brackets_keep_apostrophe(s: str) -> str:
    if not s:
        return ""
    s = s.replace("[", "").replace("]", "")
    s = s.replace("“", "'").replace("”", "'").replace('"', "'")
    return norm_text(s)


def org_id_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"/org/[^/]+/(\d+)/", url)
    if m:
        return m.group(1)
    m = re.search(r"/org/(\d+)/", url)
    if m:
        return m.group(1)
    m = re.search(r"/(\d+)/?$", url)
    return m.group(1) if m else ""


def safe_filename(s: str, max_len: int = 120) -> str:
    s = re.sub(r"[^a-zA-Z0-9а-яА-ЯёЁ _\.-]+", "", (s or "")).strip()
    s = re.sub(r"\s+", "_", s)
    return s[:max_len] or "file"
