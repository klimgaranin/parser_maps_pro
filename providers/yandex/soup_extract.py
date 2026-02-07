import re
from bs4 import BeautifulSoup

from core.utils import norm_text


def _text(s):
    return norm_text(s.get_text(" ", strip=True)) if s else ""


def extract_name(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        t = _text(h1)
        if t:
            return t

    title = soup.find("title")
    t = _text(title)
    if t:
        return t.split("—")[0].strip()

    return ""


def extract_address(soup: BeautifulSoup) -> str:
    m = soup.find(attrs={"class": re.compile(r"business-card-view__address|orgpage-header-view__address", re.I)})
    if m:
        return _text(m)

    for a in soup.find_all("a", href=True):
        if "ymaps" in a["href"] and "geo" in a["href"]:
            t = _text(a)
            if t and len(t) > 5:
                return t

    return ""


def extract_website(soup: BeautifulSoup) -> str:
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "http" in href and ("yandex" not in href):
            t = _text(a)
            if t.lower() in ("сайт", "website", "web"):
                return href
    return ""


def extract_phone(soup: BeautifulSoup) -> str:
    txt = soup.get_text("\n", strip=True)
    phones = re.findall(r"(\+?\d[\d\-\s\(\)]{8,}\d)", txt)
    if phones:
        return norm_text(phones[0])
    return ""


def extract_social(soup: BeautifulSoup) -> str:
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if any(x in href for x in ["vk.com", "t.me", "instagram.com", "facebook.com", "ok.ru", "youtube.com"]):
            links.append(a["href"])
    return ", ".join(sorted(set(links)))[:1000]
