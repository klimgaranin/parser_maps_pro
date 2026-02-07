import os
import time
import random
import logging
from urllib.parse import quote, urljoin, urlparse, parse_qs, urlencode

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

from core.utils import safe_filename
from providers.yandex.exceptions import CaptchaError, PageStructureError

log = logging.getLogger(__name__)


def _save_debug(driver, debug_dir: str, tag: str) -> None:
    os.makedirs(debug_dir, exist_ok=True)
    base = os.path.join(debug_dir, safe_filename(tag))
    html_path = base + ".html"
    png_path = base + ".png"
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
    except Exception:
        pass
    try:
        driver.save_screenshot(png_path)
    except Exception:
        pass


def is_captcha(driver) -> bool:
    url = (driver.current_url or "").lower()
    title = (driver.title or "").lower()
    if "showcaptcha" in url:
        return True
    if "captcha.yandex" in url:
        return True
    if "captcha" in title or "smartcaptcha" in title or "капча" in title:
        return True
    return False


def _wait_ready(driver, timeout: int) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )


def _find_search_input(driver, timeout: int):
    css_candidates = [
        "input.input__control",
        "input.search-form-view__input",
        "form.search-form-view input[type='text']",
        "input[type='search']",
        "input[placeholder*='Поиск']",
        "input[aria-label*='Поиск']",
    ]
    end = time.time() + timeout
    while time.time() < end:
        if is_captcha(driver):
            raise CaptchaError("Капча на странице (поиск города).")
        for css in css_candidates:
            try:
                el = driver.find_element(By.CSS_SELECTOR, css)
                if el and el.is_displayed():
                    return el
            except Exception:
                pass
        time.sleep(0.25)
    raise PageStructureError(f"Не найдено поле поиска за {timeout}с. url={driver.current_url} title={driver.title}")


def _swap_domain(url: str, domain: str) -> str:
    if not url:
        return url
    url = url.replace("yandex.ru", f"yandex.{domain}")
    url = url.replace("yandex.by", f"yandex.{domain}")
    url = url.replace("yandex.kz", f"yandex.{domain}")
    url = url.replace("yandex.com", f"yandex.{domain}")
    return url


def _extract_ll_z(url: str) -> tuple[str | None, str | None]:
    try:
        qs = parse_qs(urlparse(url).query or "")
        ll = (qs.get("ll") or [None])[0]
        z = (qs.get("z") or [None])[0]
        if ll:
            ll = ll.strip()
        if z:
            z = z.strip()
        return ll, z
    except Exception:
        return None, None


def open_city_base(driver, city: str, domain: str, debug_dir: str, save_debug: bool) -> str:
    wait_input = int(os.getenv("Y_WAIT_CITY_INPUT", "45"))
    city = (city or "").strip()
    if not city:
        raise PageStructureError("Пустое имя города (task.city).")

    driver.get(f"https://yandex.{domain}/maps")
    _wait_ready(driver, timeout=wait_input)

    if is_captcha(driver):
        if save_debug:
            _save_debug(driver, debug_dir, f"CAPTCHA_MAPS_OPEN_{city}")
        raise CaptchaError("Капча при открытии /maps")

    try:
        inp = _find_search_input(driver, timeout=wait_input)
        try:
            inp.click()
        except Exception:
            pass
        inp.send_keys(Keys.CONTROL, "a")
        inp.send_keys(Keys.BACKSPACE)
        inp.send_keys(city)
        inp.send_keys(Keys.ENTER)
    except CaptchaError:
        raise
    except Exception as e:
        if save_debug:
            _save_debug(driver, debug_dir, f"CITY_OPEN_FAIL_{city}")
        raise PageStructureError(f"Не удалось выполнить поиск города '{city}': {e}")

    time.sleep(random.uniform(2.2, 3.4))

    if is_captcha(driver):
        if save_debug:
            _save_debug(driver, debug_dir, f"CAPTCHA_CITY_{city}")
        raise CaptchaError("Капча после ввода города")

    cur = _swap_domain(driver.current_url or "", domain=domain)
    ll, z = _extract_ll_z(cur)

    if not ll:
        if save_debug:
            _save_debug(driver, debug_dir, f"CITY_LL_NOT_FOUND_{city}")
        raise PageStructureError(f"Не удалось извлечь ll/z из URL города. url={cur}")

    if not z:
        z = (os.getenv("Y_CITY_Z_DEFAULT", "11") or "11").strip()
    
    # Извлекаем city_id из текущего URL
    city_id = None
    try:
        parts = urlparse(cur).path.split("/")
        if "maps" in parts:
            idx = parts.index("maps")
            if idx + 1 < len(parts) and parts[idx + 1].isdigit():
                city_id = parts[idx + 1]
    except Exception:
        pass
    
    # Возвращаем URL с city_id если он найден
    if city_id:
        return f"https://yandex.{domain}/maps/{city_id}/?" + urlencode({"ll": ll, "z": z}, doseq=False)
    return f"https://yandex.{domain}/maps/?" + urlencode({"ll": ll, "z": z}, doseq=False)


def _get_ll_z_from_city_base(city_base: str) -> tuple[str, str, str]:
    parsed = urlparse(city_base or "")
    if not parsed.netloc:
        raise PageStructureError(f"Некорректный city_base (нет netloc): {city_base}")
    qs = parse_qs(parsed.query or "")
    ll = (qs.get("ll") or [""])[0].strip()
    z = (qs.get("z") or [""])[0].strip()
    if not ll:
        raise PageStructureError(f"В city_base нет ll: {city_base}")
    if not z:
        z = (os.getenv("Y_CITY_Z_DEFAULT", "11") or "11").strip()
    domain = parsed.netloc.split("yandex.")[-1]
    city_id = None
    try:
        parts = parsed.path.split("/")
        if "maps" in parts:
            idx = parts.index("maps")
            if idx + 1 < len(parts) and parts[idx + 1].isdigit():
                city_id = parts[idx + 1]
    except Exception:
        pass
    return domain, ll, z, city_id


def build_search_url(city_base: str, query_ru: str) -> str:
    domain, ll, z, city_id = _get_ll_z_from_city_base(city_base)
    text = (query_ru or "").strip()
    q = quote(text, safe="")
    if city_id:
        return f"https://yandex.{domain}/maps/{city_id}/?" + urlencode({"text": q, "ll": ll, "z": z}, doseq=False)
    return f"https://yandex.{domain}/maps/?" + urlencode({"text": q, "ll": ll, "z": z}, doseq=False)


def build_category_url(city_base: str, category_path: str) -> str:
    domain, ll, z, city_id = _get_ll_z_from_city_base(city_base)
    cat = (category_path or "").strip().strip("/")
    if not cat:
        raise PageStructureError("Пустой category_path.")
    if city_id:
        return f"https://yandex.{domain}/maps/{city_id}/category/{cat}/?" + urlencode({"ll": ll, "z": z}, doseq=False)
    return f"https://yandex.{domain}/maps/category/{cat}/?" + urlencode({"ll": ll, "z": z}, doseq=False)


def _find_map_container(driver):
    css_candidates = [
        "div.map-container",
        ".map-container",
        ".ymaps3x0--main-engine-container",
        ".ymaps3x0--top-engine-container",
    ]
    for css in css_candidates:
        try:
            el = driver.find_element(By.CSS_SELECTOR, css)
            if el and el.is_displayed():
                return el
        except Exception:
            pass
    return None


def map_scan(driver, grid: int, step_px: int) -> None:
    if grid <= 1:
        return

    el = _find_map_container(driver)
    if el is None:
        return

    size = el.size or {}
    w = int(size.get("width") or 0)
    h = int(size.get("height") or 0)
    if w < 200 or h < 200:
        return

    cx = int(w * 0.5)
    cy = int(h * 0.5)

    try:
        ActionChains(driver).move_to_element_with_offset(el, cx, cy).click().perform()
    except Exception:
        pass

    time.sleep(0.5)

    for row in range(grid):
        direction = 1 if row % 2 == 0 else -1
        for _ in range(grid - 1):
            dx = -direction * step_px
            try:
                ActionChains(driver).move_to_element_with_offset(el, cx, cy).click_and_hold().pause(0.05).move_by_offset(dx, 0).pause(0.05).release().perform()
            except Exception:
                pass
            time.sleep(random.uniform(0.8, 1.2))

        if row < grid - 1:
            try:
                ActionChains(driver).move_to_element_with_offset(el, cx, cy).click_and_hold().pause(0.05).move_by_offset(0, -step_px).pause(0.05).release().perform()
            except Exception:
                pass
            time.sleep(random.uniform(0.8, 1.2))


def _get_results_scroll_container(driver):
    css_candidates = [
        "aside.sidebar-view .scroll__container",
        "div.sidebar-container .scroll__container",
        "div.scroll__container",
    ]
    for css in css_candidates:
        try:
            el = driver.find_element(By.CSS_SELECTOR, css)
            if el and el.is_displayed():
                return el
        except Exception:
            pass
    return None


def _normalize_org_url(current_page_url: str, href: str) -> str | None:
    if not href:
        return None
    base = "{uri.scheme}://{uri.netloc}/".format(uri=urlparse(current_page_url))
    abs_url = urljoin(base, href)
    if "/maps/org/" not in abs_url:
        return None
    return abs_url.split("?", 1)[0]


def collect_links_from_list(driver, debug_dir: str, save_debug: bool, max_idle: int = 12) -> list[str]:
    links: set[str] = set()
    idle = 0
    last_cnt = 0
    container = _get_results_scroll_container(driver)

    for _ in range(4000):
        if is_captcha(driver):
            if save_debug:
                _save_debug(driver, debug_dir, f"CAPTCHA_LIST_{time.time()}")
            raise CaptchaError("Капча при сборе ссылок")

        items = driver.find_elements(By.CSS_SELECTOR, "a.link-overlay")
        for it in items:
            try:
                href = it.get_attribute("href")
                norm = _normalize_org_url(driver.current_url or "", href)
                if norm:
                    links.add(norm)
            except Exception:
                pass

        if len(links) == last_cnt:
            idle += 1
        else:
            idle = 0
            last_cnt = len(links)

        if idle >= max_idle:
            break

        try:
            if container is not None:
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[1];", container, 900)
            else:
                driver.execute_script("window.scrollBy(0, 900);")
        except Exception:
            pass

        time.sleep(random.uniform(0.12, 0.25))

    return list(links)


def collect_task_links(driver, task: dict, excludes: list, debug_dir: str, save_debug: bool, mapscan: bool):
    domain = (task.get("domain_pref") or os.getenv("Y_DOMAIN_DEFAULT", "by") or "by").strip().lower()
    if domain == "auto":
        domain = (os.getenv("Y_DOMAIN_DEFAULT", "by") or "by").strip().lower()

    city_base = open_city_base(driver, task.get("city") or "", domain=domain, debug_dir=debug_dir, save_debug=save_debug)

    mode = task.get("mode") or ""
    if "CATEGORY" in mode:
        url = build_category_url(city_base, task.get("category_path") or "")
        tag = f"{task.get('city','')}_CAT_{task.get('category_path','')}"
    else:
        url = build_search_url(city_base, task.get("query_ru") or "")
        tag = f"{task.get('city','')}_Q_{task.get('query_ru','')}"

    driver.get(url)
    time.sleep(random.uniform(2.2, 3.4))

    if "404" in (driver.title or "").lower():
        if save_debug:
            _save_debug(driver, debug_dir, f"HTTP404_{tag}")
        raise PageStructureError(f"Получили 404 по URL: {url}")

    if is_captcha(driver):
        if save_debug:
            _save_debug(driver, debug_dir, f"CAPTCHA_OPEN_{tag}")
        raise CaptchaError("Капча после открытия выдачи")

    if mapscan:
        g = int(os.getenv("MAPSCAN_GRID", "3"))
        step = int(os.getenv("MAPSCAN_STEP_PX", "420"))
        try:
            map_scan(driver, g, step)
        except Exception:
            pass

    hrefs = collect_links_from_list(driver, debug_dir, save_debug)
    filtered: list[str] = []

    for h in hrefs:
        h_low = h.lower()
        bad = False
        for ex in excludes or []:
            exs = (ex or "").lower()
            if exs and exs in h_low:
                bad = True
                break
        if not bad:
            filtered.append(h)

    if save_debug:
        _save_debug(driver, debug_dir, f"AFTER_{tag}_COUNT_{len(filtered)}")

    return filtered


