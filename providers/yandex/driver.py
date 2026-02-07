import os
import tempfile
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException


def _find_chromedriver() -> str:
    env = os.environ.get("CHROMEDRIVER_PATH")
    if env and Path(env).is_file():
        return str(Path(env).resolve())

    here = Path(__file__).resolve()
    root = here.parents[2]

    for name in ("chromedriver.exe", "chromedriver"):
        p = root / name
        if p.is_file():
            return str(p.resolve())

    raise FileNotFoundError("chromedriver не найден. Положи chromedriver.exe в корень проекта или укажи CHROMEDRIVER_PATH.")


def _cleanup_profile_locks(profile_dir: str) -> None:
    """
    Частая причина DevToolsActivePort: залипшие lock-файлы в user-data-dir после падения Chrome.
    """
    p = Path(profile_dir)
    if not p.exists():
        return

    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket", "DevToolsActivePort"):
        try:
            f = p / name
            if f.exists():
                f.unlink()
        except Exception:
            pass


def _build_options(profile_dir: str, headless: bool) -> webdriver.ChromeOptions:
    opts = webdriver.ChromeOptions()

    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--lang=ru-RU")
    opts.add_argument("--start-maximized")

    opts.add_argument("--remote-debugging-port=0")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(f"--user-data-dir={profile_dir}")

    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")

    prefs = {
        "profile.default_content_setting_values.geolocation": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.media_stream_mic": 2,
        "profile.default_content_setting_values.media_stream_camera": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    return opts


def make_driver(profile_dir: str | None = None, headless: bool = False) -> webdriver.Chrome:
    driver_path = _find_chromedriver()

    if not profile_dir:
        profile_dir = tempfile.mkdtemp(prefix="yandex_profile_")

    Path(profile_dir).mkdir(parents=True, exist_ok=True)

    _cleanup_profile_locks(profile_dir)

    service = Service(executable_path=driver_path)

    opts = _build_options(profile_dir=profile_dir, headless=headless)
    try:
        return webdriver.Chrome(service=service, options=opts)
    except SessionNotCreatedException as e:
        msg = str(e)
        if "DevToolsActivePort" in msg or "session not created" in msg:
            _cleanup_profile_locks(profile_dir)
            try:
                return webdriver.Chrome(service=service, options=opts)
            except Exception:
                pass
        raise
    except WebDriverException:
        tmp = tempfile.mkdtemp(prefix="yandex_profile_fallback_")
        _cleanup_profile_locks(tmp)
        opts2 = _build_options(profile_dir=tmp, headless=headless)
        return webdriver.Chrome(service=service, options=opts2)
