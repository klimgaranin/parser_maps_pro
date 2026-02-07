import os
import time
import threading
import traceback
from datetime import datetime
from pathlib import Path

from selenium.common.exceptions import TimeoutException

from providers.yandex.driver import make_driver
from providers.yandex.exceptions import CaptchaError


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_dir(p: str) -> str:
    return "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in (p or ""))


def _quit_driver_safely(driver, timeout: float = 4.0) -> None:
    """
    driver.quit() иногда может зависнуть (особенно если Chrome/профиль “залип”).
    Чтобы stop не вешал сервер — делаем quit в отдельном потоке и даём ему таймаут.
    """
    if driver is None:
        return

    exc = {"e": None}

    def _do_quit():
        try:
            driver.quit()
        except Exception as e:
            exc["e"] = e

    t = threading.Thread(target=_do_quit, daemon=True)
    t.start()
    t.join(timeout=timeout)

    # если завис — пытаемся прибить chromedriver процессом
    if t.is_alive():
        try:
            svc = getattr(driver, "service", None)
            proc = getattr(svc, "process", None)
            pid = getattr(proc, "pid", None)
            if pid:
                try:
                    import subprocess

                    subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, text=True)
                except Exception:
                    pass
        except Exception:
            pass


class Pipeline:
    """
    Два воркера:
    - links_worker: берёт задачи LINKS_* из БД, собирает ссылки организаций и пишет их в таблицу links
    - info_worker: берёт ссылки из таблицы links, открывает карточки и пишет orgs + org_sources
    """

    def __init__(self, db, headless: bool, runtime_dir: str, logger, links_fn, info_fn):
        self.db = db
        self.headless = headless
        self.runtime_dir = Path(runtime_dir)
        self.log = logger
        self.links_fn = links_fn
        self.info_fn = info_fn

        self._stop = threading.Event()

        self._links_thread = None
        self._info_thread = None

        self._links_driver = None
        self._info_driver = None

        self._stop_lock = threading.Lock()
        self._stopping = False

        (self.runtime_dir / "debug").mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "profiles").mkdir(parents=True, exist_ok=True)

    def is_alive(self) -> bool:
        t1 = self._links_thread.is_alive() if self._links_thread else False
        t2 = self._info_thread.is_alive() if self._info_thread else False
        return t1 or t2

    def start(self):
        if self.is_alive():
            return

        self._stop.clear()

        self._links_thread = threading.Thread(target=self._links_worker, daemon=False)
        self._info_thread = threading.Thread(target=self._info_worker, daemon=False)

        self._links_thread.start()
        self._info_thread.start()

        self.log.info(f"{_now()} | PIPELINE | Старт")

    def stop(self, wait: bool = True):
        self._stop.set()
        self.log.info(f"{_now()} | PIPELINE | Стоп (сигнал)")

        if not wait:
            return

        if self._links_thread:
            self._links_thread.join(timeout=12)
        if self._info_thread:
            self._info_thread.join(timeout=12)

        self._close_drivers()
        self.log.info(f"{_now()} | PIPELINE | Стоп (завершено)")

    def stop_async(self):
        """
        Вызывается из API /api/stop.
        Важно: не блокируем HTTP-обработчик — иначе “залипает” консоль.
        """
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        self._stop.set()
        self.log.info(f"{_now()} | PIPELINE | Стоп (async)")

        def _bg():
            try:
                if self._links_thread:
                    self._links_thread.join(timeout=12)
                if self._info_thread:
                    self._info_thread.join(timeout=12)
                self._close_drivers()
                self.log.info(f"{_now()} | PIPELINE | Стоп (async завершено)")
            finally:
                with self._stop_lock:
                    self._stopping = False

        threading.Thread(target=_bg, daemon=True).start()

    def _close_drivers(self):
        for d in (self._links_driver, self._info_driver):
            try:
                _quit_driver_safely(d, timeout=4.0)
            except Exception:
                pass

        self._links_driver = None
        self._info_driver = None

    def _dump_debug(self, prefix: str, driver, extra: str = ""):
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base = self.runtime_dir / "debug" / f"{_safe_dir(prefix)}_{ts}"
            html_path = str(base) + ".html"
            png_path = str(base) + ".png"

            try:
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(driver.page_source or "")
            except Exception:
                pass

            try:
                driver.save_screenshot(png_path)
            except Exception:
                pass

            if extra:
                try:
                    with open(str(base) + ".log.txt", "w", encoding="utf-8") as f:
                        f.write(extra)
                except Exception:
                    pass
        except Exception:
            pass

    def _links_worker(self):
        max_attempts = int(os.getenv("TASK_MAX_ATTEMPTS", "30"))
        profile = str(self.runtime_dir / "profiles" / "links")

        while not self._stop.is_set():
            task = self.db.pick_next_task_for_links(max_attempts=max_attempts)
            if not task:
                time.sleep(1.0)
                continue

            task_id = task["id"]
            self.log.info(f"{_now()} | LINKS | Взял задачу id={task_id} city={task.get('city')} mode={task.get('mode')}")

            try:
                if self._links_driver is None:
                    Path(profile).mkdir(parents=True, exist_ok=True)
                    self._links_driver = make_driver(profile_dir=profile, headless=self.headless)

                inserted = self.links_fn(self._links_driver, task)
                inserted = int(inserted or 0)
                self.db.set_task_links_done(task_id, inserted=inserted)
                self.log.info(f"{_now()} | LINKS | Готово id={task_id} inserted_links={inserted}")

            except CaptchaError as e:
                err = f"{e}"
                self.log.warning(f"{_now()} | LINKS | WAITCAPTCHA id={task_id} err={e}")
                if self._links_driver is not None:
                    self._dump_debug(f"links_task{task_id}_captcha", self._links_driver, extra=err)
                self.db.set_task_waitcaptcha(task_id, err, worker="links")
                time.sleep(8.0)

            except TimeoutException as e:
                err = f"{e}\n{traceback.format_exc()}"
                self.log.warning(f"{_now()} | LINKS | Timeout id={task_id}")
                if self._links_driver is not None:
                    self._dump_debug(f"links_task{task_id}_timeout", self._links_driver, extra=err)
                self.db.set_task_error(task_id, err, worker="links")
                time.sleep(2.0)

            except Exception as e:
                err = f"{e}\n{traceback.format_exc()}"
                self.log.warning(f"{_now()} | LINKS | Ошибка id={task_id} err={e}")
                if self._links_driver is not None:
                    self._dump_debug(f"links_task{task_id}", self._links_driver, extra=err)
                self.db.set_task_error(task_id, err, worker="links")

                try:
                    if self._links_driver is not None:
                        _quit_driver_safely(self._links_driver, timeout=3.0)
                except Exception:
                    pass
                self._links_driver = None
                time.sleep(2.0)

        self._close_drivers()

    def _info_worker(self):
        profile = str(self.runtime_dir / "profiles" / "info")

        while not self._stop.is_set():
            link_row = self.db.fetch_next_link()
            if not link_row:
                time.sleep(1.0)
                continue

            org_id = link_row.get("org_id")
            url = link_row.get("url")
            self.log.info(f"{_now()} | INFO | Взял ссылку org_id={org_id} url={url}")

            try:
                if self._info_driver is None:
                    Path(profile).mkdir(parents=True, exist_ok=True)
                    self._info_driver = make_driver(profile_dir=profile, headless=self.headless)

                org = self.info_fn(self._info_driver, link_row)
                if org and org.get("org_id"):
                    self.db.upsert_org(org)
                    self.db.add_source(org.get("org_id"), link_row, mode=link_row.get("source_mode", "unknown"))
                    self.log.info(f"{_now()} | INFO | Сохранено org_id={org.get('org_id')}")

            except CaptchaError as e:
                err = f"{e}"
                self.log.warning(f"{_now()} | INFO | WAITCAPTCHA org_id={org_id} err={e}")
                if self._info_driver is not None:
                    self._dump_debug(f"info_org{org_id}_captcha", self._info_driver, extra=err)
                time.sleep(8.0)

            except Exception as e:
                err = f"{e}\n{traceback.format_exc()}"
                self.log.warning(f"{_now()} | INFO | Ошибка org_id={org_id} err={e}")
                if self._info_driver is not None:
                    self._dump_debug(f"info_org{org_id}", self._info_driver, extra=err)

                try:
                    if self._info_driver is not None:
                        _quit_driver_safely(self._info_driver, timeout=3.0)
                except Exception:
                    pass
                self._info_driver = None
                time.sleep(2.0)

        self._close_drivers()
