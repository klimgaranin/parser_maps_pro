import time
import random
import logging

from bs4 import BeautifulSoup

from core.utils import strip_brackets_keep_apostrophe, org_id_from_url
from providers.yandex.link_collector import is_captcha, _save_debug
from providers.yandex.soup_extract import (
    extract_name,
    extract_address,
    extract_website,
    extract_phone,
    extract_social,
)
from providers.yandex.exceptions import CaptchaError

log = logging.getLogger(__name__)


def parse_org_page(driver, url: str, debug_dir: str, save_debug: bool) -> dict:
    driver.get(url)
    time.sleep(random.uniform(1.0, 1.8))

    if is_captcha(driver):
        if save_debug:
            _save_debug(driver, debug_dir, f"CAPTCHA_INFO_{url}")
        raise CaptchaError("Капча на карточке организации")

    soup = BeautifulSoup(driver.page_source or "", "lxml")

    org_id = org_id_from_url(url)
    name = extract_name(soup)
    address = extract_address(soup)
    website = extract_website(soup)
    phone = extract_phone(soup)
    social = extract_social(soup)

    return {
        "org_id": org_id,
        "name": strip_brackets_keep_apostrophe(name),
        "address": strip_brackets_keep_apostrophe(address),
        "website": strip_brackets_keep_apostrophe(website),
        "ypage": (url or "").split("?")[0],
        "phone": strip_brackets_keep_apostrophe(phone),
        "social": strip_brackets_keep_apostrophe(social),
    }
