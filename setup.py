from logging import config
import logging
from contextlib import contextmanager
import time
import traceback
import warnings
from credential import SCRAPEOPS
import requests
from selenium import webdriver
from contextlib import contextmanager
from selenium.webdriver.chrome.options import Options
import os

@contextmanager
def get_driver():
    chrome_options = Options()
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (useful for headless mode)
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model (useful for Docker)
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

    driver = webdriver.Chrome(options=chrome_options)
    try:
        yield driver
    finally:
        driver.quit()


def retry(max_retry_count, interval_sec):
    def decorator(func):
        def wrapper(*args, **kwargs):
            retry_count = 0
            while retry_count < max_retry_count:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retry_count += 1
                    log.error(f'{func.__name__} failed on attempt {retry_count}: {str(e)}')
                    log.error(traceback.format_exc())  # Log the traceback
                    if retry_count < max_retry_count:
                        log.info(f'Retrying {func.__name__} in {interval_sec} seconds...')
                        time.sleep(interval_sec)
            log.warning(f'{func.__name__} reached maximum retry count of {max_retry_count}.')
            return None
        return wrapper
    return decorator

@retry(max_retry_count=2, interval_sec=2)
def proxied_request(url, render_js=False, without_proxy=False):
    if without_proxy:
        response = requests.get(url)
        if response.status_code in [200, 201]:
            return response
        else:
            raise Exception(f'Proxied request failed. {response.status_code}. {response.text}')
        
    PROXY_URL = 'https://proxy.scrapeops.io/v1/'
    response = requests.get(
        url=PROXY_URL,
        params={
            'api_key': SCRAPEOPS,
            'url': url, 
            # 'residential': 'true', 
            'country': 'us',
            'render_js': render_js
        },
    )
    # if response.status_code in [200, 201]:
    return response
    # else:
    #     raise Exception(f'Proxied request failed. {response.status_code}. {response.text}')

class DummyRequest:
    def __init__(self, text="", status_code=200):
        self.text = text
        self.status_code = status_code


def configure_get_log():
    warnings.filterwarnings("ignore")

    config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
                },
                "slack_format": {
                    "format": "`[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]` %(message)s"
                },
            },
            "handlers": {
                "file": {
                    "class": "logging.FileHandler",
                    "formatter": "default",
                    "filename": "logs.log",
                },
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                },
            },
            
            "loggers": {
                "root": {
                    "level": logging.INFO,
                    "handlers": ["file", "console"],
                    "propagate": False,
                },
            },
        }
    )
    log = logging.getLogger("root")
    return log


log = configure_get_log()