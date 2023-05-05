import pickle
import time
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as chrome_options
from selenium.webdriver.firefox.options import Options as firefox_options
from seleniumwire import webdriver as xhr_webdriver
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from requests import Session
from fake_headers import Headers
from pathlib import Path
import shutil
from datetime import datetime
from uuid import uuid4
import platform
from pyvirtualdisplay import Display
import re


def start_selenium(
    driver_type='firefox', 
    seleniumwire_driver=False, 
    timeout = 60, 
    is_headless = False,
    proxy=None,
    **kwargs):
    if platform.system() == 'Linux':
        display = Display(visible=0, size=(1280, 1024))
        display.start()

    if seleniumwire_driver:
        driver_module = xhr_webdriver
    else:
        driver_module = webdriver

    if driver_type=='firefox':
        driver_class = driver_module.Firefox
        driver_path = Path(__file__).parent / '_webdrivers' / 'gecko.exe'
        webdriver_manager = GeckoDriverManager()
        options = firefox_options()
        caps = webdriver.DesiredCapabilities.FIREFOX
    elif driver_type=='chrome':
        driver_class = driver_module.Chrome
        driver_path = Path(__file__).parent / '_webdrivers' / 'chrome.exe'
        webdriver_manager = ChromeDriverManager()
        options = chrome_options()
        caps = webdriver.DesiredCapabilities.CHROME
    else:
        raise NotImplementedError(f"Запуск драйвера {driver_type} не реализован")

    if proxy:
        caps['proxy'] = {
            "proxyType": "MANUAL",
            "httpProxy": proxy,
            "sslProxy": proxy
        }

    driver_path = driver_path.resolve()
    if not driver_path.exists():
        driver_path.parent.mkdir(exist_ok=True)
        cache_path = webdriver_manager.install()
        shutil.copy(cache_path, str(driver_path))

    options.headless = is_headless
    driver = driver_class(options=options, executable_path=str(driver_path), capabilities=caps)
    if not is_headless:
        driver.maximize_window()
    driver.set_page_load_timeout(timeout)
    return driver

def selenium_wait_element(driver, xpath, timeout=60, ec_type='element_to_be_clickable'):
    ec_types = [
    'title_is'
    'title_contains'
    'presence_of_element_located'
    'visibility_of_element_located'
    'visibility_of'
    'presence_of_all_elements_located'
    'text_to_be_present_in_element'
    'text_to_be_present_in_element_value'
    'frame_to_be_available_and_switch_to_it'
    'invisibility_of_element_located'
    'element_to_be_clickable'
    'staleness_of'
    'element_to_be_selected'
    'element_located_to_be_selected'
    'element_selection_state_to_be'
    'element_located_selection_state_to_be'
    'alert_is_present'
    ]
    wait_func = getattr(EC, ec_type)
    wait = WebDriverWait(driver, timeout) 
    element = wait.until(wait_func((By.XPATH, xpath)))
    return element

def start_session():
    ses = Session()
    ses.headers = Headers().generate()
    return ses

def session_with_cookies(domain, sleep_time=0):
    ses = start_session()
    dr = start_selenium()
    dr.get(domain)
    time.sleep(sleep_time)
    transfer_cookie(dr, ses, domain)
    dr.quit()
    return ses

def transfer_cookie(fr, to, domain):
    save_cookie(fr, 'temp.cookie')
    set_cookie(to, domain, 'temp.cookie')

def save_cookie(cookie_container, cookie_path):
    '''Cохраняются куки сессии selenium driver'''
    with open(cookie_path, 'wb') as f:
        if isinstance(cookie_container, Session):
            pickle.dump(cookie_container.cookies, f)
        else:
            pickle.dump(cookie_container.get_cookies() , f)

def set_cookie(cookie_container, domain, cookie_path):
    '''Назначаем экземпляру selenium.Webdriver куки.
    Ускоряет процесс авторизации и сокращает шанс бана аккаунта'''
    cookie_container.get(domain)
    with open(cookie_path, 'rb') as file:
        cookies = pickle.load(file)   
        if isinstance(cookie_container, Session):
            cookies = {i['name']:i['value'] for i in cookies}
            cookie_container.cookies.update(cookies)
        else:
            for cookie in cookies: 
                cookie_container.add_cookie(cookie)

def scroll_page(driver, infinite_scroll=True, xpaths=[], sleep_time=1):
    '''Пролистывание страницы до тех пор, пока не встретится один из обозначенных элементов

    Parameters:
    xpaths (list<string>) - xpath-обозначение элементов для остановки скроллинга
    '''
    last_height = driver.execute_script("return document.body.scrollHeight")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(sleep_time)
    new_height = driver.execute_script("return document.body.scrollHeight")
    if not infinite_scroll:
    	return last_height==new_height
    else:
	    stop_elements = []
	    for xpath in xpaths:
	        elements = driver.find_elements_by_xpath(xpath)
	        stop_elements += elements
	    if len(stop_elements) != 0:
	    	return True
	    elif last_height==new_height:
	    	return True
	    else:
		    return scroll_page(driver, infinite_scroll, xpaths, sleep_time)

def save_html(response, path='./debug.html'):
    with open(path, 'wb') as file:
        file.write(response.content)

def save_responses(driver):
    folder = Path(__file__).parent.resolve() / '_selenium_responses' / datetime.now().strftime('%Y-%m-%d %H_%M_%S')
    folder.mkdir(parents=True)
    for r in driver.requests:
        with open(folder / str(uuid4()), 'wb') as file:
            file.write(r.response.body)

def __clear_element_text(x):
    '''убирает мусор из текста элементов при парсинге (\xa0 и прочее)'''
    if x:
        x = x.replace('\xa0', ' ') \
            .replace('&thinsp;', ' ') \
            .replace('&nbsp;', ' ') \
            .replace('&mdash;', '-') \
            .replace('&#8470;', '№') \
            .replace('&lt;', '<') \
            .replace('&gt;', '> ') \
            .strip(' \r\n\t:,.')
        x = re.sub('<.+?>', '', x)
        x = re.sub(r'\s{2,}', ' ', x)
        x = x.strip()
        return x


def find_elements_text(dom, xpath='.', text_content=False):
    '''Возвращает тексты элементов, найденного по xpath'''
    elements = dom.xpath(xpath)
    if text_content:
        elements = [__clear_element_text(i.text_content()) for i in elements]
    else:
        elements = [__clear_element_text(i.text) for i in elements]
    elements = list(filter(bool, elements))
    return elements


def find_element_text(dom, xpath='.', text_content=False):
    '''Безопасно возвращает текст первого элемента, найденного по xpath'''
    for i in dom.xpath(xpath):
        if text_content:
            return __clear_element_text(i.text_content())
        else:
            return __clear_element_text(i.text)