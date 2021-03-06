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


def start_selenium(
    driver_type='firefox', 
    seleniumwire_driver=False, 
    timeout = 60, 
    is_headless = False,
    **kwargs):
    if seleniumwire_driver:
        driver_module = xhr_webdriver
    else:
        driver_module = webdriver

    if driver_type=='firefox':
        driver_class = driver_module.Firefox
        driver_path = Path(__file__).parent / '_webdrivers' / 'gecko.exe'
        webdriver_manager = GeckoDriverManager()
        options = firefox_options()
    elif driver_type=='chrome':
        driver_class = driver_module.Chrome
        driver_path = Path(__file__).parent / '_webdrivers' / 'chrome.exe'
        webdriver_manager = ChromeDriverManager()
        options = chrome_options()
    else:
        raise NotImplementedError(f"Запуск драйвера {driver_type} не реализован")

    driver_path = driver_path.resolve()
    if not driver_path.exists():
        driver_path.parent.mkdir(exist_ok=True)
        cache_path = webdriver_manager.install()
        shutil.copy(cache_path, str(driver_path))

    options.headless = is_headless
    driver = driver_class(options=options, executable_path=str(driver_path))
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
            cookies = [{i['name']:i['value']} for i in cookies]
            print (cookies)
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

if __name__ == '__main__':
    driver = start_selenium(seleniumwire_driver=True)
    driver.scopes = ['.*catalog.api.2gis.ru/3.0/items/byid.*']
    driver.get('https://2gis.ru/moscow/geo/4504338361749652?m=37.671108%2C55.753157%2F17.03')