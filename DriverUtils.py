import pickle
import time
import os
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.common.proxy import Proxy, ProxyType
from seleniumwire import webdriver as xhr_webdriver
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from requests import Session
from fake_headers import Headers

# path = GeckoDriverManager(path='C:/geckodriver').install()
# os.replace(path, 'C:/geckodriver/geckodriver.exe')

def _get_tor_firefox_profile():
    profile = webdriver.FirefoxProfile()
    profile.set_preference("network.proxy.type", 1)
    profile.set_preference("network.proxy.socks", '127.0.0.1')
    profile.set_preference("network.proxy.socks_port", 9050)
    profile.set_preference("network.proxy.socks_remote_dns", True)
    profile.set_preference("security.ssl.enable_ocsp_stapling", False)
    profile.accept_untrusted_certs = True
    return profile

def _get_proxy_firefox_profile(proxy):
    ip, port = proxy.split(':')
    profile = webdriver.FirefoxProfile()
    profile.set_preference("network.proxy.type", 1)
    profile.set_preference("network.proxy.http", ip)
    profile.set_preference("network.proxy.http_port", int(port))
    profile.set_preference("network.proxy.share_proxy_settings", True)
    profile.set_preference("security.ssl.enable_ocsp_stapling", False)
    return profile

def start_selenium(timeout = 300, proxy=None, use_tor_proxy = False):
    if use_tor_proxy:
        firefox_profile = _get_tor_firefox_profile()

        driver = webdriver.Firefox(firefox_profile = firefox_profile)
    elif proxy:
        firefox_profile = _get_proxy_firefox_profile(proxy)
        driver = webdriver.Firefox(firefox_profile = firefox_profile)
    else:
        driver = webdriver.Firefox()
    driver.maximize_window()
    driver.set_page_load_timeout(timeout)
    return driver

def start_xhr_selenium(options = None, use_tor_proxy = False):
    driver = xhr_webdriver.Firefox(seleniumwire_options = options)
    driver.maximize_window()
    return driver

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

def save_html(response, path):
    with open(path, 'wb') as file:
        file.write(response.content)

if __name__ == '__main__':
    # from ParserUtils import change_tor_ip
    driver = start_selenium()
    # driver.get('https://whatismyipaddress.com/')