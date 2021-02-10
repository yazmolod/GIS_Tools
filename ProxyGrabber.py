from fake_headers import Headers
import cloudscraper
from bs4 import BeautifulSoup
import re
import json
import os
import requests
from concurrent.futures import ThreadPoolExecutor, wait
from time import time
import logging
import logging.config

logger = logging.getLogger(__name__)

class ProxyGrabber:
    CACHE_PATH = os.path.dirname(os.path.abspath(__file__)) + r'\proxies.json'
    PROXIES = None

    def __init__(self):
        self.PROXIES_INDEX = -1

    @staticmethod
    def _download(speed=5000, page_count=100, types = 's45'):
        '''Скачивает прокси с https://hidemy.name/ и записывает их в кэш'''
        logger.info('Downloading proxies...')
        result = []
        params = {'maxtime':speed,   # скорость прокси
                  'type': types,     #h - http, s - https, 4 - socks4, 5 - socks5
                  'anon': '1234',     #низкая, средняя и высокая
                  'start': 0,        # смещение запросов
                 }
        url = 'https://hidemy.name/ru/proxy-list/?'
        headers = Headers().generate()   
        scraper = cloudscraper.create_scraper()     # для прохождения защиты от cloudflare
        for i in range(page_count):
            params['start'] = i*64
            try:
                response = scraper.get(url, params=params, headers=headers)
            except Exception as e:
                logger.exception('Failed on download proxies')
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find('div', attrs={'class':'table_block'})
            tr = table.find('tr')                   #header
            tr = tr.find_next('tr')
            if tr == None:
                break
            while tr != None:
                tds = tr.find_all('td')
                d = {'ip': tds[0].text,
                    'port': tds[1].text,          
                    'country': tds[2].find('span', attrs={'class': 'country'}).text,
                    'city': tds[2].find('span', attrs={'class': 'city'}).text,
                    'speed': int(tds[3].find('p').text.strip(' мс')),
                    'type': tds[4].text,
                    'anon': tds[5].text,
                    'updated': tds[6].text,                 
                    }        
                d['proxy'] = re.findall(r'HTTPS|HTTP|SOCKS5|SOCKS4', d['type'])[0].lower()+'://'+d['ip']+':'+d['port']    
                result.append(d)
                tr = tr.find_next('tr')
        proxies = [i['proxy'] for i in result]
        logger.info('Downloaded: %d' % len(proxies))
        ProxyGrabber._writer_cache(proxies)
        return proxies

    @staticmethod
    def _writer_cache(cache):
        with open(ProxyGrabber.CACHE_PATH, 'w', encoding='utf-8') as file:
            json.dump(cache, file, ensure_ascii=False, indent=2)

    @staticmethod
    def _read_cache():
        '''Чтение кэша с последнего парсинга'''
        logger.info('Reading cache')
        if os.path.exists(ProxyGrabber.CACHE_PATH):
            with open(ProxyGrabber.CACHE_PATH, encoding='utf-8') as file:
                proxies = json.load(file)
            return proxies
        else:
            raise FileNotFoundError("No proxy cache found, try to download")

    @staticmethod
    def _is_proxy_ok(proxy, test_url='https://www.example.com/', timeout=10):
        '''Проверка прокси на работоспособность'''
        try:
            r = requests.get(test_url, proxies={'https': proxy, 'http': proxy}, timeout=timeout)
            return r.status_code==200
        except:
            return False

    @staticmethod
    def _filter_bad_proxies(proxies, workers=100, test_url='https://www.example.com/', timeout=10):
        '''Многопоточная фильтрация нерабочих прокси'''
        logger.info('Testing proxies...')
        good_proxies = []
        if len(proxies) > 0:        
            if isinstance(proxies[0], dict):
                proxies = [proxy['proxy'] for proxy in proxies]            
        with ThreadPoolExecutor(workers) as executor:
            futures = {executor.submit(ProxyGrabber._is_proxy_ok, i, test_url, timeout):i for i in proxies}
            wait(futures)
            for future in futures:
                if future.result():
                    good_proxies.append(futures[future])
        ProxyGrabber._writer_cache(good_proxies)
        logger.info(f'{len(good_proxies)} is good')
        return good_proxies

    @staticmethod
    def get_proxies_list(minutes_from_last_update=300):
    	if os.path.exists(ProxyGrabber.CACHE_PATH):
    		file_timestamp = os.path.getmtime(ProxyGrabber.CACHE_PATH)
    		now_timestamp = time()
    		delta = now_timestamp-file_timestamp
    		if delta/60 <= minutes_from_last_update:
    			return ProxyGrabber._read_cache()
    		else:
    			proxies = ProxyGrabber._download()
    			proxies = ProxyGrabber._filter_bad_proxies(proxies)
    			return proxies
    	else:
    		proxies = ProxyGrabber._download()
    		proxies = ProxyGrabber._filter_bad_proxies(proxies)
    		return proxies

    def get_proxy(self):
        if self.PROXIES_INDEX == -1:
            return {'http': None, 'https': None}
        else:
            if not ProxyGrabber.PROXIES:
                ProxyGrabber.PROXIES = ProxyGrabber.get_proxies_list()
            proxy = ProxyGrabber.PROXIES[self.PROXIES_INDEX]
            return {'http': proxy, 'https': proxy}

    def next_proxy(self):
        self.PROXIES_INDEX += 1  
        if ProxyGrabber.PROXIES: 
            if self.PROXIES_INDEX >= len(ProxyGrabber.PROXIES):
                self.reset()        
                ProxyGrabber.PROXIES = ProxyGrabber.get_proxies_list()
            logger.info(f'Changed proxy, {self.PROXIES_INDEX + 1} out {len(ProxyGrabber.PROXIES)}')
        return self.get_proxy()

    def reset(self):
        self.PROXIES_INDEX = -1
        logger.info('Reset proxies list')


if __name__ == '__main__':
    gr = ProxyGrabber()
    gr.next_proxy()
    

    
    