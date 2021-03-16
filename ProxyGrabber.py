from fake_headers import Headers
import cloudscraper
from lxml import html
import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor, wait
from time import time
import logging
import logging.config
logger = logging.getLogger(__name__)
from pathlib import Path
from threading import get_ident
from threading import Lock


class ProxyGrabber:
    CACHE_PATH = Path(__file__).parent / 'proxies.json'
    PROXIES = None

    def __init__(self, **kwargs):
        self.PROXIES_INDEX = -1
        self.proxy_attrs = kwargs

    @staticmethod
    def _download(speed=5000, types='s45', countries=None):
        '''Скачивает прокси с https://hidemy.name/ и записывает их в кэш'''
        logger.info('Downloading proxies...')
        result = []
        params = {'maxtime':speed,   # скорость прокси
                  'type': types,     #h - http, s - https, 4 - socks4, 5 - socks5
                  'anon': '1234',     #низкая, средняя и высокая
                  'start': 0,        # смещение запросов
                 }
        url = 'https://hidemy.name/ru/proxy-list/?'
        if countries:
            # делаю так, а не через параметры, потому что в противном случае этот аргумент попадает в конец, а сайт из за этого ставит блок
            url += f"country={''.join(countries)}&"

        headers = Headers().generate()   
        scraper = cloudscraper.create_scraper()     # для прохождения защиты от cloudflare
        from_page = 1
        last_page = -1
        while True:
            params['start'] = (from_page - 1)*64
            try:
                response = scraper.get(url, params=params, headers=headers)
            except Exception:
                logger.exception('Failed on download proxies')
            doc = html.fromstring(response.content)

            pagination = doc.xpath('//div[@class = "pagination"]//a')
            if not pagination:
                break
            else:
                last_page = int(pagination[-2].text)

            trs = doc.xpath('//div[@class = "table_block"]//tr')
            if not trs:
                break
            else:
                for tr in trs[1:]:
                    d = {'ip': tr[0].text,
                        'port': tr[1].text,          
                        'country': tr[2].xpath('.//span[@class = "country"]')[0].text,
                        'city': tr[2].xpath('.//span[@class = "city"]')[0].text,
                        'speed': int(tr[3].xpath('.//p')[0].text.strip(' мс')),
                        'type': tr[4].text,
                        'anon': tr[5].text,
                        'updated': tr[6].text,                 
                        }        
                    d['proxy'] = re.findall(r'HTTPS|HTTP|SOCKS5|SOCKS4', d['type'])[0].lower()+'://'+d['ip']+':'+d['port']    
                    result.append(d)
            from_page += 1
            if last_page < from_page:
                break
        logger.info('Downloaded: %d' % len(result))
        ProxyGrabber._writer_cache(result)

    @staticmethod
    def _writer_cache(cache):
        if GLOBAL_LOCK.locked():
            return
        else:
            GLOBAL_LOCK.acquire()
            with open(ProxyGrabber.CACHE_PATH, 'w', encoding='utf-8') as file:
                json.dump(cache, file, ensure_ascii=False, indent=2)
            GLOBAL_LOCK.release()

    @staticmethod
    def _read_cache():
        '''Чтение кэша с последнего парсинга'''
        logger.info('Reading cache')
        if ProxyGrabber.CACHE_PATH.exists():
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
    def _filter_meta_proxy(proxy, attrs):
        flags = []
        if not attrs:
            return True
        for k,v in attrs.items():
            if k in proxy:
                proxy_value = proxy.get(k, None)
                if proxy_value:
                    any_match = re.findall('|'.join(v), proxy_value)
                    if any_match:
                        flags.append(True)
                    else:
                        logger.debug(f'No match on filter {k}')
                        flags.append(False)
                else:
                    logger.debug(f'Key {k} havent value')
                    flags.append(False)
            else:
                logger.debug(f'Key {k} doesnt exists')
                flags.append(False)
        return all(flags)


    @staticmethod
    def get_proxies_list(minutes_from_last_update=300, proxy_attrs={}):
        '''
        proxy_attrs - параметр, по которому фильтруются скаченные/кэшированные прокси
        Например при proxt_attrs={countries:['Russian Federation', 'Belarus']} будут выданы
        прокси только из России и Беларуси 
        '''
        if ProxyGrabber.CACHE_PATH.exists():
            file_timestamp = ProxyGrabber.CACHE_PATH.stat().st_mtime
            now_timestamp = time()
            delta = now_timestamp-file_timestamp
            if delta/60 > minutes_from_last_update:
                ProxyGrabber._download()
        else:
            ProxyGrabber._download()

        meta_proxies = ProxyGrabber._read_cache()
        meta_proxies = list(filter(lambda x: ProxyGrabber._filter_meta_proxy(x, proxy_attrs), meta_proxies))
        proxies = [i['proxy'] for i in meta_proxies]
        return proxies

    def get_proxy(self):
        if self.PROXIES_INDEX == -1:
            return {'http': None, 'https': None}
        else:
            if not ProxyGrabber.PROXIES:
                ProxyGrabber.PROXIES = ProxyGrabber.get_proxies_list(proxy_attrs=self.proxy_attrs)
            proxy = ProxyGrabber.PROXIES[self.PROXIES_INDEX]
            return {'http': proxy, 'https': proxy}

    def next_proxy(self):
        self.PROXIES_INDEX += 1  
        if ProxyGrabber.PROXIES: 
            if self.PROXIES_INDEX >= len(ProxyGrabber.PROXIES):
                self.reset()        
                ProxyGrabber.PROXIES = ProxyGrabber.get_proxies_list(proxy_attrs=self.proxy_attrs)
            logger.info(f'Changed proxy, {self.PROXIES_INDEX + 1} out {len(ProxyGrabber.PROXIES)} [thread{get_ident()}]')
        return self.get_proxy()

    def reset(self):
        self.PROXIES_INDEX = -1
        logger.info('Reset proxies list')


class ProxyGrabberThreadingFactory:
    def __init__(self, **kwargs):
        self.default_proxy_filters = kwargs
        ProxyGrabber.get_proxies_list()
        self.threads_grabbers = {}

    def get_grabber(self):
        thread_id = get_ident()
        if thread_id not in self.threads_grabbers:
            self.threads_grabbers[thread_id] = ProxyGrabber(**self.default_proxy_filters)
        return self.threads_grabbers[thread_id]

    def get_proxy(self):
        return self.get_grabber().get_proxy()

    def next_proxy(self):
        return self.get_grabber().next_proxy()


GLOBAL_LOCK = Lock()


if __name__ == '__main__':
    # logger.debug('test')
    p3 = ProxyGrabberThreadingFactory(country=['Ukraine', 'Belarus', 'Russian Federation'], type=['HTTPS'])
    

    
    
    

    
    