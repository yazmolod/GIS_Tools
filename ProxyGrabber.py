from fake_headers import Headers
from GIS_Tools.config import HIDEMY_NAME_API_CODE
import cloudscraper
from lxml import html
import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor, wait
from time import time
import logging
logger = logging.getLogger(__name__)
from pathlib import Path
from threading import get_ident
from threading import Lock
from urllib.parse import urlencode
import warnings


class ApiCodeExpiredError(Exception):
    pass


class ProxiesDownloadError(Exception):
    pass


ACTIVE_GRABBERS = {}
def get_grabber(*args, **kwargs):
    global ACTIVE_GRABBERS
    thread_id = get_ident()
    if thread_id not in ACTIVE_GRABBERS:
        ACTIVE_GRABBERS[thread_id] = ProxyGrabber(*args, **kwargs)
    logger.debug(f'Return ProxyGrabber instance [thread{thread_id}]')
    return ACTIVE_GRABBERS[thread_id]

class ProxyGrabber:
    CACHE_PATH = Path(__file__).parent / '_proxies.json'
    PROXIES = None

    def __init__(self, allowed_countries=[], use_api=False):
        '''
        allowed_countries - параметр, по которому фильтруются скаченные/кэшированные прокси по странам, ['RU', 'BY']
        '''
        self.allowed_countries = allowed_countries
        self.__threads_indexes = {}
        self.__threads_proxies = {}
        self.default_speed = 5000
        self.default_types = 's45'
        self.default_minutes_from_last_update = 300
        self.use_api = use_api
        # качаем сразу
        self.init_proxies()

    @property
    def PROXIES_INDEX(self):
        thread_id = get_ident()
        if thread_id not in self.__threads_indexes:
            self.__threads_indexes[thread_id] = -1
        return self.__threads_indexes[thread_id]


    def _increase_index(self):
        thread_id = get_ident()
        self.__threads_indexes[thread_id] = self.PROXIES_INDEX + 1


    def _decrease_index(self):
        thread_id = get_ident()
        self.__threads_indexes[thread_id] = self.PROXIES_INDEX - 1


    def _reset_index(self):
        thread_id = get_ident()
        self.__threads_indexes[thread_id] = -1


    def _api_download(self):
        url = 'http://hidemy.name/ru/api/proxylist.php'
        params = {
        'out': 'js',
        'maxtime': self.default_speed,
        'type': self.default_types,
        'code': ProxyGrabber.HIDEMY_NAME_API_CODE
        }

        r = requests.get(url, params=params)
        if r.status_code != 200:
            cache_proxy = self.get_cache_list()
            iproxy = 0
            while True:
                r = requests.get(url, params=params, proxies=cache_proxy[iproxy])
                if r.status_code == 200:
                    break
                else:
                    iproxy += 1
        if r.text == 'NOTFOUND':
            raise ApiCodeExpiredError()
        else:
            data = r.json()
            updated_data = []
            for d in data:
                proxy = ProxyGrabber.proxy_from_dict(d)
                updated_data.append({'proxy': proxy, **d})
            logger.info('Downloaded: %d' % len(updated_data))
            return updated_data


    @staticmethod
    def proxy_from_dict(d):
        ip = d['ip']
        port = d['port']
        if d['ssl'] == '1':
            return f'https://{ip}:{port}'
        elif d['socks4'] == '1':
            return f'socks4://{ip}:{port}'
        elif d['socks5'] == '1':
            return f'socks5://{ip}:{port}'
        elif d['http'] == '1':
            return f'http://{ip}:{port}'


    def download(self):
        result = None
        if self.use_api:
            try:
                result = self._api_download()
            except Exception as e:
                logger.error(f'Error api download')
            else:
                return result
        try:
            result = self._parse_download()
        except Exception as e:
            logger.error(f'Error parse download')
        return result


    def _parse_download(self):
        '''Скачивает прокси с https://hidemy.name/ и записывает их в кэш. Работает на cloudflare поэтому временами ломается (возможно навечно)'''
        logger.info('Downloading proxies...')
        result = []
        params = {'maxtime':self.default_speed,   # скорость прокси
                  'type': self.default_types,     #h - http, s - https, 4 - socks4, 5 - socks5
                  'anon': '1234',     #низкая, средняя и высокая
                  'start': 0,        # смещение запросов
                 }        

        headers = Headers().generate()   
        scraper = cloudscraper.create_scraper()     # для прохождения защиты от cloudflare
        from_page = 1
        last_page = -1

        while True:
            logger.debug(f'page: {from_page}')
            params['start'] = (from_page - 1)*64
            url = 'https://hidemy.name/ru/proxy-list/?' + urlencode(params)
            response = scraper.get(url, headers=headers)
            assert 'access denied' not in response.text.lower(), 'Access denied by cloudflare'
            doc = html.fromstring(response.content)
            pagination = doc.xpath('//div[@class = "pagination"]//a')
            if not pagination:
                break
            elif len(pagination) <= 2:
                last_page = 1
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
        return result


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
        logger.debug('Reading cache')
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


    def _filter_meta_proxy(self, proxy, **filter_attrs):
        flags = []
        for k,v in filter_attrs.items():
            if k in proxy:
                proxy_value = proxy[k]
                if proxy_value:
                    any_match = re.findall('|'.join(v), proxy_value)
                    if any_match:
                        flags.append(True)
                    else:
                        # logger.debug(f'No match on filter {k}')
                        flags.append(False)
                else:
                    # logger.debug(f'None value for {k} for {proxy}')
                    flags.append(False)
            else:
                raise KeyError(f'Неправильный ключ для фильтрации прокси - {k}. Доступные ключи - {proxy.keys()}')
        return all(flags)


    def get_cache_list(self):
        meta_proxies = ProxyGrabber._read_cache()
        if self.allowed_countries:
            meta_proxies = list(filter(lambda x: self._filter_meta_proxy(x, country=self.allowed_countries), meta_proxies))
        proxies = [i['proxy'] for i in meta_proxies]
        return proxies


    def init_proxies(self):
        fresh_downloaded_proxies = None     
        if ProxyGrabber.CACHE_PATH.exists():
            file_timestamp = ProxyGrabber.CACHE_PATH.stat().st_mtime
            now_timestamp = time()
            delta = now_timestamp-file_timestamp
            if delta/60 > self.default_minutes_from_last_update:
                logger.debug('Cache too old, download...')
                fresh_downloaded_proxies = self.download()
        else:
            logger.debug('Cache not exists, download...')
            fresh_downloaded_proxies = self.download()
        if fresh_downloaded_proxies:
            ProxyGrabber._writer_cache(fresh_downloaded_proxies)
        proxies = self.get_cache_list()
        self.PROXIES = proxies


    def get_proxy(self, format_type='requests'):
        if self.PROXIES_INDEX == -1:
            proxy = None
        else:
            proxy = self.PROXIES[self.PROXIES_INDEX]
        if format_type=='requests':
            return {'http': proxy, 'https': proxy}
        elif format_type=='selenium':
            if proxy:
                return proxy.split('/')[-1]
        else:
            raise TypeError(f'Uknown format type "{format_type}"')


    def next_proxy(self, format_type='requests'):
        if self.PROXIES: 
            self._increase_index()
            if self.PROXIES_INDEX >= len(self.PROXIES):
                self._reset_index()        
                self.init_proxies()
            logger.info(f'Changed proxy, {self.PROXIES_INDEX + 1} out {len(self.PROXIES)} [thread{get_ident()}]')
        else:
            logger.error('NO PROXIES FOUND')
            warnings.warn('Proxies not founded!', UserWarning, stacklevel=2)
        return self.get_proxy(format_type=format_type)


GLOBAL_LOCK = Lock()