import time
import requests
from shapely.geometry import MultiPoint, Point
import geopandas as gpd
from itertools import chain

from ProxyGrabber import ProxyGrabber

import re
import logging

logger = logging.getLogger(__name__)
HERE_API_KEY = 'uqDak72P-C8FYyYkI4vz8EDxbNFhBJw4jW-fg9P4lb8'

__GRABBER = None
def get_grabber():
    global __GRABBER
    if __GRABBER is None:
        __GRABBER = ProxyGrabber()
    return __GRABBER

def _rosreestr_request(current_kadastr):
    grabber = get_grabber()
    url = 'https://pkk.rosreestr.ru/api/features/{req_type}?_={time}&text={kadastr}&limit=40&skip=0'
    req_type = 5 - len(current_kadastr.split(':'))
    formatted_url = url.format(kadastr=current_kadastr, time = round(time.time() * 1000), req_type = req_type)
    try:
        r = requests.get(formatted_url, proxies = grabber.get_proxy(), timeout=3)
    except:
        logger.debug(f'(PKK) request exception [{current_kadastr}]')
        grabber.next_proxy()
        return _rosreestr_request(current_kadastr)
    if r.status_code == 403:
        logger.debug(f'(PKK) response 403 [{current_kadastr}]')
        grabber.next_proxy()
        return _rosreestr_request(current_kadastr)
    result = r.json()
    if result['features']:
        center = result['features'][0].get('center')
        if center:
            pt = Point(center['x'], center['y'])
            pt = gpd.GeoSeries(pt, crs=3857).to_crs(4326)[0]
            logger.info(f'(PKK) Kadastr geocoded [{current_kadastr}]')
            return pt
        else:
            logger.warning(f'(PKK) No center in feature [{current_kadastr}]')
    else:
        logger.warning(f'(PKK) No features in response [{current_kadastr}]')

def rosreestr(kadastr, deep_search = False):
    kad_check = re.findall(r'[\d:]+', kadastr)
    if len(kad_check) != 1:
        raise TypeError(f'"{kadastr}" не является поддерживаемым форматом для геокодирования')
    kadastr = kad_check[0]
    if not deep_search:
        return _rosreestr_request(kadastr)
    else:
        kad_numbers = kadastr.split(':')    
        for i in reversed(range(0, len(kad_numbers))):
            current_kadastr = ':'.join(kad_numbers[: i + 1 ])
            pt = _rosreestr_request(current_kadastr)
            if pt:
                return pt                
        

def here(search_string, additional_params = None):
    # избавляемся от переносов строки, табуляции и прочего
    search_string = re.sub(r'\s+', ' ', search_string)
    # убираем повторяющиеся лексемы из запроса
    args = search_string.split(',')
    args = [i.strip().split(' ') for i in args]
    args = chain(*args)
    unique_args = []
    for a in args:
        if a not in unique_args:
            unique_args.append(a)
    search_string = ' '.join(unique_args)
    # генерируем запрос
    params = {
    'q': search_string,
    'apiKey': HERE_API_KEY,
    'lang': 'ru-RU'
    }
    url = f'https://geocode.search.hereapi.com/v1/geocode?q={params["q"]}&apiKey={params["apiKey"]}&lang=ru-RU'
    if additional_params:
        params = {**params, **additional_params}
    try:
        r = requests.get(url)
        if r.status_code == 429:
            time.sleep(1)
            return here(search_string, additional_params = None)
        data = r.json()['items']
    except:
        logger.exception(f'(HERE) Error, status_code {r.status_code}')
    else:
        if len(data) > 0:
            if len(data) > 1:
                item = sorted(data, key = lambda x: x['scoring']['queryScore'], reverse=True)[0]
            else:
                item = data[0]
            x = item['position']['lng']
            y = item['position']['lat']
            pt = Point(x,y)
            logger.info(f'(HERE) Geocoded address [{search_string}]')
            return pt
        else:
            logger.warning(f'(HERE) Geocoder return empty list')


def yandex_map(search_string):
    pass

if __name__ == '__main__':
    # current_kadastr = '65:22:0000003:1119'
    # current_kadastr = '65:22:0000003'
    # current_kadastr = '14:36:000000:19144; 14:36:107007:43; 14:36:107007:375'
    # current_kadastr = '  14:36:000000 '
    # kad_check = re.findall(r'[\d:]+', kn1)
    # r = rosreestr(current_kadastr)

    address = 'Новосибирская обл, Чулымский р-н, Чулым г, Энгельса ул Новосибирская обл, Чулымский р-н, Чулым г, Энгельса ул, дом 7'
    r = here(address)
