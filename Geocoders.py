if __name__ == '__main__':
    import FastLogging
    from ProxyGrabber import ProxyGrabber
else:
    from .ProxyGrabber import ProxyGrabber

import warnings
from rosreestr2coord import Area, VERSION
from rosreestr2coord.parser import TYPES
if 'yazmo' not in VERSION:
    warnings.warn("Use yazmolod/rosreestr2coord instead default rendrom/rosreestr2coord. It's too more safe and cooler", ImportWarning, stacklevel=2)

import time
import requests
from shapely.geometry import MultiPoint, Point, shape
from pathlib import Path
import json
from pyproj import Transformer
from itertools import chain
import re
import shutil
import logging
logger = logging.getLogger(__name__)

HERE_API_KEY = 'uqDak72P-C8FYyYkI4vz8EDxbNFhBJw4jW-fg9P4lb8'
YANDEX_API_KEY = '3e976513-247e-4072-8b7b-2fd619d45b2c'

__GRABBER = None
def get_grabber():
    global __GRABBER
    if __GRABBER is None:
        __GRABBER = ProxyGrabber()
    return __GRABBER

def _validate_kadastr(kadastr):
    kad_check = re.findall(r'[\d:]+', kadastr)
    if len(kad_check) != 1:
        raise TypeError(f'"{kadastr}" не является поддерживаемым форматом для геокодирования')
    return kad_check[0]

def _rosreestr_request(current_kadastr, recursive=1):
    if recursive >= 10:
        return
    grabber = get_grabber()
    cad = []
    for i in current_kadastr.split(':'):
        cad_part = i.lstrip('0')
        if not cad_part:
            cad.append('0')
        else:
            cad.append(cad_part)
    current_kadastr = ':'.join(cad)
    req_type = 5 - len(current_kadastr.split(':'))
    url = f'https://pkk.rosreestr.ru/api/features/{req_type}/{current_kadastr}?date_format=%c&_={round(time.time() * 1000)}'
    try:
        r = requests.get(url, proxies = grabber.get_proxy(), timeout=3)
    except:
        logger.debug(f'(PKK Point) request exception [{current_kadastr}]')
        grabber.next_proxy()
        return _rosreestr_request(current_kadastr, recursive+1)
    if r.status_code != 200:
        logger.debug(f'(PKK) bad response [{current_kadastr}], {r.status_code}')
        grabber.next_proxy()
        return _rosreestr_request(current_kadastr, recursive+1)
    result = r.json()
    feature = result.get('feature')
    if feature:
        center = feature.get('center')
        extent_parent = feature.get('extent_parent')
        x = y = None
        if center:
            x,y = center['x'], center['y']
            logger.info(f'(PKK Point) Kadastr geocoded [{current_kadastr}]')
        elif extent_parent:
            x = (extent_parent['xmin'] + extent_parent['xmax'])/2
            y = (extent_parent['ymin'] + extent_parent['ymax'])/2
            logger.info(f'(PKK Point) Kadastr geocoded by parent [{current_kadastr}]')
        if x and y:
            transformer = Transformer.from_crs("epsg:3857", "epsg:4326", always_xy=True)
            x, y = transformer.transform(x, y)
            return Point(x, y)          
        else:
            logger.warning(f'(PKK Point) No center in feature [{current_kadastr}]')
    else:
        logger.warning(f'(PKK Point) No features in response [{current_kadastr}]')

def rosreestr(kadastr, deep_search=False):
    warnings.warn('Method Geocoders.rosreestr is deprecated; use Geocoders.rosreestr_point', DeprecationWarning, stacklevel=2)
    kadastr = _validate_kadastr(kadastr)
    if not deep_search:
        return _rosreestr_request(kadastr)
    else:
        kad_numbers = kadastr.split(':')    
        for i in reversed(range(0, len(kad_numbers))):
            current_kadastr = ':'.join(kad_numbers[: i + 1 ])
            pt = _rosreestr_request(current_kadastr)
            if pt:
                return pt

KADASTR_TYPES = {k:v for k,v in TYPES.items() if k in ['Участки', 'ОКС']}
def _rosreestr_geom(kadastr, center_only):
    geom_type = 'Point' if center_only else 'Polygon'
    kadastr = _validate_kadastr(kadastr)
    for kadastr_type_alias, kadastr_type in KADASTR_TYPES.items():
        logger.debug(f'(PKK Geom) Try geocode {kadastr} ({geom_type}), type {kadastr_type} ({kadastr_type_alias})')
        area = Area(code=kadastr, area_type=kadastr_type, use_cache=True, center_only=center_only, media_path=str(Path(__file__).resolve().parent))
        geom_method = area.to_geojson_center if center_only else area.to_geojson_poly
        feature_string = geom_method()
        if feature_string:
            feature = json.loads(feature_string)
            logger.info(f'(PKK Geom) Geocoded {kadastr} ({geom_type}), type {kadastr_type} ({kadastr_type_alias})')
            geom = shape(feature['geometry'])
            return geom
        else:
            logger.debug(f'(PKK Geom) Nothing found for {kadastr} ({geom_type}), type {kadastr_type} ({kadastr_type_alias})')
            (Path(area.workspace) / "feature_info.json").unlink()   # удаляем кэш, так как был неверный тип 
    logger.warning(f'(PKK Geom) {geom_type} not found ({kadastr})')

def rosreestr_point(kadastr):
    return _rosreestr_geom(kadastr, center_only=True)

def rosreestr_polygon(kadastr):
    return _rosreestr_geom(kadastr, center_only=False)

def delete_rosreestr_cache():
    path = Path(__file__).parent.resolve() / 'rosreestr_cache'
    shutil.rmtree(str(path))

def here(search_string, additional_params=None):
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
            return here(search_string, additional_params=None)
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

def yandex(search_string):
    endpoint = 'https://geocode-maps.yandex.ru/1.x'
    params = {
        'apikey': YANDEX_API_KEY,
        'geocode': search_string,
        'format':'json'
    }
    r = requests.get(endpoint, params=params)
    data = r.json()
    features = data['response']['GeoObjectCollection']['featureMember']
    if features:
        geodata = features[0]['GeoObject']
        point = Point(list(map(float,geodata['Point']['pos'].split(' '))))
        return point
    else:
        logger.warning(f'(YANDEX) Not found "{search_string}"')


if __name__ == '__main__':
    logger = FastLogging.getLogger(__name__)
    _1 = FastLogging.getLogger('GIS_Tools.ProxyGrabber')
    _2 = FastLogging.getLogger('mylib.rosreestr2coord')
    _3 = FastLogging.getLogger('rosreestr2coord')
    # pl = rosreestr_polygon('05:41:000077:104')
    # pl = rosreestr_polygon('77:01:0001051:120')
    delete_rosreestr_cache()
